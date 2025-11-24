"""Core scraper with multi-threading and dynamic data collection."""
import time
import random
import logging
from typing import List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from queue import Queue, Empty
from dataclasses import dataclass

import requests
import cloudscraper
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

from .config import ScraperConfig, get_config, SPANISH_CITIES, MEDICAL_SPECIALTIES
from .models import Doctor, ScrapeTask, ScrapeResult
from .parser import DoctorListParser, DoctorProfileParser
from .proxy_manager import ProxyManager, get_proxy_manager
from .storage import StorageManager

logger = logging.getLogger(__name__)


class RequestHandler:
    """Handles HTTP requests with retry logic and proxy rotation."""
    
    def __init__(self, config: ScraperConfig, proxy_manager: Optional[ProxyManager] = None):
        self.config = config
        self.proxy_manager = proxy_manager
        self._ua = UserAgent()
        self._session: Optional[cloudscraper.CloudScraper] = None
        self._lock = Lock()
        self._request_count = 0
        self._last_request_time = 0.0
    
    def _get_session(self) -> cloudscraper.CloudScraper:
        """Get or create cloudscraper session."""
        if self._session is None:
            self._session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True
                }
            )
        return self._session
    
    def _get_headers(self) -> dict:
        """Get request headers with random user agent."""
        headers = self.config.DEFAULT_HEADERS.copy()
        headers['User-Agent'] = self._ua.random
        return headers
    
    def _rate_limit(self) -> None:
        """Apply rate limiting."""
        with self._lock:
            now = time.time()
            min_interval = 60.0 / self.config.REQUESTS_PER_MINUTE
            elapsed = now - self._last_request_time
            
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                time.sleep(sleep_time)
            
            # Add random delay
            delay = random.uniform(self.config.REQUEST_DELAY_MIN, self.config.REQUEST_DELAY_MAX)
            time.sleep(delay)
            
            self._last_request_time = time.time()
            self._request_count += 1
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.RequestException, ConnectionError))
    )
    def get(self, url: str) -> requests.Response:
        """Make GET request with retry logic."""
        self._rate_limit()
        
        session = self._get_session()
        headers = self._get_headers()
        proxies = None
        
        if self.proxy_manager and self.proxy_manager.has_proxies():
            proxy = self.proxy_manager.get_proxy()
            if proxy:
                proxies = proxy.get_dict()
        
        try:
            response = session.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=30,
                allow_redirects=True
            )
            
            # Check for blocking
            if response.status_code == 403 or response.status_code == 429:
                if self.proxy_manager and proxies:
                    proxy = self.proxy_manager.get_proxy()
                    if proxy:
                        self.proxy_manager.report_failure(proxy)
                raise requests.RequestException(f"Blocked: {response.status_code}")
            
            response.raise_for_status()
            
            if self.proxy_manager and proxies:
                proxy = self.proxy_manager.get_proxy()
                if proxy:
                    self.proxy_manager.report_success(proxy)
            
            return response
            
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            raise


class DoctoraliaScraper:
    """Main scraper class with multi-threading support."""
    
    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        storage: Optional[StorageManager] = None,
        proxy_manager: Optional[ProxyManager] = None
    ):
        self.config = config or get_config()
        self.storage = storage or StorageManager(
            output_dir=self.config.OUTPUT_DIR,
            output_format=self.config.OUTPUT_FORMAT,
            db_path=self.config.DB_PATH
        )
        self.proxy_manager = proxy_manager or get_proxy_manager()
        self.request_handler = RequestHandler(self.config, self.proxy_manager)
        
        self._stop_event = Event()
        self._stats = {
            'pages_scraped': 0,
            'doctors_found': 0,
            'doctors_saved': 0,
            'errors': 0
        }
        self._stats_lock = Lock()
    
    def _update_stats(self, **kwargs) -> None:
        """Thread-safe stats update."""
        with self._stats_lock:
            for key, value in kwargs.items():
                if key in self._stats:
                    self._stats[key] += value
    
    def scrape_listing_page(self, task: ScrapeTask) -> ScrapeResult:
        """Scrape a listing page and extract doctor profiles dynamically."""
        url = task.get_url(self.config.BASE_URL)
        doctors: List[Doctor] = []
        
        try:
            response = self.request_handler.get(url)
            parser = DoctorListParser(response.text, self.config.BASE_URL)
            
            if not parser.has_results():
                return ScrapeResult(
                    success=True,
                    task=task,
                    has_more_pages=False
                )
            
            # Get doctor profile URLs
            profile_urls = parser.get_doctor_profile_urls()
            
            # Dynamically scrape each profile immediately
            for profile_url in profile_urls:
                if self._stop_event.is_set():
                    break
                
                # Skip if already scraped
                if self.storage.is_url_scraped(profile_url):
                    continue
                
                doctor = self._scrape_doctor_profile(profile_url, task.specialty, task.city)
                if doctor:
                    doctors.append(doctor)
                    # Save immediately
                    self.storage.save_doctor(doctor)
                    self._update_stats(doctors_found=1, doctors_saved=1)
            
            # Check for next page
            next_page_url = parser.get_next_page_url()
            
            self._update_stats(pages_scraped=1)
            
            return ScrapeResult(
                success=True,
                task=task,
                doctors=doctors,
                next_page_url=next_page_url,
                has_more_pages=next_page_url is not None
            )
            
        except Exception as e:
            logger.error(f"Error scraping listing {url}: {e}")
            self._update_stats(errors=1)
            return ScrapeResult(
                success=False,
                task=task,
                error=str(e)
            )
    
    def _scrape_doctor_profile(self, url: str, specialty: str, city: str) -> Optional[Doctor]:
        """Scrape individual doctor profile."""
        try:
            response = self.request_handler.get(url)
            parser = DoctorProfileParser(response.text, url, specialty, city)
            return parser.parse()
        except Exception as e:
            logger.warning(f"Error scraping profile {url}: {e}")
            return None
    
    def _worker(self, task_queue: Queue, progress_callback: Optional[Callable] = None) -> None:
        """Worker thread for processing scrape tasks."""
        while not self._stop_event.is_set():
            try:
                task = task_queue.get(timeout=1)
            except Empty:
                continue
            
            try:
                result = self.scrape_listing_page(task)
                
                # If there are more pages, add next page task
                if result.has_more_pages and result.next_page_url:
                    next_task = ScrapeTask(
                        specialty=task.specialty,
                        city=task.city,
                        page=task.page + 1,
                        url=result.next_page_url
                    )
                    task_queue.put(next_task)
                
                if progress_callback:
                    progress_callback(result)
                    
            except Exception as e:
                logger.error(f"Worker error: {e}")
                self._update_stats(errors=1)
            finally:
                task_queue.task_done()
    
    def scrape_all(
        self,
        specialties: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        max_workers: Optional[int] = None
    ) -> dict:
        """
        Scrape all doctors with multi-threading.
        
        Args:
            specialties: List of specialties to scrape (default: all)
            cities: List of cities to scrape (default: all)
            max_workers: Number of worker threads (default: from config)
        
        Returns:
            Statistics dictionary
        """
        specialties = specialties or MEDICAL_SPECIALTIES
        cities = cities or SPANISH_CITIES
        max_workers = max_workers or self.config.MAX_WORKERS
        
        # Create task queue
        task_queue: Queue = Queue()
        
        # Generate initial tasks (one per specialty-city combination)
        total_combinations = len(specialties) * len(cities)
        logger.info(f"Starting scrape: {len(specialties)} specialties x {len(cities)} cities = {total_combinations} combinations")
        
        for specialty in specialties:
            for city in cities:
                task = ScrapeTask(specialty=specialty, city=city, page=1)
                task_queue.put(task)
        
        # Progress bar
        pbar = tqdm(total=total_combinations, desc="Scraping", unit="pages")
        
        def progress_callback(result: ScrapeResult):
            pbar.update(1)
            pbar.set_postfix({
                'doctors': self._stats['doctors_saved'],
                'errors': self._stats['errors']
            })
        
        # Start worker threads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for _ in range(max_workers):
                future = executor.submit(self._worker, task_queue, progress_callback)
                futures.append(future)
            
            # Wait for queue to be empty
            try:
                task_queue.join()
            except KeyboardInterrupt:
                logger.info("Stopping scraper...")
                self._stop_event.set()
            
            # Signal workers to stop
            self._stop_event.set()
        
        pbar.close()
        
        # Final stats
        logger.info(f"Scraping complete. Stats: {self._stats}")
        
        return self._stats.copy()
    
    def scrape_specialty_city(self, specialty: str, city: str) -> List[Doctor]:
        """Scrape all doctors for a specific specialty and city."""
        doctors: List[Doctor] = []
        page = 1
        
        while not self._stop_event.is_set():
            task = ScrapeTask(specialty=specialty, city=city, page=page)
            result = self.scrape_listing_page(task)
            
            doctors.extend(result.doctors)
            
            if not result.has_more_pages:
                break
            
            page += 1
        
        return doctors
    
    def stop(self) -> None:
        """Stop the scraper gracefully."""
        self._stop_event.set()
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        with self._stats_lock:
            return self._stats.copy()


class DynamicScraper(DoctoraliaScraper):
    """
    Enhanced scraper that collects data dynamically without pre-collecting links.
    Processes each doctor immediately as found.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._processed_urls: set = set()
        self._url_lock = Lock()
    
    def _is_processed(self, url: str) -> bool:
        """Check if URL was already processed in this session."""
        with self._url_lock:
            return url in self._processed_urls
    
    def _mark_processed(self, url: str) -> None:
        """Mark URL as processed."""
        with self._url_lock:
            self._processed_urls.add(url)
    
    def scrape_listing_page(self, task: ScrapeTask) -> ScrapeResult:
        """
        Scrape listing page with immediate doctor processing.
        No link collection phase - data is extracted on the fly.
        """
        url = task.get_url(self.config.BASE_URL)
        doctors: List[Doctor] = []
        
        try:
            logger.debug(f"Scraping listing: {url}")
            response = self.request_handler.get(url)
            parser = DoctorListParser(response.text, self.config.BASE_URL)
            
            if not parser.has_results():
                logger.debug(f"No results for {url}")
                return ScrapeResult(
                    success=True,
                    task=task,
                    has_more_pages=False
                )
            
            # Get and immediately process each doctor profile
            profile_urls = parser.get_doctor_profile_urls()
            logger.debug(f"Found {len(profile_urls)} profiles on {url}")
            
            for profile_url in profile_urls:
                if self._stop_event.is_set():
                    break
                
                # Skip duplicates
                if self._is_processed(profile_url):
                    continue
                
                if self.storage.is_url_scraped(profile_url):
                    self._mark_processed(profile_url)
                    continue
                
                # Scrape and save immediately
                doctor = self._scrape_doctor_profile(profile_url, task.specialty, task.city)
                
                if doctor:
                    doctors.append(doctor)
                    saved = self.storage.save_doctor(doctor)
                    if saved:
                        self._update_stats(doctors_found=1, doctors_saved=1)
                        logger.info(f"Saved: {doctor.name} ({doctor.specialty}, {doctor.city})")
                
                self._mark_processed(profile_url)
            
            # Get next page
            next_page_url = parser.get_next_page_url()
            self._update_stats(pages_scraped=1)
            
            return ScrapeResult(
                success=True,
                task=task,
                doctors=doctors,
                next_page_url=next_page_url,
                has_more_pages=next_page_url is not None
            )
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            self._update_stats(errors=1)
            
            # Retry logic
            if task.retries < self.config.RETRY_ATTEMPTS:
                task.retries += 1
                time.sleep(self.config.RETRY_DELAY)
                return self.scrape_listing_page(task)
            
            return ScrapeResult(
                success=False,
                task=task,
                error=str(e)
            )
