"""Browser-based scraper using undetected-chromedriver to bypass Cloudflare."""
import time
import random
import logging
from typing import Optional, List
from threading import Lock

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from .config import ScraperConfig, get_config
from .models import Doctor, ScrapeTask, ScrapeResult
from .parser import DoctorListParser, DoctorProfileParser
from .storage import StorageManager

logger = logging.getLogger(__name__)


class BrowserPool:
    """Pool of browser instances for multi-threaded scraping."""
    
    def __init__(self, pool_size: int = 3, headless: bool = True):
        self.pool_size = pool_size
        self.headless = headless
        self._browsers: List[uc.Chrome] = []
        self._available: List[uc.Chrome] = []
        self._lock = Lock()
        self._initialized = False
    
    def _create_browser(self) -> uc.Chrome:
        """Create a new undetected Chrome browser."""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=es-ES')
        
        # Spanish locale
        prefs = {
            'intl.accept_languages': 'es-ES,es',
            'profile.default_content_setting_values.geolocation': 1
        }
        options.add_experimental_option('prefs', prefs)
        
        driver = uc.Chrome(options=options, version_main=None)
        driver.set_page_load_timeout(60)
        
        return driver
    
    def initialize(self) -> None:
        """Initialize the browser pool."""
        if self._initialized:
            return
        
        logger.info(f"Initializing browser pool with {self.pool_size} browsers...")
        
        for i in range(self.pool_size):
            try:
                browser = self._create_browser()
                self._browsers.append(browser)
                self._available.append(browser)
                logger.info(f"Browser {i+1}/{self.pool_size} initialized")
            except Exception as e:
                logger.error(f"Failed to create browser {i+1}: {e}")
        
        self._initialized = True
        logger.info(f"Browser pool ready with {len(self._browsers)} browsers")
    
    def acquire(self) -> Optional[uc.Chrome]:
        """Acquire a browser from the pool."""
        with self._lock:
            if self._available:
                return self._available.pop()
            return None
    
    def release(self, browser: uc.Chrome) -> None:
        """Release a browser back to the pool."""
        with self._lock:
            if browser in self._browsers and browser not in self._available:
                self._available.append(browser)
    
    def close_all(self) -> None:
        """Close all browsers."""
        for browser in self._browsers:
            try:
                browser.quit()
            except Exception:
                pass
        self._browsers.clear()
        self._available.clear()
        self._initialized = False


class BrowserScraper:
    """Scraper using real browser to bypass Cloudflare."""
    
    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        storage: Optional[StorageManager] = None,
        headless: bool = True,
        browser_count: int = 2
    ):
        self.config = config or get_config()
        self.storage = storage or StorageManager(
            output_dir=self.config.OUTPUT_DIR,
            output_format=self.config.OUTPUT_FORMAT,
            db_path=self.config.DB_PATH
        )
        self.browser_pool = BrowserPool(pool_size=browser_count, headless=headless)
        
        self._stats = {
            'pages_scraped': 0,
            'doctors_found': 0,
            'doctors_saved': 0,
            'errors': 0
        }
        self._stats_lock = Lock()
        self._processed_urls: set = set()
    
    def _update_stats(self, **kwargs) -> None:
        """Thread-safe stats update."""
        with self._stats_lock:
            for key, value in kwargs.items():
                if key in self._stats:
                    self._stats[key] += value
    
    def _random_delay(self) -> None:
        """Add random delay between requests."""
        delay = random.uniform(2.0, 5.0)
        time.sleep(delay)
    
    def _get_page_source(self, browser: uc.Chrome, url: str) -> Optional[str]:
        """Get page source with retry logic."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                browser.get(url)
                
                # Wait for page to load
                WebDriverWait(browser, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Additional wait for dynamic content
                time.sleep(2)
                
                # Check for Cloudflare challenge
                page_source = browser.page_source
                if "challenge-running" in page_source or "cf-browser-verification" in page_source:
                    logger.info("Cloudflare challenge detected, waiting...")
                    time.sleep(10)
                    page_source = browser.page_source
                
                return page_source
                
            except TimeoutException:
                logger.warning(f"Timeout loading {url}, attempt {attempt + 1}/{max_retries}")
                time.sleep(5)
            except WebDriverException as e:
                logger.warning(f"Browser error for {url}: {e}")
                time.sleep(5)
        
        return None
    
    def scrape_listing_page(self, browser: uc.Chrome, task: ScrapeTask) -> ScrapeResult:
        """Scrape a listing page using browser."""
        url = task.get_url(self.config.BASE_URL)
        doctors: List[Doctor] = []
        
        try:
            self._random_delay()
            page_source = self._get_page_source(browser, url)
            
            if not page_source:
                return ScrapeResult(success=False, task=task, error="Failed to load page")
            
            parser = DoctorListParser(page_source, self.config.BASE_URL)
            
            if not parser.has_results():
                return ScrapeResult(success=True, task=task, has_more_pages=False)
            
            # Get doctor profile URLs
            profile_urls = parser.get_doctor_profile_urls()
            logger.info(f"Found {len(profile_urls)} doctors on {url}")
            
            # Scrape each profile
            for profile_url in profile_urls:
                if profile_url in self._processed_urls:
                    continue
                
                if self.storage.is_url_scraped(profile_url):
                    self._processed_urls.add(profile_url)
                    continue
                
                doctor = self._scrape_doctor_profile(browser, profile_url, task.specialty, task.city)
                
                if doctor:
                    doctors.append(doctor)
                    self.storage.save_doctor(doctor)
                    self._update_stats(doctors_found=1, doctors_saved=1)
                    logger.info(f"Saved: {doctor.name} ({doctor.specialty}, {doctor.city})")
                
                self._processed_urls.add(profile_url)
            
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
            logger.error(f"Error scraping {url}: {e}")
            self._update_stats(errors=1)
            return ScrapeResult(success=False, task=task, error=str(e))
    
    def _scrape_doctor_profile(
        self, 
        browser: uc.Chrome, 
        url: str, 
        specialty: str, 
        city: str
    ) -> Optional[Doctor]:
        """Scrape individual doctor profile."""
        try:
            self._random_delay()
            page_source = self._get_page_source(browser, url)
            
            if not page_source:
                return None
            
            parser = DoctorProfileParser(page_source, url, specialty, city)
            return parser.parse()
            
        except Exception as e:
            logger.warning(f"Error scraping profile {url}: {e}")
            return None
    
    def scrape_specialty_city(self, specialty: str, city: str) -> List[Doctor]:
        """Scrape all doctors for a specialty-city combination."""
        self.browser_pool.initialize()
        browser = self.browser_pool.acquire()
        
        if not browser:
            logger.error("No browser available")
            return []
        
        doctors: List[Doctor] = []
        page = 1
        
        try:
            while True:
                task = ScrapeTask(specialty=specialty, city=city, page=page)
                result = self.scrape_listing_page(browser, task)
                
                doctors.extend(result.doctors)
                
                if not result.has_more_pages or not result.success:
                    break
                
                page += 1
                
        finally:
            self.browser_pool.release(browser)
        
        return doctors
    
    def scrape_all(
        self,
        specialties: List[str],
        cities: List[str]
    ) -> dict:
        """Scrape all combinations (single-threaded with browser)."""
        self.browser_pool.initialize()
        browser = self.browser_pool.acquire()
        
        if not browser:
            logger.error("No browser available")
            return self._stats
        
        total = len(specialties) * len(cities)
        current = 0
        
        try:
            for specialty in specialties:
                for city in cities:
                    current += 1
                    logger.info(f"[{current}/{total}] Scraping {specialty} in {city}")
                    
                    page = 1
                    while True:
                        task = ScrapeTask(specialty=specialty, city=city, page=page)
                        result = self.scrape_listing_page(browser, task)
                        
                        if not result.has_more_pages or not result.success:
                            break
                        
                        page += 1
                        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.browser_pool.release(browser)
            self.browser_pool.close_all()
        
        return self._stats
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        with self._stats_lock:
            return self._stats.copy()
    
    def close(self) -> None:
        """Close all resources."""
        self.browser_pool.close_all()
