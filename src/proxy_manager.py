"""Proxy management for Spanish proxies."""
import os
import random
import logging
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
from threading import Lock
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class Proxy:
    """Proxy configuration."""
    protocol: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    country: str = "ES"
    is_working: bool = True
    fail_count: int = 0
    success_count: int = 0
    
    def get_url(self) -> str:
        """Get proxy URL string."""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"
    
    def get_dict(self) -> Dict[str, str]:
        """Get proxy dict for requests library."""
        url = self.get_url()
        return {
            "http": url,
            "https": url
        }


class ProxyManager:
    """Manages proxy rotation and health checking."""
    
    def __init__(self):
        self._proxies: deque = deque()
        self._lock = Lock()
        self._current_proxy: Optional[Proxy] = None
        self._request_count = 0
        self._rotation_interval = 10
        
    def load_from_env(self) -> None:
        """Load proxies from environment variable."""
        proxy_list = os.getenv("PROXY_LIST", "")
        if proxy_list:
            for proxy_str in proxy_list.split(","):
                proxy = self._parse_proxy_string(proxy_str.strip())
                if proxy:
                    self._proxies.append(proxy)
        logger.info(f"Loaded {len(self._proxies)} proxies from environment")
    
    def load_from_file(self, filepath: str) -> None:
        """Load proxies from a file (one per line)."""
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxy = self._parse_proxy_string(line)
                        if proxy:
                            self._proxies.append(proxy)
            logger.info(f"Loaded {len(self._proxies)} proxies from {filepath}")
        except FileNotFoundError:
            logger.warning(f"Proxy file not found: {filepath}")
    
    def load_from_provider(self, api_url: str, api_key: Optional[str] = None) -> None:
        """Load proxies from a proxy provider API."""
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Handle common API response formats
            proxies_data = data if isinstance(data, list) else data.get("proxies", [])
            
            for proxy_data in proxies_data:
                if isinstance(proxy_data, str):
                    proxy = self._parse_proxy_string(proxy_data)
                elif isinstance(proxy_data, dict):
                    proxy = Proxy(
                        protocol=proxy_data.get("protocol", "http"),
                        host=proxy_data.get("host", proxy_data.get("ip")),
                        port=int(proxy_data.get("port")),
                        username=proxy_data.get("username"),
                        password=proxy_data.get("password"),
                        country=proxy_data.get("country", "ES")
                    )
                else:
                    continue
                    
                if proxy:
                    self._proxies.append(proxy)
                    
            logger.info(f"Loaded {len(self._proxies)} proxies from provider")
        except Exception as e:
            logger.error(f"Failed to load proxies from provider: {e}")
    
    def add_free_proxies(self, test_proxies: bool = True, max_proxies: int = 50) -> None:
        """Add free proxies from multiple sources with optional testing."""
        import concurrent.futures
        
        # Multiple free proxy APIs - prioritizing more reliable sources
        free_proxy_apis = [
            "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&ssl=yes&anonymity=elite",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        ]
        
        all_proxies = []
        
        for api_url in free_proxy_apis:
            try:
                response = requests.get(api_url, timeout=10)
                if response.status_code == 200:
                    for line in response.text.strip().split("\n"):
                        line = line.strip()
                        if ":" in line and line[0].isdigit():
                            parts = line.split(":")
                            if len(parts) >= 2:
                                try:
                                    proxy = Proxy(
                                        protocol="http",
                                        host=parts[0],
                                        port=int(parts[1]),
                                        country="UNKNOWN"
                                    )
                                    all_proxies.append(proxy)
                                except ValueError:
                                    continue
            except Exception as e:
                logger.debug(f"Failed to fetch from {api_url}: {e}")
        
        logger.info(f"Fetched {len(all_proxies)} proxies, testing...")
        
        if test_proxies and all_proxies:
            # Test proxies in parallel to find working ones
            working_proxies = []
            
            def test_proxy(proxy: Proxy) -> Optional[Proxy]:
                try:
                    test_url = "https://httpbin.org/ip"
                    resp = requests.get(
                        test_url,
                        proxies=proxy.get_dict(),
                        timeout=5
                    )
                    if resp.status_code == 200:
                        return proxy
                except Exception:
                    pass
                return None
            
            # Test in smaller batches with shorter timeout
            logger.info("Testing proxies (this may take 30-60 seconds)...")
            test_batch = all_proxies[:200]  # Test fewer proxies
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                futures = {executor.submit(test_proxy, p): p for p in test_batch}
                
                try:
                    for future in concurrent.futures.as_completed(futures, timeout=45):
                        try:
                            result = future.result(timeout=1)
                            if result:
                                working_proxies.append(result)
                                logger.info(f"Working proxy: {result.host}:{result.port}")
                                if len(working_proxies) >= max_proxies:
                                    # Cancel remaining futures
                                    for f in futures:
                                        f.cancel()
                                    break
                        except Exception:
                            pass
                except concurrent.futures.TimeoutError:
                    logger.info("Proxy testing timed out, using found proxies")
            
            self._proxies.extend(working_proxies)
            logger.info(f"Added {len(working_proxies)} verified working proxies")
        else:
            # Add without testing (less reliable)
            self._proxies.extend(all_proxies[:max_proxies])
            logger.info(f"Added {min(len(all_proxies), max_proxies)} proxies (untested)")
    
    def add_free_spanish_proxies(self) -> None:
        """Alias for add_free_proxies (kept for compatibility)."""
        self.add_free_proxies()
    
    def _parse_proxy_string(self, proxy_str: str) -> Optional[Proxy]:
        """Parse proxy string in format: protocol://user:pass@host:port or protocol://host:port"""
        try:
            # Remove protocol
            if "://" in proxy_str:
                protocol, rest = proxy_str.split("://", 1)
            else:
                protocol = "http"
                rest = proxy_str
            
            # Check for auth
            if "@" in rest:
                auth, host_port = rest.rsplit("@", 1)
                username, password = auth.split(":", 1)
            else:
                host_port = rest
                username = None
                password = None
            
            host, port = host_port.rsplit(":", 1)
            
            return Proxy(
                protocol=protocol,
                host=host,
                port=int(port),
                username=username,
                password=password
            )
        except Exception as e:
            logger.warning(f"Failed to parse proxy string '{proxy_str}': {e}")
            return None
    
    def get_proxy(self) -> Optional[Proxy]:
        """Get next proxy with rotation."""
        with self._lock:
            if not self._proxies:
                return None
            
            self._request_count += 1
            
            # Rotate proxy every N requests
            if self._request_count >= self._rotation_interval or self._current_proxy is None:
                self._request_count = 0
                # Move current to back of queue
                if self._current_proxy:
                    self._proxies.append(self._current_proxy)
                # Get next working proxy
                for _ in range(len(self._proxies)):
                    proxy = self._proxies.popleft()
                    if proxy.is_working:
                        self._current_proxy = proxy
                        return proxy
                    self._proxies.append(proxy)
                # All proxies failed, reset and try again
                for proxy in self._proxies:
                    proxy.is_working = True
                    proxy.fail_count = 0
                if self._proxies:
                    self._current_proxy = self._proxies.popleft()
                    return self._current_proxy
            
            return self._current_proxy
    
    def report_success(self, proxy: Proxy) -> None:
        """Report successful use of proxy."""
        with self._lock:
            proxy.success_count += 1
            proxy.is_working = True
    
    def report_failure(self, proxy: Proxy) -> None:
        """Report failed use of proxy."""
        with self._lock:
            proxy.fail_count += 1
            if proxy.fail_count >= 3:
                proxy.is_working = False
                logger.warning(f"Proxy {proxy.host}:{proxy.port} marked as not working")
    
    def get_working_count(self) -> int:
        """Get count of working proxies."""
        return sum(1 for p in self._proxies if p.is_working) + (1 if self._current_proxy and self._current_proxy.is_working else 0)
    
    def has_proxies(self) -> bool:
        """Check if any proxies are available."""
        return len(self._proxies) > 0 or self._current_proxy is not None


# Singleton instance
_proxy_manager: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    """Get or create proxy manager singleton."""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
