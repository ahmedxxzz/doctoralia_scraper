"""Utility functions for the scraper."""
import os
import sys
import logging
from datetime import datetime
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    log_to_file: bool = True
) -> logging.Logger:
    """
    Configure logging for the scraper.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files
        log_to_file: Whether to also log to file
    
    Returns:
        Root logger instance
    """
    os.makedirs(log_dir, exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"scraper_{timestamp}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def validate_proxy_format(proxy_str: str) -> bool:
    """Validate proxy string format."""
    import re
    
    patterns = [
        r'^(http|https|socks4|socks5)://[\w.-]+:\d+$',
        r'^(http|https|socks4|socks5)://[\w.-]+:[\w.-]+@[\w.-]+:\d+$',
        r'^[\w.-]+:\d+$',
    ]
    
    for pattern in patterns:
        if re.match(pattern, proxy_str):
            return True
    return False


def estimate_scrape_time(
    num_specialties: int,
    num_cities: int,
    avg_pages_per_combo: int = 5,
    avg_doctors_per_page: int = 20,
    requests_per_minute: int = 30
) -> dict:
    """
    Estimate scraping time and data volume.
    
    Returns:
        Dictionary with estimates
    """
    total_combinations = num_specialties * num_cities
    total_listing_pages = total_combinations * avg_pages_per_combo
    total_profile_pages = total_listing_pages * avg_doctors_per_page
    total_requests = total_listing_pages + total_profile_pages
    
    # Time calculation
    minutes = total_requests / requests_per_minute
    hours = minutes / 60
    
    return {
        'total_combinations': total_combinations,
        'estimated_listing_pages': total_listing_pages,
        'estimated_doctor_profiles': total_profile_pages,
        'total_requests': total_requests,
        'estimated_minutes': round(minutes, 1),
        'estimated_hours': round(hours, 1),
        'estimated_days': round(hours / 24, 2)
    }


def print_banner() -> None:
    """Print scraper banner."""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║           DOCTORALIA SPAIN DOCTOR SCRAPER v1.0                ║
║                                                               ║
║  Multi-threaded scraper with Spanish proxy support            ║
║  Dynamic data collection - no pre-link gathering              ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_stats(stats: dict) -> None:
    """Print scraping statistics."""
    print("\n" + "=" * 50)
    print("SCRAPING STATISTICS")
    print("=" * 50)
    print(f"Pages Scraped:    {stats.get('pages_scraped', 0):,}")
    print(f"Doctors Found:    {stats.get('doctors_found', 0):,}")
    print(f"Doctors Saved:    {stats.get('doctors_saved', 0):,}")
    print(f"Errors:           {stats.get('errors', 0):,}")
    print("=" * 50)


def get_spanish_proxy_providers() -> list:
    """Get list of recommended Spanish proxy providers."""
    return [
        {
            'name': 'Bright Data (Luminati)',
            'url': 'https://brightdata.com',
            'features': ['Residential IPs', 'Spanish IPs', 'API access'],
            'pricing': 'From $500/month'
        },
        {
            'name': 'Oxylabs',
            'url': 'https://oxylabs.io',
            'features': ['Datacenter & Residential', 'Spanish locations', 'High speed'],
            'pricing': 'From $180/month'
        },
        {
            'name': 'Smartproxy',
            'url': 'https://smartproxy.com',
            'features': ['Residential proxies', 'Spain pool', 'Pay as you go'],
            'pricing': 'From $75/month'
        },
        {
            'name': 'IPRoyal',
            'url': 'https://iproyal.com',
            'features': ['Residential & Datacenter', 'Spanish IPs', 'Affordable'],
            'pricing': 'From $1.75/GB'
        },
        {
            'name': 'ProxyScrape',
            'url': 'https://proxyscrape.com',
            'features': ['Free Spanish proxies', 'API access', 'Limited reliability'],
            'pricing': 'Free tier available'
        }
    ]
