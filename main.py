#!/usr/bin/env python3
"""
Doctoralia Spain Doctor Scraper - Main Entry Point

This scraper collects doctor contact information from doctoralia.es
using multi-threading and Spanish proxies for reliable data collection.

Features:
- Multi-threaded scraping with configurable workers
- Spanish proxy support with automatic rotation
- Dynamic data collection (no pre-link gathering)
- Automatic retry on failures
- Rate limiting to avoid blocks
- Multiple output formats (CSV, JSON, SQLite)
- Resume capability via SQLite deduplication

Usage:
    python main.py                          # Full scrape with defaults
    python main.py --specialties dentista   # Specific specialty
    python main.py --cities madrid barcelona # Specific cities
    python main.py --workers 3              # Custom thread count
    python main.py --test                   # Test mode (limited scrape)
"""

import os
import sys
import argparse
import signal
from typing import Optional

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import get_config, SPANISH_CITIES, MEDICAL_SPECIALTIES
from src.scraper import DynamicScraper
from src.browser_scraper import BrowserScraper
from src.storage import StorageManager
from src.proxy_manager import get_proxy_manager
from src.utils import setup_logging, print_banner, print_stats, estimate_scrape_time


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Scrape doctor contacts from Doctoralia Spain',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                    # Full scrape
  python main.py --test                             # Test with 2 specialties, 3 cities
  python main.py --specialties dentista ginecologo  # Specific specialties
  python main.py --cities madrid barcelona          # Specific cities
  python main.py --workers 5 --output json          # 5 threads, JSON output
  python main.py --proxy-file proxies.txt           # Use proxy file
        """
    )
    
    # Scraping options
    parser.add_argument(
        '--specialties', '-s',
        nargs='+',
        help='Specific specialties to scrape (default: all)'
    )
    parser.add_argument(
        '--cities', '-c',
        nargs='+',
        help='Specific cities to scrape (default: all major Spanish cities)'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=5,
        help='Number of worker threads (default: 5)'
    )
    
    # Output options
    parser.add_argument(
        '--output', '-o',
        choices=['csv', 'json'],
        default='csv',
        help='Output format (default: csv)'
    )
    parser.add_argument(
        '--output-dir',
        default='data',
        help='Output directory (default: data)'
    )
    
    # Proxy options
    parser.add_argument(
        '--proxy-file',
        help='Path to file containing proxies (one per line)'
    )
    parser.add_argument(
        '--no-proxy',
        action='store_true',
        help='Disable proxy usage'
    )
    parser.add_argument(
        '--free-proxies',
        action='store_true',
        help='Try to use free Spanish proxies (unreliable)'
    )
    
    # Rate limiting
    parser.add_argument(
        '--rate-limit',
        type=int,
        default=30,
        help='Requests per minute (default: 30)'
    )
    
    # Browser mode
    parser.add_argument(
        '--browser',
        action='store_true',
        help='Use browser mode (bypasses Cloudflare, slower but more reliable)'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode (default: True)'
    )
    parser.add_argument(
        '--show-browser',
        action='store_true',
        help='Show browser window (disables headless)'
    )
    
    # Other options
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: scrape only 2 specialties and 3 cities'
    )
    parser.add_argument(
        '--estimate',
        action='store_true',
        help='Show time/data estimates and exit'
    )
    parser.add_argument(
        '--list-specialties',
        action='store_true',
        help='List all available specialties and exit'
    )
    parser.add_argument(
        '--list-cities',
        action='store_true',
        help='List all available cities and exit'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def setup_signal_handlers(scraper: DynamicScraper) -> None:
    """Setup graceful shutdown handlers."""
    def signal_handler(signum, frame):
        print("\n\nReceived interrupt signal. Stopping gracefully...")
        scraper.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main() -> int:
    """Main entry point."""
    args = parse_arguments()
    
    # Handle info commands
    if args.list_specialties:
        print("Available Medical Specialties:")
        print("-" * 40)
        for i, spec in enumerate(MEDICAL_SPECIALTIES, 1):
            print(f"  {i:2}. {spec}")
        return 0
    
    if args.list_cities:
        print("Available Spanish Cities:")
        print("-" * 40)
        for i, city in enumerate(SPANISH_CITIES, 1):
            print(f"  {i:2}. {city}")
        return 0
    
    # Determine specialties and cities
    specialties = args.specialties or MEDICAL_SPECIALTIES
    cities = args.cities or SPANISH_CITIES
    
    if args.test:
        specialties = specialties[:2]
        cities = cities[:3]
        print("TEST MODE: Limited to 2 specialties and 3 cities")
    
    # Show estimates
    if args.estimate:
        estimates = estimate_scrape_time(
            len(specialties),
            len(cities),
            requests_per_minute=args.rate_limit
        )
        print("\nScraping Estimates:")
        print("-" * 40)
        print(f"Specialty-City Combinations: {estimates['total_combinations']:,}")
        print(f"Estimated Listing Pages:     {estimates['estimated_listing_pages']:,}")
        print(f"Estimated Doctor Profiles:   {estimates['estimated_doctor_profiles']:,}")
        print(f"Total Requests:              {estimates['total_requests']:,}")
        print(f"Estimated Time:              {estimates['estimated_hours']:.1f} hours ({estimates['estimated_days']:.1f} days)")
        return 0
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_logging(log_level=log_level, log_dir="logs")
    
    # Print banner
    print_banner()
    
    # Setup configuration
    config = get_config()
    config.MAX_WORKERS = args.workers
    config.REQUESTS_PER_MINUTE = args.rate_limit
    config.OUTPUT_FORMAT = args.output
    config.OUTPUT_DIR = args.output_dir
    
    # Setup proxy manager
    proxy_manager = get_proxy_manager()
    
    if not args.no_proxy:
        # Load proxies from environment
        proxy_manager.load_from_env()
        
        # Load from file if specified
        if args.proxy_file:
            proxy_manager.load_from_file(args.proxy_file)
        
        # Try free proxies if requested
        if args.free_proxies:
            print("Fetching free Spanish proxies (may be unreliable)...")
            proxy_manager.add_free_spanish_proxies()
        
        proxy_count = proxy_manager.get_working_count()
        if proxy_count > 0:
            print(f"Loaded {proxy_count} proxies")
        else:
            print("WARNING: No proxies loaded. Scraping without proxies may result in blocks.")
            print("Consider using --proxy-file or setting PROXY_LIST environment variable.")
    
    # Setup storage
    storage = StorageManager(
        output_dir=config.OUTPUT_DIR,
        output_format=config.OUTPUT_FORMAT,
        db_path=config.DB_PATH
    )
    
    # Determine headless mode
    headless = not args.show_browser
    
    # Print scraping info
    print(f"\nScraping Configuration:")
    print(f"  Specialties: {len(specialties)}")
    print(f"  Cities: {len(cities)}")
    print(f"  Mode: {'Browser' if args.browser else 'HTTP Requests'}")
    if not args.browser:
        print(f"  Workers: {args.workers}")
    print(f"  Rate Limit: {args.rate_limit} req/min")
    print(f"  Output: {args.output.upper()} -> {args.output_dir}/")
    print()
    
    # Create appropriate scraper
    if args.browser:
        print("Using BROWSER MODE - This bypasses Cloudflare but is slower")
        print(f"Browser: {'Headless' if headless else 'Visible'}")
        scraper = BrowserScraper(
            config=config,
            storage=storage,
            headless=headless,
            browser_count=1
        )
    else:
        scraper = DynamicScraper(
            config=config,
            storage=storage,
            proxy_manager=proxy_manager if not args.no_proxy else None
        )
        # Setup signal handlers (only for threaded scraper)
        setup_signal_handlers(scraper)
    
    # Run scraper
    try:
        if args.browser:
            stats = scraper.scrape_all(
                specialties=specialties,
                cities=cities
            )
        else:
            stats = scraper.scrape_all(
                specialties=specialties,
                cities=cities,
                max_workers=args.workers
            )
        
        # Print final stats
        print_stats(stats)
        
        # Export final CSV
        output_file = storage.get_output_filepath()
        print(f"\nData saved to: {output_file}")
        
        # Also export from database
        final_csv = storage.export_final_csv()
        print(f"Final export: {final_csv}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        stats = scraper.get_stats()
        print_stats(stats)
        return 1
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
