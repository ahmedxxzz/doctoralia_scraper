# Doctoralia Spain Doctor Scraper

A high-performance, multi-threaded Python scraper for collecting doctor contact information from [Doctoralia.es](https://www.doctoralia.es).

## Features

- **Multi-threaded scraping** - Configurable worker threads for parallel processing
- **Spanish proxy support** - Built-in proxy rotation with health checking
- **Dynamic data collection** - No pre-link gathering; data extracted on-the-fly
- **Automatic retry logic** - Exponential backoff on failures
- **Rate limiting** - Configurable requests per minute to avoid blocks
- **Multiple output formats** - CSV, JSON, and SQLite database
- **Resume capability** - SQLite deduplication prevents re-scraping
- **Graceful shutdown** - Ctrl+C saves progress before exit

## Installation

```bash
# Clone or navigate to the project
cd doctoralia_scraper

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Key settings:
```env
# Proxy Configuration (Spanish Proxies Required)
PROXY_LIST=socks5://user:pass@spain-proxy1.com:1080,http://user:pass@spain-proxy2.com:8080

# Scraper Settings
MAX_WORKERS=5
REQUESTS_PER_MINUTE=30
RETRY_ATTEMPTS=3
OUTPUT_FORMAT=csv
```

### Proxy Setup

For reliable scraping, you need Spanish proxies. Options:

1. **Paid Proxy Providers** (Recommended):
   - [Bright Data](https://brightdata.com) - Residential Spanish IPs
   - [Oxylabs](https://oxylabs.io) - High-speed datacenter proxies
   - [Smartproxy](https://smartproxy.com) - Pay-as-you-go residential
   - [IPRoyal](https://iproyal.com) - Affordable residential proxies

2. **Proxy File**: Create `proxies.txt` with one proxy per line:
   ```
   http://user:pass@spain-proxy1.com:8080
   socks5://user:pass@spain-proxy2.com:1080
   http://192.168.1.1:3128
   ```

3. **Free Proxies** (Unreliable): Use `--free-proxies` flag

## Usage

### Basic Usage

```bash
# Full scrape (all specialties, all cities)
python main.py

# Test mode (2 specialties, 3 cities)
python main.py --test

# Specific specialties
python main.py --specialties dentista ginecologo dermatologo

# Specific cities
python main.py --cities madrid barcelona valencia

# Custom configuration
python main.py --workers 5 --rate-limit 20 --output json
```

### Command Line Options

```
Options:
  --specialties, -s    Specific specialties to scrape
  --cities, -c         Specific cities to scrape
  --workers, -w        Number of worker threads (default: 5)
  --output, -o         Output format: csv or json (default: csv)
  --output-dir         Output directory (default: data)
  --proxy-file         Path to proxy file
  --no-proxy           Disable proxy usage
  --free-proxies       Try free Spanish proxies (unreliable)
  --rate-limit         Requests per minute (default: 30)
  --test               Test mode with limited scope
  --estimate           Show time estimates and exit
  --list-specialties   List all available specialties
  --list-cities        List all available cities
  --verbose, -v        Enable verbose logging
```

### Examples

```bash
# Estimate scraping time
python main.py --estimate

# List available specialties
python main.py --list-specialties

# Scrape dentists in Madrid with proxies
python main.py -s dentista -c madrid --proxy-file proxies.txt

# Full scrape with 10 workers, JSON output
python main.py --workers 10 --output json --proxy-file proxies.txt

# Test run without proxies (may get blocked)
python main.py --test --no-proxy
```

## Output

### CSV Format
```csv
name,title,specialty,city,phone,email,website,address_street,...
Dr. Juan García,Dr.,dentista,madrid,+34912345678,,https://...,Calle Mayor 1,...
```

### JSON Format
```json
[
  {
    "name": "Dr. Juan García",
    "title": "Dr.",
    "specialty": "dentista",
    "city": "madrid",
    "phone": "+34912345678",
    ...
  }
]
```

### SQLite Database
Located at `data/doctors.db` with full schema for querying.

## Data Collected

For each doctor, the scraper collects:

| Field | Description |
|-------|-------------|
| `name` | Doctor's full name |
| `title` | Dr./Dra. |
| `specialty` | Primary specialty |
| `sub_specialties` | Additional specializations |
| `city` | Practice city |
| `phone` | Contact phone number |
| `email` | Email address (if available) |
| `website` | Personal/clinic website |
| `address_street` | Street address |
| `address_city` | City |
| `address_postal_code` | Postal code |
| `clinic_name` | Clinic/practice name |
| `rating` | Average rating (0-5) |
| `review_count` | Number of reviews |
| `consultation_price` | In-person consultation price |
| `online_consultation_price` | Online consultation price |
| `insurance_accepted` | List of accepted insurances |
| `languages` | Languages spoken |
| `experience_years` | Years of experience |
| `education` | Education/training |
| `services` | Services offered |
| `diseases_treated` | Conditions treated |
| `profile_url` | Doctoralia profile URL |
| `scraped_at` | Timestamp of scraping |

## Architecture

```
doctoralia_scraper/
├── main.py              # Entry point with CLI
├── requirements.txt     # Dependencies
├── .env.example         # Environment template
├── src/
│   ├── __init__.py
│   ├── config.py        # Configuration & constants
│   ├── models.py        # Data models (Doctor, Address, etc.)
│   ├── parser.py        # HTML parsing logic
│   ├── scraper.py       # Core scraper with threading
│   ├── proxy_manager.py # Proxy rotation & health
│   ├── storage.py       # CSV, JSON, SQLite handlers
│   └── utils.py         # Logging & utilities
├── data/                # Output files
└── logs/                # Log files
```

## Scraping Strategy

1. **Task Generation**: Creates tasks for each specialty-city combination
2. **Multi-threaded Processing**: Workers pull tasks from queue
3. **Listing Page Scraping**: Extract doctor profile URLs from search results
4. **Dynamic Profile Scraping**: Immediately scrape each profile (no link pre-collection)
5. **Immediate Storage**: Save data as soon as extracted
6. **Pagination Handling**: Automatically follow "next page" links
7. **Deduplication**: SQLite prevents re-scraping same profiles

## Rate Limiting & Anti-Detection

- Configurable requests per minute
- Random delays between requests (1-3 seconds)
- User-Agent rotation
- Proxy rotation every 10 requests
- CloudScraper for JavaScript challenge bypass
- Exponential backoff on failures

## Legal Disclaimer

This tool is for educational purposes only. Before scraping:

1. Review Doctoralia's Terms of Service
2. Check `robots.txt` at https://www.doctoralia.es/robots.txt
3. Respect rate limits to avoid server strain
4. Use data responsibly and in compliance with GDPR
5. Consider reaching out to Doctoralia for official API access

The authors are not responsible for misuse of this tool.

## Troubleshooting

### Getting Blocked (403/429 errors)
- Use Spanish residential proxies
- Reduce `--rate-limit` to 10-15
- Reduce `--workers` to 2-3

### No Data Extracted
- Check if page structure changed
- Enable `--verbose` for debug logs
- Test with `--test --no-proxy` first

### Slow Performance
- Increase `--workers` (with good proxies)
- Increase `--rate-limit` (with good proxies)

## License

MIT License - Use responsibly.
