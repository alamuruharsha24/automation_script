# RSS News Scraper

A comprehensive web scraping tool for collecting news articles from RSS feeds across multiple countries. This script can extract news data from 20+ countries and 37+ news sources, with support for various output formats and advanced features.

## Important Note on Historical Data

**Please be aware that standard RSS feeds typically only contain the most recent articles (e.g., the last 10-50 items) and do not provide access to deep historical archives.**

The `--days` parameter in this script **filters the articles currently available in the live RSS feed** based on their publication date. It **cannot retrieve articles older than what the feed currently offers.**

To collect a true historical dataset (e.g., for the past year), you would need to:

1.  **Run this script continuously** (e.g., daily) over the desired period and aggregate the results.
2.  **Utilize external news archive services or APIs** that specialize in storing historical news data (these often require subscriptions or have usage limits).

This script provides a robust way to capture *current* news from RSS feeds but is limited by the nature of RSS itself regarding historical depth.

## Installation

### Prerequisites
- Python 3.6 or higher
- Internet connection

### Dependencies
Install the required Python packages using pip:

```bash
pip install requests feedparser
```

For optional advanced features:
```bash
# For language detection
pip install langdetect

# For API server functionality
pip install flask
```

## Usage

### Basic Usage
Run the script with default settings (all countries, JSON output, filtering items published within the last 365 days *if present in the feed*):

```bash
python human_news_scraper.py
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--format FORMAT` | Output format: json, csv, or sqlite (default: json) |
| `--countries COUNTRIES` | Comma-separated list of country codes to scrape (default: all) |
| `--days DAYS` | Filter items *currently in the feed* published within the last DAYS days (default: 365). **See note on Historical Data above.** |
| `--detect-languages` | Enable language detection |
| `--workers WORKERS` | Maximum number of parallel workers (default: 5) |
| `--output OUTPUT` | Output filename (without extension) |
| `--validate` | Validate feeds without scraping |
| `--stats` | Show statistics only without scraping |
| `--api` | Start API server after scraping |

### Examples

Scrape news from specific countries:
```bash
python human_news_scraper.py --countries us,uk,jp
```

Scrape recent news (last 30 days *present in the feed*) with language detection:
```bash
python human_news_scraper.py --days 30 --detect-languages
```

Save output in CSV format:
```bash
python human_news_scraper.py --format csv --output news_data
```

Validate all configured feeds:
```bash
python human_news_scraper.py --validate
```

Show feed statistics:
```bash
python human_news_scraper.py --stats
```

Run with API server (requires Flask):
```bash
python human_news_scraper.py --format sqlite --api
```

## Bonus Features

### 1. Multi-threaded Scraping
The script uses Python's `concurrent.futures` module to scrape multiple feeds in parallel, significantly improving performance.

### 2. Language Detection
When enabled with the `--detect-languages` flag, the script can automatically detect the language of each article using the `langdetect` library.

### 3. API Server
The script can start a Flask-based API server to provide programmatic access to the scraped data. Available endpoints:
- `/api/news` - Get news items with optional filtering
- `/api/countries` - Get list of available countries
- `/api/sources` - Get list of available sources
- `/api/stats` - Get statistics about the news data

### 4. Robust Error Handling
The script implements comprehensive error handling with:
- Rate limiting to avoid overloading servers
- Exponential backoff for retries
- Timeout handling
- Encoding normalization

### 5. Multiple Storage Formats
Support for multiple output formats:
- JSON (with proper UTF-8 encoding)
- CSV (with proper handling of list fields)
- SQLite database (with query support)

## Issues and Solutions

### Common Issues

1.  **Historical Data Limitation**: As noted above, RSS feeds do not provide deep historical data. The `--days` flag only filters items currently in the feed.

2.  **Rate Limiting**: Some news sources may implement rate limiting. The script handles this with automatic backoff and retry logic.

3.  **Feed Availability**: RSS feeds occasionally change URLs or become temporarily unavailable. Use the `--validate` option to check feed status before scraping.

4.  **Memory Usage**: When scraping a large number of feeds, memory usage may increase. Adjust the `--days` parameter or scrape fewer countries at once.

5.  **Language Detection Accuracy**: The language detection feature may not be 100% accurate for very short texts or mixed-language content.

### Troubleshooting

If you encounter issues:

1. Check your internet connection
2. Verify feed URLs with the `--validate` option
3. Try reducing the number of parallel workers with `--workers 2`
4. Check the log file in the `logs` directory for detailed error messages

## Data Structure

Each news item contains the following fields:

- `id`: Unique identifier
- `title`: Article title
- `publication_date`: Publication date in ISO format
- `source`: News source name
- `country`: Country of origin
- `summary`: Article summary or description
- `url`: Link to the full article
- `language`: Language code from the feed
- `categories`: Article categories/tags
- `author`: Article author (if available)
- `detected_language`: Detected language (if language detection is enabled)
