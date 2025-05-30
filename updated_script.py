#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard library imports
import os
import sys
import logging
import argparse
import time
import random
import json
import csv
import sqlite3
import re
import hashlib
import concurrent.futures
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional, Tuple, Set

# Third-party imports
import requests
import feedparser

# Optional imports for advanced features
try:
    from langdetect import detect as detect_language_lib
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from flask import Flask, jsonify, request
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

# --- Configuration --- 

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_FILE = os.path.join(LOG_DIR, f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Network Requests
REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 0.5
RATE_LIMIT_DELAY = 1.0 # Min seconds between requests to the same domain

# Data Processing
DEFAULT_ENCODING = "utf-8"
HISTORICAL_DATA_DAYS = 365

# Storage
DEFAULT_STORAGE_FORMAT = "json"
CSV_DELIMITER = ","
JSON_INDENT = 2
DATABASE_PATH = os.path.join(OUTPUT_DIR, "news_data.db")

# API Server (Optional)
API_HOST = "0.0.0.0"
API_PORT = 5000
API_DEBUG = False

# RSS Feed Sources (Add more sources as needed)
RSS_FEEDS = {
    "us": {
        "country_name": "United States",
        "feeds": [
            {"name": "CNN", "url": "http://rss.cnn.com/rss/edition.rss", "language": "en"},
            {"name": "New York Times", "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "language": "en"},
            {"name": "Washington Post", "url": "http://feeds.washingtonpost.com/rss/world", "language": "en"},
        ]
    },
    "uk": {
        "country_name": "United Kingdom",
        "feeds": [
            {"name": "BBC News", "url": "http://feeds.bbci.co.uk/news/rss.xml", "language": "en"},
            {"name": "The Guardian", "url": "https://www.theguardian.com/world/rss", "language": "en"},
            {"name": "The Independent", "url": "https://www.independent.co.uk/news/world/rss", "language": "en"},
        ]
    },
    "ca": {
        "country_name": "Canada",
        "feeds": [
            {"name": "CBC News", "url": "https://www.cbc.ca/cmlink/rss-topstories", "language": "en"},
            {"name": "Global News", "url": "https://globalnews.ca/feed/", "language": "en"},
        ]
    },
    "au": {
        "country_name": "Australia",
        "feeds": [
            {"name": "ABC News", "url": "https://www.abc.net.au/news/feed/51120/rss.xml", "language": "en"},
            {"name": "Sydney Morning Herald", "url": "https://www.smh.com.au/rss/feed.xml", "language": "en"},
        ]
    },
    "in": {
        "country_name": "India",
        "feeds": [
            {"name": "The Times of India", "url": "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms", "language": "en"},
            {"name": "The Hindu", "url": "https://www.thehindu.com/news/national/feeder/default.rss", "language": "en"},
            {"name": "NDTV", "url": "https://feeds.feedburner.com/ndtvnews-top-stories", "language": "en"},
        ]
    },
    "sg": {
        "country_name": "Singapore",
        "feeds": [
            {"name": "The Straits Times", "url": "https://www.straitstimes.com/news/asia/rss.xml", "language": "en"},
            {"name": "Channel News Asia", "url": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511", "language": "en"},
        ]
    },
    "my": {
        "country_name": "Malaysia",
        "feeds": [
            {"name": "The Star", "url": "https://www.thestar.com.my/rss/News", "language": "en"},
            # {"name": "New Straits Times", "url": "https://www.nst.com.my/rss/news", "language": "en"}, # Often 404
        ]
    },
    "id": {
        "country_name": "Indonesia",
        "feeds": [
            # {"name": "Jakarta Post", "url": "https://www.thejakartapost.com/news/feed", "language": "en"}, # Often 404
            {"name": "Antara News", "url": "https://en.antaranews.com/rss/news.xml", "language": "en"},
        ]
    },
    "kr": {
        "country_name": "South Korea",
        "feeds": [
            # {"name": "Korea Herald", "url": "http://www.koreaherald.com/rss/front.xml", "language": "en"}, # Often empty
            {"name": "Korea Times", "url": "http://www.koreatimes.co.kr/www/rss/rss.xml", "language": "en"},
        ]
    },
    "jp": {
        "country_name": "Japan",
        "feeds": [
            {"name": "NHK World", "url": "https://www3.nhk.or.jp/rss/news/cat0.xml", "language": "ja"},
            {"name": "Japan Times", "url": "https://www.japantimes.co.jp/feed/", "language": "en"},
        ]
    },
    "cn": {
        "country_name": "China",
        "feeds": [
            {"name": "China Daily", "url": "http://www.chinadaily.com.cn/rss/world_rss.xml", "language": "en"},
            {"name": "Global Times", "url": "https://www.globaltimes.cn/rss/outbrain.xml", "language": "en"},
        ]
    },
    "fr": {
        "country_name": "France",
        "feeds": [
            {"name": "France 24", "url": "https://www.france24.com/en/rss", "language": "en"},
            {"name": "Le Monde", "url": "https://www.lemonde.fr/en/rss/une.xml", "language": "en"},
        ]
    },
    "de": {
        "country_name": "Germany",
        "feeds": [
            {"name": "Deutsche Welle", "url": "https://rss.dw.com/rdf/rss-en-all", "language": "en"},
            {"name": "Der Spiegel", "url": "https://www.spiegel.de/international/index.rss", "language": "en"},
        ]
    },
    "it": {
        "country_name": "Italy",
        "feeds": [
            {"name": "ANSA", "url": "https://www.ansa.it/english/english_rss.xml", "language": "en"},
            # {"name": "Italy24 News", "url": "https://www.italy24news.com/feed", "language": "en"}, # Often 520 error
        ]
    },
    "es": {
        "country_name": "Spain",
        "feeds": [
            {"name": "El País", "url": "https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada", "language": "en"},
            {"name": "The Local Spain", "url": "https://feeds.thelocal.com/rss/es", "language": "en"},
        ]
    },
    "ru": {
        "country_name": "Russia",
        "feeds": [
            {"name": "RT News", "url": "https://www.rt.com/rss/", "language": "en"},
            {"name": "TASS", "url": "https://tass.com/rss/v2.xml", "language": "en"},
        ]
    },
    "br": {
        "country_name": "Brazil",
        "feeds": [
            {"name": "Brazil Reports", "url": "https://brazilreports.com/feed/", "language": "en"},
            {"name": "The Rio Times", "url": "https://www.riotimesonline.com/feed/", "language": "en"},
        ]
    },
    "za": {
        "country_name": "South Africa",
        "feeds": [
            {"name": "News24", "url": "https://feeds.news24.com/articles/news24/World/rss", "language": "en"},
            {"name": "Mail & Guardian", "url": "https://mg.co.za/feed/", "language": "en"},
        ]
    },
    "ng": {
        "country_name": "Nigeria",
        "feeds": [
            # {"name": "The Guardian Nigeria", "url": "https://guardian.ng/feed/", "language": "en"}, # Often 403
            {"name": "Vanguard News", "url": "https://www.vanguardngr.com/feed/", "language": "en"},
        ]
    },
    "ae": {
        "country_name": "United Arab Emirates",
        "feeds": [
            # {"name": "Gulf News", "url": "https://gulfnews.com/rss/world", "language": "en"}, # Often 404
            # {"name": "Khaleej Times", "url": "https://www.khaleejtimes.com/rss/international.xml", "language": "en"}, # Often 404
        ]
    },
    "qa": {
        "country_name": "Qatar",
        "feeds": [
            {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "language": "en"},
            # {"name": "The Peninsula", "url": "https://thepeninsulaqatar.com/rss/feed/home", "language": "en"}, # Often 404
        ]
    },
}

# --- Logging Setup --- 

logger = logging.getLogger("NewsScraper")

def setup_logger(log_file: str, log_level: str = "INFO"):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    logging.basicConfig(
        level=numeric_level,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

# --- Utility Functions --- 

_last_request_time: Dict[str, float] = {}

def make_request(url: str) -> Optional[requests.Response]:
    # Makes an HTTP GET request with rate limiting and retries
    domain = urlparse(url).netloc
    
    # Rate limiting
    current_time = time.time()
    if domain in _last_request_time:
        elapsed = current_time - _last_request_time[domain]
        if elapsed < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - elapsed
            logger.debug(f"Rate limiting: Sleeping for {sleep_time:.2f}s before requesting {domain}")
            time.sleep(sleep_time)
    
    _last_request_time[domain] = time.time()
    
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            logger.debug(f"Requesting URL: {url}")
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                return response
            
            if response.status_code == 429:
                logger.warning(f"Rate limit exceeded for {url}. Status code: {response.status_code}")
                sleep_time = RETRY_BACKOFF_FACTOR * (2 ** retries) + random.uniform(1, 3)
                time.sleep(sleep_time)
            elif response.status_code >= 500:
                logger.warning(f"Server error for {url}. Status code: {response.status_code}")
            else:
                logger.warning(f"HTTP error for {url}. Status code: {response.status_code}")
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout for {url}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error for {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for {url}: {str(e)}")
        
        retries += 1
        if retries <= MAX_RETRIES:
            sleep_time = RETRY_BACKOFF_FACTOR * (2 ** retries) + random.uniform(0, 1)
            logger.info(f"Retrying {url} in {sleep_time:.2f} seconds (attempt {retries}/{MAX_RETRIES})")
            time.sleep(sleep_time)
    
    logger.error(f"Failed to retrieve {url} after {MAX_RETRIES} retries")
    return None

def parse_date(date_str: str) -> Optional[datetime]:
    # Parses date strings from feeds into datetime objects
    if not date_str:
        return None
        
    formats = [
        "%a, %d %b %Y %H:%M:%S %z", # RFC 822
        "%a, %d %b %Y %H:%M:%S %Z", # RFC 822 with timezone name
        "%Y-%m-%dT%H:%M:%S%z",       # ISO 8601
        "%Y-%m-%dT%H:%M:%SZ",        # ISO 8601 UTC
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            # Need to handle timezone info carefully
            dt = datetime.strptime(date_str, fmt)
            # If timezone is naive, make it aware (assume UTC if not specified)
            # This part might need refinement based on feed specifics
            # For simplicity here, we'll often rely on feedparser's parsed time struct
            return dt 
        except ValueError:
            continue
    
    # Fallback using feedparser's more robust parsing if direct strptime fails
    try:
        parsed_time = feedparser._parse_date(date_str)
        if parsed_time:
             # Convert time.struct_time to datetime
             # Note: feedparser's parsed dates are generally timezone-aware (UTC)
            return datetime.fromtimestamp(time.mktime(parsed_time))
    except Exception:
        pass # Ignore errors in fallback

    logger.warning(f"Could not parse date: {date_str}")
    return None

def clean_html(html_text: str) -> str:
    # Removes HTML tags and entities from text
    if not html_text:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', html_text)
    entities = {
        '&nbsp;': ' ', '&lt;': '<', '&gt;': '>', '&amp;': '&',
        '&quot;': '"', '&apos;': "'",
        '\u2018': "'", '\u2019': "'", '\u201c': '"', '\u201d': '"',
        '\u2013': '-', '\u2014': '--', '\u00a0': ' '
    }
    for entity, replacement in entities.items():
        clean_text = clean_text.replace(entity, replacement)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text

def generate_unique_id(title: str, source: str, date_str: str) -> str:
    # Creates a unique ID for a news item
    combined = f"{title}|{source}|{date_str}".encode('utf-8')
    return hashlib.md5(combined).hexdigest()

def is_within_timeframe(dt: Optional[datetime], days: int) -> bool:
    # Checks if a date is within the specified historical range
    if dt is None:
        return False
    
    # Make sure we compare aware with aware or naive with naive
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    cutoff_date = now - timedelta(days=days)
    
    try:
        return dt >= cutoff_date
    except TypeError:
        # Handle comparison issues between timezone-aware and naive datetimes
        # A simple approach: make cutoff naive if dt is naive
        if dt.tzinfo is None and cutoff_date.tzinfo is not None:
             return dt >= cutoff_date.replace(tzinfo=None)
        # Or make dt aware (assume UTC) if cutoff is aware
        elif dt.tzinfo is None and cutoff_date.tzinfo is not None:
             # This assumption might be wrong, date parsing needs care
             # For now, let's log a warning and potentially skip
             logger.warning(f"Timezone comparison issue for date {dt}")
             return False # Safer to exclude if unsure
        else:
             # Both aware or both naive (but different timezones?) - comparison might still fail
             logger.warning(f"Could not compare dates: {dt} and {cutoff_date}")
             return False

def detect_language(text: str) -> str:
    # Detects language using langdetect library if available
    if not LANGDETECT_AVAILABLE or not text:
        return "unknown"
    try:
        return detect_language_lib(text)
    except Exception as e:
        logger.warning(f"Language detection failed: {str(e)}")
        return "unknown"

# --- Core Scraping Logic --- 

processed_item_ids: Set[str] = set() # Global set to track processed items across feeds

def fetch_and_parse_feed(feed_url: str) -> Optional[feedparser.FeedParserDict]:
    # Fetches and parses a single RSS feed URL
    response = make_request(feed_url)
    if response is None:
        logger.error(f"Failed to retrieve feed: {feed_url}")
        return None
    
    try:
        feed = feedparser.parse(response.content)
        # Basic validation
        if feed.bozo:
             logger.warning(f"Feed is not well-formed: {feed_url}, Error: {feed.bozo_exception}")
             # Continue processing if entries exist
        if not hasattr(feed, 'entries') or len(feed.entries) == 0:
            # Check if it's just an empty feed vs parse error
            if not feed.bozo:
                 logger.info(f"Feed is valid but has no entries: {feed_url}")
            return None # Treat as empty or failed parse
            
        logger.info(f"Successfully fetched feed: {feed_url} ({len(feed.entries)} entries)")
        return feed
    except Exception as e:
        logger.error(f"Error parsing feed {feed_url}: {str(e)}")
        return None

def extract_news_items(feed_data: feedparser.FeedParserDict, 
                       source_name: str, country_name: str, 
                       historical_days: int) -> List[Dict[str, Any]]:
    # Extracts relevant information from feed entries
    items = []
    if feed_data is None or not hasattr(feed_data, 'entries'):
        return []

    for entry in feed_data.entries:
        # Get publication date using feedparser's parsed struct if possible
        pub_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                # feedparser dates are often UTC
                pub_date = datetime.fromtimestamp(time.mktime(entry.published_parsed)).replace(tzinfo=timedelta(0)) 
            except Exception:
                pub_date = parse_date(entry.get('published', '')) # Fallback
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
             try:
                pub_date = datetime.fromtimestamp(time.mktime(entry.updated_parsed)).replace(tzinfo=timedelta(0))
             except Exception:
                pub_date = parse_date(entry.get('updated', '')) # Fallback
        else:
            pub_date_str = entry.get('published', entry.get('updated', ''))
            pub_date = parse_date(pub_date_str)

        # Skip items outside the desired timeframe
        if not is_within_timeframe(pub_date, historical_days):
            continue

        title = clean_html(entry.get('title', '')).strip()
        if not title:
            continue # Skip items without titles

        link = entry.get('link', '')
        summary = clean_html(entry.get('summary', entry.get('description', '')))
        
        # Generate ID for deduplication
        item_id = generate_unique_id(title, source_name, str(pub_date))
        if item_id in processed_item_ids:
            continue # Skip duplicates already seen
        processed_item_ids.add(item_id)

        item = {
            'id': item_id,
            'title': title,
            'publication_date': pub_date.isoformat() if pub_date else '',
            'source': source_name,
            'country': country_name,
            'summary': summary,
            'url': link,
            'language': entry.get('language', feed_data.feed.get('language', '')), # Use entry lang, fallback to feed lang
            'categories': [tag.term for tag in entry.get('tags', []) if tag.term],
            'author': entry.get('author', ''),
        }
        items.append(item)
        
    logger.debug(f"Extracted {len(items)} items from {source_name} ({country_name})")
    return items

def scrape_single_feed(feed_info: Dict[str, Any], historical_days: int) -> List[Dict[str, Any]]:
    # Scrapes a single feed URL
    feed_url = feed_info['url']
    source_name = feed_info['name']
    country_name = feed_info['country_name']
    logger.info(f"Scraping feed: {source_name} ({country_name}) - {feed_url}")
    
    parsed_feed = fetch_and_parse_feed(feed_url)
    if parsed_feed is None:
        return []
        
    return extract_news_items(parsed_feed, source_name, country_name, historical_days)

def scrape_feeds_concurrently(feeds_to_scrape: List[Dict[str, Any]], 
                              historical_days: int, max_workers: int) -> List[Dict[str, Any]]:
    # Scrapes multiple feeds using a thread pool
    all_items = []
    logger.info(f"Starting concurrent scraping of {len(feeds_to_scrape)} feeds with {max_workers} workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_feed = {executor.submit(scrape_single_feed, feed, historical_days): feed 
                          for feed in feeds_to_scrape}
        
        for future in concurrent.futures.as_completed(future_to_feed):
            feed = future_to_feed[future]
            try:
                items = future.result()
                all_items.extend(items)
                logger.info(f"Completed scraping {feed['name']} ({feed['country_name']}): Found {len(items)} items")
            except Exception as e:
                logger.error(f"Error scraping {feed['name']} ({feed['country_name']}): {str(e)}")
                
    logger.info(f"Finished concurrent scraping. Total items found: {len(all_items)}")
    return all_items

# --- Data Processing --- 

def process_scraped_items(items: List[Dict[str, Any]], detect_lang: bool) -> List[Dict[str, Any]]:
    # Cleans, normalizes, and optionally detects language for scraped items
    processed_items = []
    seen_ids_in_batch = set() # Deduplicate within the final list as well
    
    logger.info(f"Processing {len(items)} scraped items...")
    
    for item in items:
        if item['id'] in seen_ids_in_batch:
            continue
            
        # Basic cleaning already done during extraction, focus on language detection here
        if detect_lang:
            item['detected_language'] = detect_language(item['summary'] or item['title'])
        else:
            item['detected_language'] = 'unknown'
            
        processed_items.append(item)
        seen_ids_in_batch.add(item['id'])
        
    logger.info(f"Finished processing. Returning {len(processed_items)} unique items.")
    return processed_items

# --- Storage Functions --- 

def save_to_json(items: List[Dict[str, Any]], filename: str):
    # Saves data to a JSON file
    output_path = os.path.join(OUTPUT_DIR, filename + ".json")
    try:
        with open(output_path, 'w', encoding=DEFAULT_ENCODING) as f:
            json.dump(items, f, indent=JSON_INDENT, ensure_ascii=False)
        logger.info(f"Saved {len(items)} items to JSON: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving to JSON {output_path}: {str(e)}")
        raise

def save_to_csv(items: List[Dict[str, Any]], filename: str):
    # Saves data to a CSV file
    output_path = os.path.join(OUTPUT_DIR, filename + ".csv")
    if not items:
        logger.warning("No items to save to CSV.")
        return output_path # Create empty file?
        
    fieldnames = list(items[0].keys()) # Use keys from first item
    try:
        with open(output_path, 'w', encoding=DEFAULT_ENCODING, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=CSV_DELIMITER)
            writer.writeheader()
            for item in items:
                # Convert list fields (like categories) to string
                row = {k: (';'.join(v) if isinstance(v, list) else v) for k, v in item.items()}
                writer.writerow(row)
        logger.info(f"Saved {len(items)} items to CSV: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving to CSV {output_path}: {str(e)}")
        raise

def save_to_sqlite(items: List[Dict[str, Any]], db_path: str):
    # Saves data to an SQLite database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Define table structure - ensure all expected keys are columns
        # Use a fixed set of columns based on potential item keys
        columns = [
            'id', 'title', 'publication_date', 'source', 'country', 
            'summary', 'url', 'language', 'categories', 'author', 
            'detected_language', 'created_at'
        ]
        column_defs = ", ".join([f"{col} TEXT" for col in columns])
        # Make 'id' the primary key
        column_defs = column_defs.replace("id TEXT", "id TEXT PRIMARY KEY")
        
        cursor.execute(f"CREATE TABLE IF NOT EXISTS news_items ({column_defs})")
        
        insert_query = f"INSERT OR REPLACE INTO news_items ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        
        rows_to_insert = []
        for item in items:
            item['created_at'] = datetime.now().isoformat()
            # Ensure all columns exist in the item, use None if missing
            row_values = []
            for col in columns:
                value = item.get(col)
                # Convert list fields (like categories) to string
                if isinstance(value, list):
                    value = ';'.join(value)
                row_values.append(value)
            rows_to_insert.append(tuple(row_values))
            
        cursor.executemany(insert_query, rows_to_insert)
        
        conn.commit()
        conn.close()
        logger.info(f"Saved/Updated {len(items)} items in SQLite database: {db_path}")
        return db_path
    except Exception as e:
        logger.error(f"Error saving to SQLite {db_path}: {str(e)}")
        raise

def query_sqlite(db_path: str, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    # Queries the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row # Return rows as dict-like objects
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        logger.info(f"Query returned {len(results)} results from SQLite: {db_path}")
        return results
    except Exception as e:
        logger.error(f"Error querying SQLite {db_path}: {str(e)}")
        return []

# --- Feed Validation & Stats --- 

def validate_feed(feed_url: str) -> Tuple[bool, Optional[str]]:
    # Checks if a feed URL is accessible and seems valid
    response = make_request(feed_url)
    if response is None:
        return False, "Failed to retrieve feed"
    try:
        feed = feedparser.parse(response.content)
        if feed.bozo:
            return False, f"Feed not well-formed: {feed.bozo_exception}"
        if not hasattr(feed, 'entries'): # Check if entries exist, even if empty
             return False, "Feed parsed but contains no 'entries' attribute"
        # Allow feeds with 0 entries but otherwise valid structure
        # if len(feed.entries) == 0:
        #     return False, "Feed has no entries"
        if not hasattr(feed, 'feed') or not hasattr(feed.feed, 'title'):
            return False, "Feed has no title"
        return True, None
    except Exception as e:
        logger.error(f"Error validating feed {feed_url}: {str(e)}")
        return False, str(e)

def get_feed_stats() -> Dict[str, Any]:
    # Gathers statistics about the configured feeds
    stats = {
        "total_countries": len(RSS_FEEDS),
        "total_feeds": 0,
        "feeds_per_country": {},
        "languages": {},
    }
    total_feeds = 0
    for country_code, country_data in RSS_FEEDS.items():
        country_name = country_data["country_name"]
        num_feeds = len(country_data["feeds"])
        stats["feeds_per_country"][country_name] = num_feeds
        total_feeds += num_feeds
        for feed in country_data["feeds"]:
            lang = feed.get("language", "unknown")
            stats["languages"][lang] = stats["languages"].get(lang, 0) + 1
            
    stats["total_feeds"] = total_feeds
    return stats

# --- API Server Setup (Optional) --- 

def setup_api_server(db_path):
    # Sets up a Flask API server if Flask is available
    if not FLASK_AVAILABLE:
        logger.error("Flask is not installed. Cannot start API server.")
        return None
    
    app = Flask(__name__)

    @app.route('/api/news', methods=['GET'])
    def get_news():
        country = request.args.get('country')
        source = request.args.get('source')
        language = request.args.get('language')
        days = request.args.get('days', type=int)
        
        query = "SELECT * FROM news_items WHERE 1=1"
        params = []
        if country: query += " AND country = ?"; params.append(country)
        if source: query += " AND source = ?"; params.append(source)
        if language: query += " AND (language = ? OR detected_language = ?)"; params.extend([language, language])
        if days: query += " AND publication_date >= datetime('now', ? || ' days')"; params.append(f"-{days}")
        query += " ORDER BY publication_date DESC"
        
        results = query_sqlite(db_path, query, tuple(params))
        return jsonify(results)

    @app.route('/api/countries', methods=['GET'])
    def get_countries():
        query = "SELECT DISTINCT country FROM news_items ORDER BY country"
        results = query_sqlite(db_path, query)
        return jsonify([item['country'] for item in results])

    @app.route('/api/sources', methods=['GET'])
    def get_sources():
        country = request.args.get('country')
        query = "SELECT DISTINCT source, country FROM news_items"
        params = []
        if country: query += " WHERE country = ?"; params.append(country)
        query += " ORDER BY source"
        results = query_sqlite(db_path, query, tuple(params))
        return jsonify(results)

    @app.route('/api/stats', methods=['GET'])
    def get_api_stats():
        # Simplified stats for API
        total_items = query_sqlite(db_path, "SELECT COUNT(*) as count FROM news_items")[0]['count']
        countries = query_sqlite(db_path, "SELECT country, COUNT(*) as count FROM news_items GROUP BY country")
        sources = query_sqlite(db_path, "SELECT source, COUNT(*) as count FROM news_items GROUP BY source")
        return jsonify({'total_items': total_items, 'countries': countries, 'sources': sources})

    return app

# --- Main Execution --- 

def parse_arguments():
    # Parses command line arguments
    parser = argparse.ArgumentParser(description='RSS News Scraper')
    parser.add_argument('--format', choices=['json', 'csv', 'sqlite'], default=DEFAULT_STORAGE_FORMAT, help='Output format')
    parser.add_argument('--countries', default='all', help='Comma-separated country codes (e.g., us,uk) or \'all\'')
    parser.add_argument("--days", type=int, default=HISTORICAL_DATA_DAYS, help="Filter items currently in the feed published within the last DAYS days (Note: RSS feeds typically do not provide deep historical data)")
    parser.add_argument('--detect-languages', action='store_true', help='Enable language detection (requires langdetect)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers for scraping')
    parser.add_argument('--output', default=None, help='Output filename stem (e.g., news_data)')
    parser.add_argument('--validate', action='store_true', help='Validate configured feeds and exit')
    parser.add_argument('--stats', action='store_true', help='Show feed configuration stats and exit')
    parser.add_argument('--api', action='store_true', help='Run API server after scraping (requires Flask & sqlite format)')
    return parser.parse_args()

def main():
    args = parse_arguments()
    setup_logger(LOG_FILE, LOG_LEVEL)
    
    logger.info("--- RSS News Scraper Started ---")
    logger.info(f"Arguments: {vars(args)}")

    # Handle --validate
    if args.validate:
        logger.info("Validating feeds...")
        valid_feeds, invalid_feeds = [], []
        all_config_feeds = []
        for country_data in RSS_FEEDS.values():
             all_config_feeds.extend(country_data['feeds'])
             
        for i, feed_conf in enumerate(all_config_feeds, 1):
            url = feed_conf['url']
            name = feed_conf['name']
            logger.info(f"Validating feed {i}/{len(all_config_feeds)}: {name} ({url})")
            is_valid, error = validate_feed(url)
            if is_valid:
                valid_feeds.append(feed_conf)
            else:
                feed_conf['error'] = error
                invalid_feeds.append(feed_conf)
                logger.warning(f"Invalid feed: {name} - {error}")
                
        print(f"\nFeed Validation Results:")
        print(f"Valid feeds: {len(valid_feeds)}")
        print(f"Invalid feeds: {len(invalid_feeds)}")
        if invalid_feeds:
            print("\nInvalid Feed Details:")
            for feed in invalid_feeds:
                print(f"  - {feed['name']} ({feed['url']}): {feed['error']}")
        logger.info("--- Validation Complete ---")
        return

    # Handle --stats
    if args.stats:
        logger.info("Gathering feed statistics...")
        stats = get_feed_stats()
        print("\nFeed Configuration Statistics:")
        print(f"Total countries configured: {stats['total_countries']}")
        print(f"Total feeds configured: {stats['total_feeds']}")
        print("\nFeeds per country:")
        for country, count in stats['feeds_per_country'].items():
            print(f"  - {country}: {count}")
        print("\nLanguages specified:")
        for lang, count in stats['languages'].items():
            print(f"  - {lang}: {count}")
        logger.info("--- Statistics Displayed ---")
        return

    # Prepare list of feeds to scrape based on args.countries
    feeds_to_scrape = []
    target_countries = []
    if args.countries.lower() == 'all':
        target_countries = list(RSS_FEEDS.keys())
    else:
        target_countries = [c.strip().lower() for c in args.countries.split(',')]
        
    for code in target_countries:
        if code in RSS_FEEDS:
            country_data = RSS_FEEDS[code]
            for feed in country_data['feeds']:
                feed_copy = feed.copy()
                feed_copy['country_code'] = code
                feed_copy['country_name'] = country_data['country_name']
                feeds_to_scrape.append(feed_copy)
        else:
            logger.warning(f"Country code '{code}' not found in configuration, skipping.")

    if not feeds_to_scrape:
        logger.error("No valid feeds selected for scraping. Exiting.")
        return
        
    logger.info(f"Selected {len(feeds_to_scrape)} feeds from {len(set(target_countries))} countries for scraping.")

    # --- Scraping --- 
    start_time = time.time()
    scraped_items = scrape_feeds_concurrently(feeds_to_scrape, args.days, args.workers)
    logger.info(f"Scraping finished in {time.time() - start_time:.2f} seconds. Found {len(scraped_items)} raw items.")

    # --- Processing --- 
    processed_items = process_scraped_items(scraped_items, args.detect_languages)
    logger.info(f"Processing complete. {len(processed_items)} unique items remaining.")

    # --- Saving --- 
    if not processed_items:
        logger.warning("No items were processed successfully. Nothing to save.")
    else:
        output_filename_stem = args.output if args.output else f"news_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_path = ""
        try:
            if args.format == 'json':
                output_path = save_to_json(processed_items, output_filename_stem)
            elif args.format == 'csv':
                output_path = save_to_csv(processed_items, output_filename_stem)
            elif args.format == 'sqlite':
                output_path = save_to_sqlite(processed_items, DATABASE_PATH)
            logger.info(f"Data successfully saved to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to save data in {args.format} format: {e}")

    # --- Summary --- 
    elapsed_time = time.time() - start_time
    print("\n--- Scraping Summary ---")
    print(f"Countries targeted: {len(set(target_countries))}")
    print(f"Feeds scraped: {len(feeds_to_scrape)}")
    print(f"Raw items found: {len(scraped_items)}")
    print(f"Processed (unique) items: {len(processed_items)}")
    if output_path:
        print(f"Output format: {args.format}")
        print(f"Output file: {output_path}")
    print(f"Total time: {elapsed_time:.2f} seconds")

    # --- API Server (Optional) --- 
    if args.api:
        if args.format != 'sqlite':
            logger.warning("API server requires data in SQLite format. Saving to default DB path...")
            try:
                db_path = save_to_sqlite(processed_items, DATABASE_PATH)
            except Exception as e:
                 logger.error(f"Failed to save to SQLite for API: {e}")
                 db_path = None
        else:
            db_path = output_path # Use the path where sqlite was saved
            
        if db_path and os.path.exists(db_path):
            logger.info("Starting API server...")
            app = setup_api_server(db_path)
            if app:
                print(f"\n--- API Server Running --- ")
                print(f"Access at: http://{API_HOST}:{API_PORT}/api/...")
                print(f"Endpoints: /api/news, /api/countries, /api/sources, /api/stats")
                print(f"Press Ctrl+C to stop.")
                try:
                    app.run(host=API_HOST, port=API_PORT, debug=API_DEBUG)
                except Exception as e:
                    logger.error(f"API server failed: {e}")
            else:
                logger.error("Failed to initialize API server.")
        else:
             logger.error("Cannot start API server: SQLite database file not found or failed to create.")

    logger.info("--- RSS News Scraper Finished ---")

if __name__ == "__main__":
    # Check minimum country requirement for feeds
    if len(RSS_FEEDS) < 20:
        print(f"Configuration Error: Need at least 20 countries defined in RSS_FEEDS, found {len(RSS_FEEDS)}.", file=sys.stderr)
        # sys.exit(1) # Or just log warning and continue?
        logging.warning(f"Configuration Warning: Less than 20 countries defined ({len(RSS_FEEDS)}).")
        
    main()

