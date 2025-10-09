# -----------------------------------------------------------------------------
# Kalshi Event DDGS Search RAG Pipeline
# -----------------------------------------------------------------------------

import os
import json
import time
import random
import datetime as dt
from typing import List, Dict, Any, Tuple
from collections import Counter

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from pymongo import MongoClient
from openai import OpenAI

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

MODEL_NAME = "gpt-4o-mini-2024-07-18"
TARGET_EVENTS = 200
NUM_QUERIES = 6
NUM_URLS = 5
MAX_QUERY_WORDS = 7

MONGO_URI = os.getenv("MONGO_URI")
OPENAI_ORG_ID = os.getenv("OPENAI_ORG_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(organization=OPENAI_ORG_ID, api_key=OPENAI_API_KEY)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

def utc_stamp() -> str:
    """Return current UTC date as YYYYMMDD string."""
    return dt.datetime.utcnow().strftime("%Y%m%d")

def log(msg: str):
    """Simple timestamped logger."""
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}")

# -----------------------------------------------------------------------------
# MongoDB I/O
# -----------------------------------------------------------------------------

def write_to_db(report: str, contents: List[Dict[str, Any]], timestamp: str, event_ticker: str):
    """Insert the report and contents into MongoDB."""
    collection = db["reports"]
    data = {"timestamp": timestamp, "event_ticker": event_ticker, "ddgs_report": report}
    result = collection.insert_one(data)
    log(f"Inserted document with _id: {result.inserted_id}")

def read_from_db(timestamp: str, event_ticker: str) -> str | None:
    """Retrieve stored report from MongoDB."""
    collection = db["reports"]
    query = {"timestamp": timestamp, "event_ticker": event_ticker}
    record = collection.find_one(query)
    return record["ddgs_report"] if record else None

# -----------------------------------------------------------------------------
# OpenAI Helpers
# -----------------------------------------------------------------------------

def run_openai(prompt: str, model: str = MODEL_NAME) -> str:
    """Run an OpenAI chat completion."""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log(f"OpenAI API error: {e}")
        return ""

# -----------------------------------------------------------------------------
# DDGS Search & Web Scraping
# -----------------------------------------------------------------------------

def search_ddgs(query: str, num_urls: int = NUM_URLS) -> List[Dict[str, Any]]:
    """Perform DuckDuckGo search for a query."""
    results = list(DDGS().text(query, max_results=num_urls * 2, timelimit="y") or [])
    seen, deduped = set(), []
    for r in results:
        href = r.get("href")
        if href and href not in seen:
            seen.add(href)
            deduped.append(r)
    return deduped[:num_urls]

def scrape_urls(search_results: List[Dict[str, Any]], num_urls: int = NUM_URLS) -> List[Dict[str, str]]:
    """Scrape HTML pages and extract paragraph text."""
    contents = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for result in search_results[:num_urls]:
        url = result.get("href")
        if not url:
            continue
        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            paragraphs = soup.find_all("p")
            text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
            if 200 <= len(text) <= 100000:
                contents.append({
                    "title": result.get("title", ""),
                    "body": result.get("body", ""),
                    "href": url,
                    "article": text,
                })
        except Exception as e:
            log(f"Scrape failed for {url}: {e}")
    return contents

# -----------------------------------------------------------------------------
# Query & Summarization Steps
# -----------------------------------------------------------------------------

def get_market_descriptions(event: Dict[str, Any]) -> str:
    """Generate readable descriptions for markets."""
    markets = event["markets"]
    if len(markets) == 1:
        m = markets[0]
        desc = (
            f"Event title: {event['title']}\n"
            f"Title: {m['title']}\n"
            f"Subtitle: {m['yes_sub_title']}\n"
            "Possible Outcomes: Yes (0) or No (1)\n"
            f"Rules: {m['rules_primary']}\n"
        )
        if m.get("rules_secondary"):
            desc += f"Secondary rules: {m['rules_secondary']}\n"
        desc += f"Scheduled close date: {m['expiration_time']}\n"
        return desc
    desc = ""
    for idx, m in enumerate(markets, start=1):
        desc += (
            f"# Market {idx}\n"
            f"Ticker: {m['ticker']}\n"
            f"Title: {m['title']}\n"
            f"Subtitle: {m.get('yes_sub_title', '')}\n"
            "Possible Outcomes: Yes (0) or No (1)\n"
            f"Rules: {m.get('rules_primary', '')}\n"
        )
        if m.get("rules_secondary"):
            desc += f"Secondary rules: {m['rules_secondary']}\n"
        desc += f"Scheduled close date: {m['expiration_time']}\n\n"
    return desc

def generate_search_queries(event: Dict[str, Any], market_descriptions: str) -> List[str]:
    """Generate search queries via OpenAI."""
    prompt = f"""
The following are markets under the event titled "{event['title']}". 

{market_descriptions}

# Instructions
What are {NUM_QUERIES} short search queries that would meaningfully improve the accuracy and confidence of a forecast regarding the market outcomes described above? 
Output exactly {NUM_QUERIES} queries, one query per line, without any other text or number. 
Each query should be less than {MAX_QUERY_WORDS} words.
Do not include numbers, symbols, or explanations.
"""
    output = run_openai(prompt)
    return [line.strip() for line in output.splitlines() if line.strip()]

def summarize_articles(contents: List[Dict[str, str]], event: Dict[str, Any], market_descriptions: str) -> str:
    """Summarize scraped articles via OpenAI."""
    all_articles = ""
    for i, c in enumerate(contents, 1):
        all_articles += (
            f"# Article {i}\n"
            f"Title: {c['title']}\n"
            f"Body: {c['body']}\n"
            f"Source URL: {c['href']}\n"
            f"Full Content: {c['article']}\n\n"
        )

    prompt = f"""
The following are markets under the event titled "{event['title']}".
{market_descriptions}

{all_articles}

# Instructions
Generate one paragraph per relevant article summarizing factual insights or context related to these markets. 
Avoid subjective statements. Include the article date and source URL at the end of each paragraph.
Exclude articles that are entirely unrelated.
"""
    return run_openai(prompt)

def process_query(query: str, event: Dict[str, Any], market_descriptions: str) -> Tuple[str, List[Dict[str, str]]]:
    """Run full pipeline for a single query."""
    results = search_ddgs(query)
    contents = scrape_urls(results)
    summary = summarize_articles(contents, event, market_descriptions)
    return summary, contents

def get_ddgs_report(event: Dict[str, Any]) -> Tuple[str, List[List[Dict[str, str]]]]:
    """Generate combined DDGS research report for a single event."""
    market_descriptions = get_market_descriptions(event)
    queries = generate_search_queries(event, market_descriptions)
    summaries, all_contents = [], []
    for q in queries:
        summary, contents = process_query(q, event, market_descriptions)
        summaries.append(summary)
        all_contents.append(contents)
    combined_report = "\n\n".join(f"# Research Report {i+1}\n{summary}" for i, summary in enumerate(summaries))
    return combined_report.strip(), all_contents

# -----------------------------------------------------------------------------
# Event Fetching & Sampling
# -----------------------------------------------------------------------------

def fetch_current_events() -> List[Dict[str, Any]]:
    """Fetch current active Kalshi events."""
    url = "https://raw.githubusercontent.com/jyoonsong/FutureBench/refs/heads/main/active_events.json"
    for _ in range(5):
        try:
            return requests.get(url, timeout=10).json()
        except Exception as e:
            log(f"Retrying fetch_current_events: {e}")
            time.sleep(2)
    raise RuntimeError("Failed to fetch events after 5 retries.")

def stratified_sample_events(events: List[Dict[str, Any]], target: int = TARGET_EVENTS) -> List[Dict[str, Any]]:
    """Sample events across categories."""
    if len(events) <= target:
        return events
    random.seed(37)
    categories = {}
    for e in events:
        categories.setdefault(e["category"], []).append(e)
    sampled, remaining = [], target
    cat_lists = sorted(categories.values(), key=len)
    for i, lst in enumerate(cat_lists):
        share = max(1, remaining // (len(cat_lists) - i))
        take = len(lst) if len(lst) <= share else random.sample(lst, share)
        sampled += take
        remaining -= min(len(lst), share)
    orig_counts = Counter(e["category"] for e in events)
    sampled_counts = Counter(e["category"] for e in sampled)
    for cat in orig_counts:
        log(f"{cat}: original={orig_counts[cat]}, sampled={sampled_counts.get(cat, 0)}")
    return sampled

# -----------------------------------------------------------------------------
# Main Orchestration
# -----------------------------------------------------------------------------

def main():
    log("Starting daily report generation...")
    timestamp = utc_stamp()
    events = fetch_current_events()
    log(f"Fetched {len(events)} events from GitHub.")

    sampled_events = stratified_sample_events(events)

    for idx, event_meta in enumerate(sampled_events):
        ticker = event_meta["event_ticker"]
        if read_from_db(timestamp, ticker):
            log(f"Already exists: {ticker}, skipping.")
            continue

        # Fetch full event with nested markets
        try:
            resp = requests.get(
                f"https://api.elections.kalshi.com/trade-api/v2/events/{ticker}?with_nested_markets=true",
                timeout=15
            )
            event = resp.json().get("event", {})
            if not event.get("markets"):
                continue
            report, contents = get_ddgs_report(event)
            write_to_db(report, contents, timestamp, ticker)
        except Exception as e:
            log(f"Failed processing {ticker}: {e}")

    log("Report generation completed.")

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
