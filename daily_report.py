# -----------------------------------------------------------------------------
# Kalshi Event DDGS Search RAG Pipeline (Synchronous Version)
# -----------------------------------------------------------------------------

import requests
from ddgs import DDGS
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import datetime as dt
from typing import List, Dict, Any
import json
import time
import random
from collections import Counter
from openai import OpenAI

# Load env vars
load_dotenv()

# Sync OpenAI client
client = OpenAI(
    organization=os.getenv("OPENAI_ORG_ID"),
    api_key=os.getenv("OPENAI_API_KEY")
)

# Mongo
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]

# -----------------------------------------------------------------------------

def utc_stamp():
    return dt.datetime.utcnow().strftime("%Y%m%d")

def write_to_db(report, contents, timestamp, event_ticker):
    collection = db["reports"]
    data = {
        "timestamp": timestamp,
        "event_ticker": event_ticker,
        "ddgs_report": report,
    }
    result = collection.insert_one(data)
    print(f"Inserted document with _id: {result.inserted_id}")

def read_from_db(timestamp, event_ticker):
    collection = db["reports"]
    query = {"timestamp": timestamp, "event_ticker": event_ticker}
    reports = list(collection.find(query))
    return None if not reports else reports[0]["ddgs_report"]

def get_market_descriptions(event, markets):
    market_descriptions = "" 
    if len(markets) == 1:
        m = markets[0]
        market_descriptions = f"""Event title: {event['title']}
Title: {m['title']}
Subtitle: {m['yes_sub_title']}
Possible Outcomes: Yes (0) or No (1)
Rules: {m['rules_primary']}"""
        if isinstance(m['rules_secondary'], str) and len(m['rules_secondary']) > 0:
            market_descriptions += f"\nSecondary rules: {m['rules_secondary']}"
        market_descriptions += f"\nScheduled close date: {m['expiration_time']}\n"
    else:
        for idx, m in enumerate(markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m['ticker']}
Title: {m['title']}
Subtitle: {m.get('yes_sub_title', '')}
Possible Outcomes: Yes (0) or No (1)
Rules: {m.get('rules_primary', '')}"""
            if isinstance(m.get('rules_secondary', None), str) and len(m.get('rules_secondary', '')) > 0:
                market_descriptions += f"\nSecondary rules: {m['rules_secondary']}"
            market_descriptions += f"\nScheduled close date: {m['expiration_time']}\n\n"
    return market_descriptions

def run_openai(prompt, model="gpt-4o-mini-2024-07-18"):
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content

def step1_generate_queries(event, market_descriptions):
    num_queries = 6
    max_words_in_query = 7
    query_prompt = f"""The following are markets under the event titled "{event['title']}". 

{market_descriptions}

# Instructions
What are {num_queries} short search queries..."""
    output_text = run_openai(query_prompt)
    queries = [q.strip() for q in output_text.strip().split("\n") if q.strip()]
    print(queries)
    return queries

def step2_search_ddgs(search_query, num_urls=5):
    results = list(DDGS().text(search_query, max_results=num_urls * 2, timelimit="y") or [])
    search_results, seen_urls = [], set()
    for r in results:
        href = r.get("href")
        if href and href not in seen_urls:
            seen_urls.add(href)
            search_results.append(r)
    return search_results

def step3_scrape_urls(search_results, num_urls=5):
    contents = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    for result in search_results[:num_urls]:
        url = result.get('href')
        if not url: 
            continue
        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(['script', 'style']):
                tag.decompose()
            paragraphs = soup.find_all('p')
            article_text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
            if 200 <= len(article_text) <= 100000:
                contents.append({
                    "title": result.get('title', ''),
                    "body": result.get('body', ''),
                    "href": url,
                    "article": article_text
                })
        except Exception as e:
            continue
    return contents

def step4_summarization(contents, event, market_descriptions):
    articles_concatenated = ""
    for i, c in enumerate(contents):
        articles_concatenated += f"""# Article {i+1}
Title: {c['title']}
Body: {c['body']}
Source URL: {c['href']}
Full Content: {c['article']}\n\n"""
    prompt = f"""The following are markets under the event titled "{event['title']}".
{market_descriptions}

{articles_concatenated}

# Instructions
Carefully read the articles..."""
    return run_openai(prompt)

def process_query(search_query, event, market_descriptions, num_urls=5):
    results  = step2_search_ddgs(search_query, num_urls)
    contents = step3_scrape_urls(results, num_urls)
    summary  = step4_summarization(contents, event, market_descriptions)
    return summary, contents

def get_ddgs_report(index, event):
    market_descriptions = get_market_descriptions(event, event['markets'])
    queries = step1_generate_queries(event, market_descriptions)
    summaries, all_contents = [], []
    for q in queries:
        summary, contents = process_query(q, event, market_descriptions)
        summaries.append(summary)
        all_contents.append(contents)
    reports = "\n\n".join(f"# Research Report {i+1}:\n{summary}" for i, summary in enumerate(summaries)).strip()
    return reports, all_contents

def fetch_current_events():
    url = "https://raw.githubusercontent.com/jyoonsong/FutureBench/refs/heads/main/active_events.json"
    events = None
    while events == None:
        try:
            r = requests.get(url)
            events = r.json()
            return events
        except Exception as e:
            print("Retrying fetch_current_events:", e)
            events = None

def main():
    print("Starting daily report generation...")
    timestamp = utc_stamp()
    print(f"Timestamp: {timestamp}")
    events = fetch_current_events()
    print(f"Fetched {len(events)} events")
    for index, e in enumerate(events[:5]):  # limit for demo
        ticker = e["event_ticker"]
        if read_from_db(timestamp, ticker):
            print(f"Already exists: {ticker}, skipping.")
            continue
        # fetch full event
        resp = requests.get(
            f"https://api.elections.kalshi.com/trade-api/v2/events/{ticker}?with_nested_markets=true",
            timeout=15
        )
        event = resp.json()["event"]
        if not event.get("markets"):
            continue
        report, contents = get_ddgs_report(index, event)
        write_to_db(report, contents, timestamp, ticker)

if __name__ == "__main__":
    main()
