import requests
from ddgs import DDGS
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import datetime as dt
from typing import List, Dict, Any

import aiohttp
import asyncio
# from openai import OpenAI
from openai import AsyncOpenAI

load_dotenv()

client = AsyncOpenAI(
    organization=os.getenv("OPENAI_ORG_ID"),
    api_key=os.getenv("OPENAI_API_KEY")
)

MONGO_URI = os.getenv("MONGO_URI")

# Set up mongodb client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]  # Change name if needed

# --- concurrency controls (tune these) ---
SEM_OPENAI = asyncio.Semaphore(3)   # concurrent OpenAI calls (steps 1 & 4)
SEM_HTTP   = asyncio.Semaphore(10)  # concurrent HTTP fetches (step 3)
SEM_TICKERS = asyncio.Semaphore(4)
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)


def utc_stamp():
    # return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return dt.datetime.utcnow().strftime("%Y%m%d")


def write_to_db(report, timestamp, event_ticker):
    collection = db["reports"]

    data = {}
    data["timestamp"] = timestamp
    data["event_ticker"] = event_ticker
    data["ddgs_report"] = report
    
    result = collection.insert_one(data)
    print(f"Inserted document with _id: {result.inserted_id}")


def read_from_db(timestamp, event_ticker):
    collection = db["reports"]

    # Get market-level predictions for this ensemble run
    query = {
        "timestamp": timestamp,
        "event_ticker": event_ticker,
    }
    cursor = collection.find(query)
    reports = list(cursor)

    if len(reports) == 0:
        return None
    else:
        return reports[0]["ddgs_report"]


def get_market_descriptions(event, markets):
    # Generate market descriptions
    market_descriptions = "" 
    
    if len(markets) == 1:
        m = markets[0]
        market_descriptions = f"""Event title: {event['title']}
Title: {m['title']}
Subtitle: {m['yes_sub_title']}
Possible Outcomes: Yes (0) or No (1)
Rules: {m['rules_primary']}"""

        if type(m['rules_secondary']) == str and len(m['rules_secondary']) > 0:
            market_descriptions += f"\nSecondary rules: {m['rules_secondary']}"
        market_descriptions += f"\nScheduled close date: {m['expiration_time']}"
        market_descriptions += f"\n(Note: The market may resolve before this date.)\n"

    elif len(markets) > 1:
        for idx, m in enumerate(markets):
            market_descriptions += f"""# Market {idx + 1}
Ticker: {m['ticker']}
Title: {m['title']}
Subtitle: {getattr(m, 'yes_sub_title', '')}
Possible Outcomes: Yes (0) or No (1)
Rules: {getattr(m, 'rules_primary', '')}"""

            if isinstance(getattr(m, 'rules_secondary', None), str) and len(getattr(m, 'rules_secondary', '')) > 0:
                market_descriptions += f"\nSecondary rules: {m['rules_secondary']}"
            market_descriptions += f"\nScheduled close date: {m['expiration_time']}\n\n"
    
    return market_descriptions


async def run_openai(prompt, model="gpt-4o-mini-2024-07-18"):
    async with SEM_OPENAI:
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
    response_text = response.choices[0].message.content
    return response_text


async def step1_generate_queries(event, market_descriptions):
    num_queries = 6
    max_words_in_query = 7

    query_prompt = f"""The following are markets under the event titled "{event['title']}". The markets can resolve before the scheduled close date.
{market_descriptions}

# Instructions
What are {num_queries} short search queries that would meaningfully improve the accuracy and confidence of a forecast regarding the market outcomes described above? Output exactly {num_queries} queries, one query per line, without any other text or number. Each query should be less than {max_words_in_query} words."""
    output_text = await run_openai(prompt=query_prompt, model="gpt-4o-mini-2024-07-18")
    queries = output_text.strip().split("\n")
    queries = [q.strip() for q in queries if len(q.strip()) > 0]

    return queries

async def step2_search_ddgs(search_query, num_urls=5):
    def blocking_ddgs():
        return list(DDGS().text(search_query, max_results=num_urls * 2, timelimit="y") or [])
    results = await asyncio.to_thread(blocking_ddgs)

    # deduplicate by URL
    search_results = []
    seen_urls = set()
    for result in results:
        href = result.get("href")
        if href and href not in seen_urls:
            seen_urls.add(href)
            search_results.append(result)
    return search_results


async def step3_scrape_urls(search_results, num_urls=5):
    contents = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    async def fetch_parse(session, result):
        url = result.get('href')
        if not url or not url.startswith(("http://", "https://")):
            return None
        async with SEM_HTTP:
            try:
                async with session.get(url, timeout=HTTP_TIMEOUT, headers=headers) as resp:
                    # Some sites reject without a successful status; continue on non-200s.
                    if resp.status != 200:
                        return None
                    text = await resp.text()
            except Exception:
                return None

        soup = BeautifulSoup(text, "html.parser")
        for tag in soup(['script', 'style']):
            tag.decompose()
        paragraphs = soup.find_all('p')
        article_text = "\n".join(p.get_text(separator="\n", strip=True) for p in paragraphs)

        if 200 <= len(article_text) <= 100000:
            return {
                "title": result.get('title', ''),
                "body": result.get('body', ''),
                "href": url,
                "article": article_text
            }
        return None

    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_parse(session, r)) for r in search_results]
        try:
            for coro in asyncio.as_completed(tasks):
                item = await coro
                if item:
                    contents.append(item)
                    if len(contents) >= num_urls:
                        break
        finally:
            # Cancel remaining tasks and wait for them to finish to avoid
            # "Task exception was never retrieved" and "Connector is closed."
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return contents


async def step4_summarization(contents, event, market_descriptions):
    # Concatenate articles 
    articles_concatenated = ""
    for index, content in enumerate(contents):
        articles_concatenated += f"""# Article {index + 1}
Title: {content['title']}
Body: {content['body']}
Source URL: {content['href']}
Full Content: {content['article']}\n\n"""
    articles_concatenated = articles_concatenated.strip()

    # Run OpenAI API to summarize the articles
    prompt = f"""The following are markets under the event titled "{event['title']}". The markets can resolve before the scheduled close date.
{market_descriptions}

{articles_concatenated}

# Instructions
Carefully read the articles provided above. Your task is to generate a multi-paragraph summary (one paragraph per article) that highlights factual insights or relevant context related to the listed markets. Avoid subjective opinions or speculative statements. Omit an article that does not contain meaningful information. Use plain text without markdown syntax, heading, or numbering. Do not add any additional text outside the summary.
Important note: Include the date and source URL of the article at the end of each paragraph."""
    output_text = await run_openai(prompt=prompt, model="gpt-4o-mini-2024-07-18")
    return output_text


# --- per-query chain (2->3->4) ---
async def process_query(search_query, event, market_descriptions, num_urls=5):
    results  = await step2_search_ddgs(search_query, num_urls)
    contents = await step3_scrape_urls(results, num_urls)
    return await step4_summarization(contents, event, market_descriptions)


# --- per-ticker pipeline with fan-out across queries ---
async def get_ddgs_report(index, event):
    print(f"Generating report for event {index}: {event['event_ticker']}")
    market_descriptions = get_market_descriptions(event, event['markets'])
    queries = await step1_generate_queries(event, market_descriptions)

    # run step 2->3->4 for many queries in parallel (bounded by semaphores inside)
    per_query_tasks = [asyncio.create_task(process_query(q, event, market_descriptions, num_urls=5)) for q in queries]
    summaries = await asyncio.gather(*per_query_tasks, return_exceptions=False)

    # combine (your step 5)
    reports = "\n\n".join(f"# Research Report {i+1}:\n{summary}" for i, summary in enumerate(summaries)).strip()

    print(f"Completed report for event {index}: {event['event_ticker']}")
    return reports


def fetch_current_events():
    json_url = "https://raw.githubusercontent.com/jyoonsong/FutureBench/refs/heads/main/active_events.json"

    event_tickers = None
    while event_tickers == None:
        try:
            response = requests.get(json_url)
            events = response.json()
            event_tickers = [event['event_ticker'] for event in events]
        except Exception as e:
            print(f"Error fetching current events: {e}")
            continue
    
    return event_tickers
    

async def main():
    timestamp = utc_stamp()
    print(f"Running Kalshi scraper at {timestamp}")

    event_tickers = fetch_current_events()
    print(f"Fetched {len(event_tickers)} current events from GitHub")

    async def guarded_process(index, event):
        async with SEM_TICKERS:
            ddgs_report = await get_ddgs_report(index, event)
            write_to_db(ddgs_report, timestamp, event["event_ticker"])

    tasks = []
    for index, event_ticker in enumerate(event_tickers):
        existing_report = read_from_db(timestamp, event_ticker)
        if existing_report is not None:
            print(f"Report already exists for {event_ticker} at {timestamp}, skipping...")
            continue

        # fetch event details without blocking the whole loop
        def fetch_event():
            while True:
                try:
                    resp = requests.get(
                        f"https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}?with_nested_markets=true",
                        timeout=15
                    )
                    resp.raise_for_status()
                    return resp.json()["event"]
                except Exception as e:
                    print(f"Retrying event fetch for {event_ticker}: {e}")

        event = await asyncio.to_thread(fetch_event)  # offload blocking requests
        tasks.append(asyncio.create_task(guarded_process(index, event)))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())