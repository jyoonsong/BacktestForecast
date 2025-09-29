# -----------------------------------------------------------------------------
# Kalshi Event DDGS Search RAG Pipeline
# -----------------------------------------------------------------------------
# High-level pipeline (per event_ticker):
#   1) Use OpenAI (gpt-4o-mini) to generate N short web search queries.
#   2) For each query, search the web via DDGS (DuckDuckGo Search wrapper).
#   3) Scrape top results (async HTTP with aiohttp), parse & clean HTML to text.
#   4) Summarize scraped articles with OpenAI into plain-text paragraphs.
#   5) Combine summaries into a single "research report" string.
#   6) Persist the report to MongoDB keyed by (timestamp, event_ticker).
#
# Concurrency model:
#   - SEM_TICKERS limits how many event tickers are processed at once.
#   - SEM_OPENAI bounds concurrent OpenAI calls inside steps 1 & 4.
#   - SEM_HTTP bounds async HTTP fetches during scraping.
#   - Blocking calls (requests, DDGS) are offloaded via asyncio.to_thread.
#
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

import aiohttp
import asyncio
from openai import AsyncOpenAI

# Load env vars from .env if present (OPENAI keys, Mongo URI, etc.)
load_dotenv()

# Async OpenAI client (only used for chat.completions in this script)
client = AsyncOpenAI(
    organization=os.getenv("OPENAI_ORG_ID"),
    api_key=os.getenv("OPENAI_API_KEY")
)

# Mongo connection URI from env (e.g., mongodb+srv://...)
MONGO_URI = os.getenv("MONGO_URI")

# Set up mongodb client and DB handle
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]  # Change name if needed

# --- concurrency controls (tune these) ---
# Limit how many OpenAI requests run concurrently across tasks.
SEM_OPENAI = asyncio.Semaphore(3)   # concurrent OpenAI calls (steps 1 & 4)
# Limit how many HTTP requests (scraping) run concurrently.
SEM_HTTP   = asyncio.Semaphore(10)  # concurrent HTTP fetches (step 3)
# Limit how many event tickers run end-to-end concurrently.
SEM_TICKERS = asyncio.Semaphore(4)
# Global per-request timeout for aiohttp GETs (total time).
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)


def utc_stamp():
    # Timestamp used as a run key for idempotency (one report/day).
    # Example: "20250830" for Aug 30, 2025.
    # return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return dt.datetime.utcnow().strftime("%Y%m%d")


def write_to_db(report, contents, timestamp, event_ticker):
    """
    Persist a single research report for (timestamp, event_ticker).
    Overwrites are not handled; caller ensures uniqueness before insert.
    """
    collection = db["reports"]

    # filtered_urls = [
    #     {
    #         "title": content.get("title", ""),
    #         "body": content.get("body", ""),
    #         "href": content.get("href", ""),
    #     }
    #     for sublist in contents
    #     for content in sublist
    # ]

    data = {}
    data["timestamp"] = timestamp
    data["event_ticker"] = event_ticker
    data["ddgs_report"] = report
    # data["ddgs_urls"] = json.dumps(filtered_urls)
    
    result = collection.insert_one(data)
    print(f"Inserted document with _id: {result.inserted_id}")


def read_from_db(timestamp, event_ticker):
    """
    Read back an existing report for (timestamp, event_ticker).
    Returns the stored ddgs_report string, or None if not found.
    """
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
    """
    Build a human-readable block describing the event and its markets.
    This text conditions downstream LLM prompts (query generation, summarization).
    """
    # Generate market descriptions
    market_descriptions = "" 
    
    if len(markets) == 1:
        # Single yes/no market under the event
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
        # Multiple markets (iterates and appends each)
        for idx, m in enumerate(markets):
            # NOTE: m is likely a dict; getattr(...) will return default.
            # Consider m.get('yes_sub_title', '') / m.get('rules_primary', '') later.
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
    """
    Thin wrapper to call OpenAI with concurrency guard.
    Returns the text content of the first choice.
    """
    async with SEM_OPENAI:
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
    response_text = response.choices[0].message.content
    return response_text


async def step1_generate_queries(event, market_descriptions):
    """
    Step 1: Ask LLM for short, informative search queries about the markets.
    Output is a list of strings, one query per line.
    """
    num_queries = 6
    max_words_in_query = 7

    query_prompt = f"""The following are markets under the event titled "{event['title']}". The markets can resolve before the scheduled close date.
{market_descriptions}

# Instructions
What are {num_queries} short search queries that would meaningfully improve the accuracy and confidence of a forecast regarding the market outcomes described above? Output exactly {num_queries} queries, one query per line, without any other text or number. Each query should be less than {max_words_in_query} words."""
    # output_text = await run_openai(prompt=query_prompt, model="gpt-4.1-nano")
    output_text = await run_openai(prompt=query_prompt, model="gpt-4o-mini-2024-07-18")
    queries = output_text.strip().split("\n")
    queries = [q.strip() for q in queries if len(q.strip()) > 0]

    return queries

async def step2_search_ddgs(search_query, num_urls=5):
    """
    Step 2: Search DDGS (DuckDuckGo) for a query and return deduplicated results.
    Runs blocking DDGS().text(...) inside a thread to avoid blocking the event loop.
    """
    def blocking_ddgs():
        # timelimit="y" ~ last year; fetch 2x to allow for dedup & filtering
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
    """
    Step 3: Asynchronously fetch & parse the top search results.
    - Uses aiohttp for concurrency, bounded by SEM_HTTP.
    - Strips <script>/<style>, concatenates <p> text as article content.
    - Returns up to num_urls cleaned article dicts.
    """
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
                # Swallow per-URL exceptions to keep the batch moving.
                return None

        # Parse HTML and reduce to raw readable text.
        soup = BeautifulSoup(text, "html.parser")
        for tag in soup(['script', 'style']):
            tag.decompose()
        paragraphs = soup.find_all('p')
        article_text = "\n".join(p.get_text(separator="\n", strip=True) for p in paragraphs)

        # Basic length guard to avoid empty or massive blobs.
        if 200 <= len(article_text) <= 100000:
            return {
                "title": result.get('title', ''),
                "body": result.get('body', ''),
                "href": url,
                "article": article_text
            }
        return None

    async with aiohttp.ClientSession() as session:
        # Fire off all fetches, then consume completions as they finish.
        tasks = [asyncio.create_task(fetch_parse(session, r)) for r in search_results]
        try:
            for coro in asyncio.as_completed(tasks):
                item = await coro
                if item:
                    contents.append(item)
                    if len(contents) >= num_urls:
                        break
        finally:
            # Cancel remaining tasks and wait for them to finish to avoid warnings like:
            # "Task exception was never retrieved" and "Connector is closed."
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return contents


async def step4_summarization(contents, event, market_descriptions):
    """
    Step 4: Summarize cleaned articles into plain-text paragraphs.
    - One paragraph per article.
    - Include date and source URL at end of each paragraph (per prompt).
    - Avoids headings/markdown in the response.
    """
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
Carefully read the articles provided above. Your task is to generate a multi-paragraph summary (one paragraph per article) that highlights factual insights or relevant context related to the listed markets. Avoid subjective opinions or speculative statements. Use plain text without markdown syntax, heading, or numbering. Do not add any additional text outside the summary.
Return blank for an article that does not contain relevant information. Not all of the articles are relevant to the markets above. Some are clearly unrelated to the topic and should be excluded. Exclude only the articles that are clearly off-topic, entirely unrelated to the markets. If an article is at least broadly related or offers potentially useful context, it should be considered relevant.
Important note: Include the date and source URL of the article at the end of each paragraph."""
    # output_text = await run_openai(prompt=prompt, model="gpt-4.1-nano")
    output_text = await run_openai(prompt=prompt, model="gpt-4o-mini-2024-07-18")
    return output_text


# --- per-query chain (2->3->4) ---
async def process_query(search_query, event, market_descriptions, num_urls=5):
    """
    Orchestrates steps 2->3->4 for a single search query:
      search -> scrape -> summarize
    """
    results  = await step2_search_ddgs(search_query, num_urls)
    contents = await step3_scrape_urls(results, num_urls)
    summary = await step4_summarization(contents, event, market_descriptions)
    return summary, contents


# --- per-ticker pipeline with fan-out across queries ---
async def get_ddgs_report(index, event):
    """
    For a single event:
      - Generate queries (step 1).
      - Fan out processing (steps 2-4) across queries concurrently.
      - Join summaries into a single research report string.
    """
    MAX_RETRIES = 5  # max URLs to fetch per query
    trials = 0
    while trials < MAX_RETRIES:
        try:
            print(f"Generating report for event {index}: {event['event_ticker']}")
            market_descriptions = get_market_descriptions(event, event['markets'])
            queries = await step1_generate_queries(event, market_descriptions)

            # run step 2->3->4 for many queries in parallel (bounded by semaphores inside)
            per_query_tasks = [asyncio.create_task(process_query(q, event, market_descriptions, num_urls=5)) for q in queries]
            results = await asyncio.gather(*per_query_tasks, return_exceptions=False)

            # Separate summaries and contents
            summaries, all_contents = zip(*results)  # Each result is a (summary, contents) tuple

            # combine summaries (your step 5)
            reports = "\n\n".join(f"# Research Report {i+1}:\n{summary}" for i, summary in enumerate(summaries)).strip()

            print(f"Completed report generation for event {index}: {event['event_ticker']}")
            return reports, all_contents
        except Exception as e:
            trials += 1
            print(f"Error processing event, retrying... ({trials}/{MAX_RETRIES}): {e}")
            time.sleep(3)
            continue



def fetch_current_events():
    """
    Fetch the current list of active event tickers from GitHub (raw JSON).
    Retries on error until a valid list is returned.
    """
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
    """
    End-to-end runner:
      - Creates daily timestamp key.
      - Fetches active event tickers.
      - Skips tickers already processed today.
      - Fetches event JSON (with_nested_markets) via Kalshi API.
      - Generates & stores reports concurrently, bounded by SEM_TICKERS.
    """
    timestamp = utc_stamp()
    print(f"Running Kalshi scraper at {timestamp}")

    event_tickers = fetch_current_events()
    print(f"Fetched {len(event_tickers)} current events from GitHub")

    if len(event_tickers) > 2000:
        # sample 2000 of the event_tickers
        random.seed(37) # for reproducibility
        event_tickers = random.sample(event_tickers, 2000)
    print(f"Processing {len(event_tickers)} events after sampling")

    async def guarded_process(index, event):
        # Per-event concurrency guard so we don't overload downstream services.
        async with SEM_TICKERS:
            ddgs_report, all_contents = await get_ddgs_report(index, event)
            write_to_db(ddgs_report, all_contents, timestamp, event["event_ticker"])

    tasks = []
    for index, event_ticker in enumerate(event_tickers):
        # Idempotency: if today's report exists for this ticker, skip the work.
        existing_report = read_from_db(timestamp, event_ticker)
        if existing_report is not None:
            print(f"Report already exists for {event_ticker} at {index}, skipping...")
            continue

        # fetch event details without blocking the whole loop
        def fetch_event():
            # Retry until success; consider backoff or max-retries in the future.
            trials = 0
            MAX_TRIALS = 10
            while trials < MAX_TRIALS:
                try:
                    resp = requests.get(
                        f"https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}?with_nested_markets=true",
                        timeout=15
                    )
                    resp.raise_for_status()
                    return resp.json()["event"]
                except Exception as e:
                    print(f"Retrying event fetch for {event_ticker}: {e}")
                    trials += 1

        # Offload the blocking requests.get to a worker thread.
        event = await asyncio.to_thread(fetch_event)  # offload blocking requests
        if event is None or "markets" not in event or event["markets"] is None or len(event["markets"]) == 0:
            print(f"No markets found for event {event_ticker} at {index}, skipping...")
            continue
        if len(event["markets"]) > 6:
            print(f"Too many markets ({len(event['markets'])}) for event {event_ticker} at {index}, skipping...")
            continue
        tasks.append(asyncio.create_task(guarded_process(index, event)))

    # Wait for all event tasks to complete.
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Entry point for async program.
    asyncio.run(main())
