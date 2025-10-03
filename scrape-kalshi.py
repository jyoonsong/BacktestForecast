#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Kalshi event & market snapshotter
# -----------------------------------------------------------------------------
# What this script does (daily-friendly):
# 1) Pull all (optionally filtered) Kalshi events, with nested markets.
# 2) Compare with previously saved JSONs to:
#       - carry forward still-active items,
#       - detect newly active events/markets,
#       - mark no-longer-active markets/events as resolved (add resolution_date),
#       - update market price snapshots keyed by date (YYYYMMDD).
# 3) Attempt to enrich active events with daily DDGS reports stored in MongoDB.
# 4) Write four JSON files: active_events, resolved_events, active_markets, resolved_markets.
# 5) Push the updated files to a GitHub repo (main branch by default).
# -----------------------------------------------------------------------------

import base64
import datetime as dt
import json
import logging
import os
import requests
from pymongo import MongoClient

# Configure logging: INFO for normal run; set to DEBUG for request params, etc.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kalshi REST endpoints
base_url_events = "https://api.elections.kalshi.com/trade-api/v2/events"

# Mongo connection URI from env (e.g., mongodb+srv://...)
MONGO_URI = os.getenv("MONGO_URI")

# Set up mongodb client and DB handle
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]  # Change name if needed

def read_from_db(timestamp, event_ticker):
    """
    Retrieve an existing DDGS research report string for (timestamp, event_ticker).
    Returns:
        str | None: The 'ddgs_report' string if present; otherwise None.
    """
    collection = db["reports"]

    # Query matches one day's run and the specific event ticker.
    query = {
        "timestamp": timestamp,
        "event_ticker": event_ticker,
    }
    cursor = collection.find(query)
    reports = list(cursor)

    if len(reports) == 0:
        return None
    else:
        ddgs_report = reports[0].get("ddgs_report", None)
        return ddgs_report

def get_timestamps():
    """
    Create a list of recent day stamps (UTC), newest first, format YYYYMMDD.
    Current implementation returns today, yesterday, and the day before.
    """
    timestamps = []
    for delta in range(3):
        day = dt.datetime.utcnow() - dt.timedelta(days=delta)
        timestamps.append(day.strftime("%Y%m%d"))
    return timestamps

def fetch_all_events(status=None, with_markets=True):
    """
    Fetch all events (optionally filtered) from Kalshi with pagination.
    Args:
        status (str | None): e.g., 'open' to fetch only open events.
        with_markets (bool): If True, include nested markets in results.
    Returns:
        list[dict]: All fetched events.
    """
    params = {}
    if status:
        params['status'] = status
    if with_markets:
        params['with_nested_markets'] = "true"

    events = []
    cursor = None

    logger.info(f"Fetching all events with status={status} and with_markets={with_markets}")

    while True:
        if cursor:
            params['cursor'] = cursor

        logger.debug(f"Requesting events with params: {params}")
        resp = requests.get(base_url_events, params=params)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch events: {resp.status_code}")
            break

        data = resp.json()
        batch_events = data.get("events", [])
        logger.info(f"Fetched {len(batch_events)} events in this batch")
        events.extend(batch_events)

        cursor = data.get("cursor")
        if not cursor:
            break

    logger.info(f"Total events fetched: {len(events)}")
    return events


def scrape_kalshi_events():
    """
    Merge current Kalshi events/markets with previously stored JSONs to:
      - keep active items,
      - add new items,
      - move no-longer-active items to resolved,
      - update market price snapshots for active markets.
    Writes four JSONs and returns their paths along with the final active events.
    """
    logger.info("Starting Kalshi event scraping...")
    timestamps = get_timestamps()
    timestamp_now = timestamps[0]

    final_events = []
    final_markets = []

    # Pull current events from Kalshi and limit to simpler (under-6-markets) ones.
    current_events = fetch_all_events(status='open', with_markets=True)
    current_events = [e for e in current_events if len(e['markets']) < 6]
    current_event_tickers = [e['event_ticker'] for e in current_events]

    # File names we read from and write back to.
    files = [
        "active_events.json", 
        "resolved_events.json",
        "active_markets.json", 
        "resolved_markets.json",
    ]

    # Load previous snapshots; these files are expected to exist beforehand.
    with open(files[0], "r") as f:
        previous_events = json.load(f)
    previous_event_tickers = [e['event_ticker'] for e in previous_events]

    with open(files[1], "r") as f:
        resolved_events = json.load(f)

    with open(files[2], "r") as f:
        previous_markets = json.load(f)
    previous_market_tickers = [m['ticker'] for m in previous_markets]

    with open(files[3], "r") as f:
        resolved_markets = json.load(f)

    # Reconcile events: keep active ones; move disappeared ones to resolved.
    for event in previous_events:
        if event['event_ticker'] in current_event_tickers:
            logger.info(f"Event {event['event_ticker']} is still active.")
            # TODO: add a timestamped research report for this event
            if "bing_reports" not in event:
                event['bing_reports'] = {}

            # Ensure 'ddgs_reports' field exists, then try to backfill last 3 days.
            if "ddgs_reports" not in event:
                event['ddgs_reports'] = {}
            for timestamp in timestamps:
                if timestamp not in event["ddgs_reports"]:
                    report = read_from_db(timestamp, event["event_ticker"])
                    if report is not None:
                        # generate unique hash id
                        hash_id = f"ddgs_{event['event_ticker'].lower()}_{timestamp}"
                        # save hash id in events.json
                        event['ddgs_reports'][timestamp] = hash_id

            # Keep the event active.
            final_events.append(event)
            
        else:
            logger.info(f"Event {event['event_ticker']} is no longer active.")
            resolved_event = None
            trials = 0
            while resolved_event == None and trials < 3:
                try:
                    resp = requests.get(
                        f"https://api.elections.kalshi.com/trade-api/v2/events/{event['event_ticker']}?with_nested_markets=true",
                        timeout=15
                    )
                    resp.raise_for_status()
                    resolved_event = resp.json()["event"]
                except Exception as e:
                    logger.error(f"Failed to fetch details for resolved event {event['event_ticker']}: {e}")
                    trials += 1
                    continue

            earliest_open_time = None
            latest_close_time = None
            is_resolved = True
            has_resolved = False
            if "markets" not in resolved_event:
                continue
            for market in resolved_event["markets"]:
                # parse 2025-08-26T19:30:40.273125Z to datetime
                open_time_format = "%Y-%m-%dT%H:%M:%SZ" if "." not in market["open_time"] else "%Y-%m-%dT%H:%M:%S.%fZ"
                close_time_format = "%Y-%m-%dT%H:%M:%SZ" if "." not in market["close_time"] else "%Y-%m-%dT%H:%M:%S.%fZ"
                open_time = dt.datetime.strptime(market["open_time"], open_time_format)
                close_time = dt.datetime.strptime(market["close_time"], close_time_format)
                if earliest_open_time is None or open_time < earliest_open_time:
                    earliest_open_time = open_time
                if latest_close_time is None or close_time > latest_close_time:
                    latest_close_time = close_time

                if market["status"] == "active" or market["status"] == "initialized":
                    is_resolved = False
                if market["status"] != "active" and market["status"] != "initialized" and market["result"] in ["yes", "no"]:
                    has_resolved = True

            event['resolution_date'] = latest_close_time.strftime("%Y-%m-%d")
            event['latest_close_time'] = latest_close_time.strftime("%Y-%m-%d")
            event['earliest_open_time'] = earliest_open_time.strftime("%Y-%m-%d")
            event['category'] = resolved_event.get("category", "Uncategorized")
            event['is_resolved'] = is_resolved
            event['has_resolved'] = has_resolved

            # Ensure 'ddgs_reports' field exists, then try to backfill last 3 days.
            if "ddgs_reports" not in event:
                event['ddgs_reports'] = {}
            for timestamp in timestamps:
                if timestamp not in event["ddgs_reports"]:
                    report = read_from_db(timestamp, event["event_ticker"])
                    if report is not None:
                        # generate unique hash id
                        hash_id = f"ddgs_{event['event_ticker'].lower()}_{timestamp}"
                        # save hash id in events.json
                        event['ddgs_reports'][timestamp] = hash_id

            resolved_events.append(event)
    
    # Add newly active events not seen in previous snapshot.
    for event in current_events:
        if event['event_ticker'] not in previous_event_tickers:
            logger.info(f"New active event found: {event['event_ticker']}")
            # TODO: add a timestamped research report for this event
            event_obj = {}
            event_obj['bing_reports'] = {}
            event_obj['ddgs_reports'] = {}
            
            # find ddgs report for today
            report = read_from_db(timestamp_now, event['event_ticker'])
            if report is not None:
                # generate unique hash id
                hash_id = f"ddgs_{event['event_ticker'].lower()}_{timestamp_now}"
                # save hash id in events.json
                event_obj['ddgs_reports'][timestamp_now] = hash_id

            event_obj['event_ticker'] = event['event_ticker']
            event_obj['series_ticker'] = event['series_ticker']
            event_obj['title'] = event['title']
            event_obj['sub_title'] = event['sub_title']
            event_obj['mutually_exclusive'] = event['mutually_exclusive']
            event_obj['category'] = event['category']
            final_events.append(event_obj)

        # Markets within this event: add or update active ones.
        markets = event.get("markets", [])
        for market in markets:
            if market['status'] == "active":
                yes_bid = market.get("yes_bid", "")
                no_bid = market.get("no_bid", "")
                last_price = market.get("last_price", "")
                
                if market['ticker'] not in previous_market_tickers:
                    logger.info(f"New market found: {market['ticker']}")
                    market_obj = {}
                    market_obj['ticker'] = market.get("ticker", "")
                    market_obj['event_ticker'] = market.get("event_ticker", "")
                    market_obj['title'] = market.get("title", "")
                    market_obj['subtitle'] = market.get("subtitle", "")
                    market_obj['yes_sub_title'] = market.get("yes_sub_title", "")
                    market_obj['no_sub_title'] = market.get("no_sub_title", "")
                    market_obj['rules_primary'] = market.get("rules_primary", "")
                    market_obj['rules_secondary'] = market.get("rules_secondary", "")
                    market_obj['open_time'] = market.get("open_time", "")
                    market_obj['close_time'] = market.get("close_time", "")
                    market_obj['expiration_time'] = market.get("expiration_time", "")
                    market_obj['status'] = market.get("status", "")
                    market_obj['response_price_units'] = market.get("response_price_units", "")
                    market_obj['yes_bid'] = yes_bid
                    market_obj['yes_ask'] = market.get("yes_ask", "")
                    market_obj['no_bid'] = no_bid
                    market_obj['no_ask'] = market.get("no_ask", "")
                    market_obj['last_price'] = last_price
                    market_obj['volume'] = market.get("volume", "")
                    market_obj['liquidity'] = market.get("liquidity", "")
                    # Price snapshot for today:
                    market_price = yes_bid / (yes_bid + no_bid) if (yes_bid + no_bid) > 0 else last_price / 100
                    market_obj['market_price'] = {timestamp: market_price}
                    final_markets.append(market_obj)

                else:
                    logger.info(f"Market {market['ticker']} is still active.")
                    # Update fields on previously known market; keep other fields intact.
                    prev_market = next((m for m in previous_markets if m['ticker'] == market['ticker']), None)
                    if prev_market:
                        prev_market.update({
                            "yes_bid": market.get("yes_bid", prev_market.get("yes_bid", "")),
                            "yes_ask": market.get("yes_ask", prev_market.get("yes_ask", "")),
                            "no_bid": market.get("no_bid", prev_market.get("no_bid", "")),
                            "no_ask": market.get("no_ask", prev_market.get("no_ask", "")),
                            "last_price": market.get("last_price", prev_market.get("last_price", "")),
                            "volume": market.get("volume", prev_market.get("volume", "")),
                            "liquidity": market.get("liquidity", prev_market.get("liquidity", "")),
                        })
                        # Append today's price snapshot:
                        # WARNING: assumes 'market_price' dict exists on prev_market.
                        market_price = yes_bid / (yes_bid + no_bid) if (yes_bid + no_bid) > 0 else last_price / 100
                        prev_market['market_price'][timestamp_now] = market_price
                        final_markets.append(prev_market)

    # Any previously-known market not seen as active now is considered resolved.
    final_market_tickers = [m['ticker'] for m in final_markets]
    for market in previous_markets:
        if market['ticker'] not in final_market_tickers:
            logger.info(f"Market {market['ticker']} is no longer active.")
            market['resolution_date'] = timestamp_now
            resolved_markets.append(market)

    # For events that resolved within 5 days from today, try to backfill last 3 days of reports.
    for index, event in enumerate(resolved_events):
        latest_close_time = event.get("latest_close_time", None)
        if latest_close_time is None:
            continue
        latest_close_time = dt.datetime.strptime(latest_close_time, "%Y-%m-%d")
        if latest_close_time >= dt.datetime.utcnow() - dt.timedelta(days=5):
            print(f"Backfilling reports for recently resolved event {event['event_ticker']}")
            if "ddgs_reports" not in event:
                resolved_events[index]['ddgs_reports'] = {}
            for timestamp in timestamps:
                if timestamp not in event["ddgs_reports"]:
                    report = read_from_db(timestamp, event["event_ticker"])
                    if report is not None:
                        # generate unique hash id
                        hash_id = f"ddgs_{event['event_ticker'].lower()}_{timestamp}"
                        # save hash id in events.json
                        resolved_events[index]['ddgs_reports'][timestamp] = hash_id

    # Persist updated snapshots to disk.
    with open(files[0], "w") as f:
        json.dump(final_events, f, indent=4)
    with open(files[1], "w") as f:
        json.dump(resolved_events, f, indent=4)
    with open(files[2], "w") as f:
        json.dump(final_markets, f, indent=4)
    with open(files[3], "w") as f:
        json.dump(resolved_markets, f, indent=4)

    return files, final_events


def push_to_github_repo(filepath, github_token, repo_full, branch='main'):
    """
    Create or update a single file in a GitHub repo via the Contents API.
    Args:
        filepath (str): Local path to file that has been updated.
        github_token (str): PAT or Actions token with 'contents: write'.
        repo_full (str): 'owner/repo' format.
        branch (str): Branch name to update (default 'main').
    Returns:
        str | None: The GitHub HTML URL of the updated file, if successful.
    """
    owner, repo = repo_full.split("/", 1)
    filename = os.path.basename(filepath)

    with open(filepath, "r") as f:
        content = f.read()
    content_encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    base = f"https://api.github.com/repos/{owner}/{repo}/contents/{filename}"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }

    # Get current SHA if file exists; required to update an existing file.
    r = requests.get(base, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None
    timestamps = get_timestamps()

    data = {
        "message": f"Update {filename} - {timestamps[0]}",
        "content": content_encoded,
        "branch": branch,
    }
    if sha:
        data['sha'] = sha

    r = requests.put(base, json=data, headers=headers)
    if r.status_code in (200, 201):
        url = r.json()['content']['html_url']
        logger.info(f"✅ Pushed {filename} to GitHub: {url}")
        return url
    logger.error(f"❌ Failed to push {filename}: {r.status_code} {r.text}")
    return None


def main():
    """
    Entry point:
      - Validate required env vars.
      - Scrape and reconcile Kalshi snapshots.
      - Push updated JSONs to GitHub.
      - Emit a simple summary (counts + uploaded URLs).
    """
    github_token = os.getenv("GITHUB_TOKEN")
    repo_full = os.getenv("GITHUB_REPOSITORY")  # e.g., "owner/repo"

    if not github_token:
        logger.error("Missing GITHUB_TOKEN in environment.")
        return
    if not repo_full:
        logger.error("Missing GITHUB_REPOSITORY in environment (owner/repo).")
        return

    files, events = scrape_kalshi_events()
    if not files:
        logger.error("Failed to scrape events")
        return

    urls = {}
    for path in files:
        if os.path.exists(path):
            url = push_to_github_repo(path, github_token, repo_full)
            if url:
                urls[path] = url

    with open("github_urls.json", "w") as f:
        json.dump(urls, f, indent=2)

    logger.info("=== Summary ===")
    logger.info(f"Processed {len(events)} events")
    for path, url in urls.items():
        logger.info(f"{path} -> {url}")

if __name__ == "__main__":
    main()