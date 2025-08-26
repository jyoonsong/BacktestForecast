#!/usr/bin/env python3
import base64
import datetime as dt
import json
import logging
import os
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

base_url_events = "https://api.elections.kalshi.com/trade-api/v2/events"
base_url_markets = "https://api.elections.kalshi.com/trade-api/v2/markets"


def utc_stamp():
    # return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return dt.datetime.utcnow().strftime("%Y%m%d")


def fetch_all_events(status=None, with_markets=True):
    params = {}
    if status:
        params["status"] = status
    if with_markets:
        params["with_nested_markets"] = "true"

    events = []
    cursor = None

    logger.info(f"Fetching all events with status={status} and with_markets={with_markets}")

    while True:
        if cursor:
            params["cursor"] = cursor

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
    logger.info("Starting Kalshi event scraping...")
    timestamp = utc_stamp()

    final_events = []

    current_events = fetch_all_events(status='open', with_markets=True)
    current_events = [e for e in current_events if len(e['markets']) < 6]
    current_event_tickers = [e['event_ticker'] for e in current_events]

    files = ["data/active_events.json", "data/resolved_events.json"]

    with open(files[0], "r") as f:
        previous_events = json.load(f)
    previous_event_tickers = [e['event_ticker'] for e in previous_events]

    with open(files[1], "r") as f:
        resolved_events = json.load(f)

    for event in previous_events:
        if event["event_ticker"] in current_event_tickers:
            logger.info(f"Event {event['event_ticker']} is still active.")
            # TODO: add a timestamped research report for this event
            event["bing_reports"] = ""
            event["ddgs_reports"] = ""
            # save the event
            final_events.append(event)
            
        else:
            logger.info(f"Event {event['event_ticker']} is no longer active.")
            event["resolution_date"] = timestamp
            resolved_events.append(event)

    for event in current_events:
        if event["event_ticker"] not in previous_event_tickers:
            logger.info(f"New active event found: {event['event_ticker']}")
            # TODO: add a timestamped research report for this event
            event_obj = {}
            event_obj["bing_reports"] = ""
            event_obj["ddgs_reports"] = ""
            # save the event
            event_obj["event_ticker"] = event["event_ticker"]
            event_obj["series_ticker"] = event["series_ticker"]
            event_obj["title"] = event["title"]
            event_obj["sub_title"] = event["sub_title"]
            event_obj["mutually_exclusive"] = event["mutually_exclusive"]
            event_obj["category"] = event["category"]
            # save the event
            final_events.append(event_obj)

    with open(files[0], "w") as f:
        json.dump(final_events, f, indent=4)
    with open(files[1], "w") as f:
        json.dump(resolved_events, f, indent=4)

    return files, final_events


def push_to_github_repo(filepath, github_token, repo_full, branch='main'):
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

    # Get current SHA if file exists
    r = requests.get(base, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None

    data = {
        "message": f"Update {filename} - {utc_stamp()}",
        "content": content_encoded,
        "branch": branch,
    }
    if sha:
        data["sha"] = sha

    r = requests.put(base, json=data, headers=headers)
    if r.status_code in (200, 201):
        url = r.json()["content"]["html_url"]
        logger.info(f"✅ Pushed {filename} to GitHub: {url}")
        return url
    logger.error(f"❌ Failed to push {filename}: {r.status_code} {r.text}")
    return None


def main():
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