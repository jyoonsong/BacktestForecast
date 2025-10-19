import os
import requests
import time
from .utils import log, utc_stamp
from .db import read_from_db, write_to_db
from .events import fetch_sampled_events
from .summarization import get_ddgs_report

def main():
    print("Starting daily report generation...")
    timestamp = utc_stamp()
    events = fetch_sampled_events()
    log(f"Fetched {len(events)} events from GitHub.")

    # Read K from environment or default
    K = int(os.getenv("K", 0))

    for e in events[K : K+70]:
        ticker = e["event_ticker"]
        if read_from_db(timestamp, ticker):
            log(f"Already exists: {ticker}, skipping.")
            continue

        event = None
        trials = 0
        while event == None and trials < 5:
            try:
                resp = requests.get(
                    f"https://api.elections.kalshi.com/trade-api/v2/events/{ticker}?with_nested_markets=true",
                    timeout=15,
                )
                event = resp.json().get("event", {})
                if not event.get("markets"):
                    log(f"No markets for {ticker}, skipping.")
                    event = None
                    continue
            except Exception as err:
                log(f"Error fetching event {ticker}: {err}")
                trials += 1
                time.sleep(3)
        
        if event is None:
            log(f"Failed to fetch event {ticker} after retries, skipping.")
            continue
        
        report, contents = get_ddgs_report(event)
        write_to_db(report, contents, timestamp, ticker)
        
    log("Report generation completed.")

if __name__ == "__main__":
    main()
