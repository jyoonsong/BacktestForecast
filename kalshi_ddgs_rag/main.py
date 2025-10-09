import requests
from .utils import log, utc_stamp
from .db import read_from_db, write_to_db
from .events import fetch_current_events, stratified_sample_events
from .summarization import get_ddgs_report

def main():
    print("Starting daily report generation...")
    timestamp = utc_stamp()
    events = fetch_current_events()
    log(f"Fetched {len(events)} events from GitHub.")

    sampled = stratified_sample_events(events)

    for e in sampled:
        ticker = e["event_ticker"]
        if read_from_db(timestamp, ticker):
            log(f"Already exists: {ticker}, skipping.")
            continue

        try:
            resp = requests.get(
                f"https://api.elections.kalshi.com/trade-api/v2/events/{ticker}?with_nested_markets=true",
                timeout=15,
            )
            event = resp.json().get("event", {})
            if not event.get("markets"):
                continue
            report, contents = get_ddgs_report(event)
            write_to_db(report, contents, timestamp, ticker)
        except Exception as err:
            log(f"Failed processing {ticker}: {err}")

    log("Report generation completed.")

if __name__ == "__main__":
    main()
