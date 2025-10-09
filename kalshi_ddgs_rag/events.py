import requests, random, time
from collections import Counter
from typing import List, Dict
from .utils import log
from .config import TARGET_EVENTS

def fetch_current_events() -> List[Dict[str, any]]:
    """Fetch active events from GitHub."""
    url = "https://raw.githubusercontent.com/jyoonsong/FutureBench/refs/heads/main/active_events.json"
    for _ in range(5):
        try:
            return requests.get(url, timeout=10).json()
        except Exception as e:
            log(f"Retrying fetch_current_events: {e}")
            time.sleep(2)
    raise RuntimeError("Failed to fetch events after retries.")

def stratified_sample_events(events: List[Dict[str, any]], target: int = TARGET_EVENTS) -> List[Dict[str, any]]:
    """Stratified sampling of events across categories."""
    if len(events) <= target:
        return events
    random.seed(37)
    cats = {}
    for e in events:
        cats.setdefault(e["category"], []).append(e)
    sampled, remaining = [], target
    cat_lists = sorted(cats.values(), key=len)
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
