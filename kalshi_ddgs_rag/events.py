import requests, random, time
from collections import Counter
from typing import List, Dict
from .utils import log

def fetch_sampled_events() -> List[Dict[str, any]]:
    """Fetch sampled active events from GitHub."""
    url = "https://raw.githubusercontent.com/jyoonsong/FutureBench/refs/heads/main/data/sampled_events.json"
    for _ in range(5):
        try:
            return requests.get(url, timeout=10).json()
        except Exception as e:
            log(f"Retrying fetch_current_events: {e}")
            time.sleep(2)
    raise RuntimeError("Failed to fetch events after retries.")
