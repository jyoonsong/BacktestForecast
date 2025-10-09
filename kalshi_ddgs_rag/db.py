from typing import List, Dict, Any
from .config import db
from .utils import log

def write_to_db(report: str, contents: List[Dict[str, Any]], timestamp: str, event_ticker: str):
    """Insert report document into MongoDB."""
    collection = db["reports"]
    data = {"timestamp": timestamp, "event_ticker": event_ticker, "ddgs_report": report}
    result = collection.insert_one(data)
    print(f"Inserted document with _id: {result.inserted_id}")

def read_from_db(timestamp: str, event_ticker: str) -> str | None:
    """Retrieve a stored report from MongoDB if exists."""
    collection = db["reports"]
    record = collection.find_one({"timestamp": timestamp, "event_ticker": event_ticker})
    return record["ddgs_report"] if record else None
