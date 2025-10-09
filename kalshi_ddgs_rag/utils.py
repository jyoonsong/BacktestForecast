import datetime as dt

def utc_stamp() -> str:
    """Return current UTC date as YYYYMMDD string."""
    return dt.datetime.utcnow().strftime("%Y%m%d")

def log(message: str):
    """Simple timestamped logger."""
    now = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{now}] {message}")
