import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from typing import List, Dict, Any
from .utils import log
from .config import NUM_URLS

def search_ddgs(query: str, num_urls: int = NUM_URLS) -> List[Dict[str, Any]]:
    """Perform DuckDuckGo search and deduplicate results."""
    results = list(DDGS().text(query, max_results=num_urls * 2, timelimit="y") or [])
    seen, deduped = set(), []
    for r in results:
        href = r.get("href")
        if href and href not in seen:
            seen.add(href)
            deduped.append(r)
    return deduped[:num_urls]

def scrape_urls(search_results: List[Dict[str, Any]], num_urls: int = NUM_URLS) -> List[Dict[str, str]]:
    """Scrape HTML pages and extract paragraphs."""
    contents = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for result in search_results[:num_urls]:
        url = result.get("href")
        if not url:
            continue
        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            paragraphs = soup.find_all("p")
            text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
            if 200 <= len(text) <= 100000:
                contents.append({
                    "title": result.get("title", ""),
                    "body": result.get("body", ""),
                    "href": url,
                    "article": text,
                })
        except Exception as e:
            log(f"Scrape failed for {url}: {e}")
    return contents
