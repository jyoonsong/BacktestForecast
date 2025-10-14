import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from typing import List, Dict, Any
from .utils import log
from .config import NUM_URLS

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def search_ddgs(query: str, num_urls: int = NUM_URLS) -> List[Dict[str, Any]]:
    """Perform DuckDuckGo search and deduplicate results."""
    results = list(DDGS().text(query, max_results=num_urls * 2, timelimit="y") or [])
    seen, deduped = set(), []
    for r in results:
        href = r.get("href")
        if href and href not in seen:
            seen.add(href)
            deduped.append(r)
    return deduped

def scrape_urls(search_results: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Scrape HTML pages and extract paragraphs."""
    contents = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for result in search_results:
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

def filter_contents(contents: List[Dict[str, str]], market_descriptions: str, num_urls: int = NUM_URLS) -> List[Dict[str, str]]:
    """Filter articles based on relevance to market descriptions."""
    
    # calculate semantic similarity between market descriptions and article content
    for content in contents:
        vectorizer = TfidfVectorizer().fit_transform([market_descriptions, content["article"]])
        vectors = vectorizer.toarray()
        content["similarity"] = cosine_similarity([vectors[0]], [vectors[1]])[0][0]

    # sort by similarity and return top num_urls
    contents.sort(key=lambda x: x.get("similarity", 0), reverse=True)
    return contents[:num_urls]