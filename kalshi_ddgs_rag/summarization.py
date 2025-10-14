from typing import List, Dict, Tuple
from .openai_utils import run_openai
from .search_utils import search_ddgs, scrape_urls, filter_contents
from .config import NUM_QUERIES, MAX_QUERY_WORDS
from .utils import log

def get_market_descriptions(event: Dict[str, any]) -> str:
    """Generate readable descriptions for markets."""
    markets = event["markets"]
    if len(markets) == 1:
        m = markets[0]
        desc = (
            f"Event title: {event['title']}\n"
            f"Title: {m['title']}\n"
            f"Subtitle: {m['yes_sub_title']}\n"
            f"Possible Outcomes: Yes (0) or No (1)\n"
            f"Rules: {m['rules_primary']}\n"
        )
        if m.get("rules_secondary"):
            desc += f"Secondary rules: {m['rules_secondary']}\n"
        desc += f"Scheduled close date: {m['expiration_time']}\n"
        return desc

    desc = ""
    for idx, m in enumerate(markets, start=1):
        desc += (
            f"# Market {idx}\n"
            f"Ticker: {m['ticker']}\n"
            f"Title: {m['title']}\n"
            f"Subtitle: {m.get('yes_sub_title', '')}\n"
            f"Possible Outcomes: Yes (0) or No (1)\n"
            f"Rules: {m.get('rules_primary', '')}\n"
        )
        if m.get("rules_secondary"):
            desc += f"Secondary rules: {m['rules_secondary']}\n"
        desc += f"Scheduled close date: {m['expiration_time']}\n\n"
    return desc

def generate_search_queries(event: Dict[str, any], market_descriptions: str) -> List[str]:
    """Generate short search queries via OpenAI."""
    prompt = f"""
The following are markets under the event titled "{event['title']}". 

{market_descriptions}

# Instructions
What are {NUM_QUERIES} short search queries that would meaningfully improve the accuracy and confidence of a forecast regarding the market outcomes described above? 
Output exactly {NUM_QUERIES} queries, one query per line, without any other text or number. 
Each query should be less than {MAX_QUERY_WORDS} words.
Do not include numbers, symbols, or explanations.
"""
    output = run_openai(prompt)
    return [line.strip() for line in output.splitlines() if line.strip()]

def summarize_articles(contents: List[Dict[str, str]], event: Dict[str, any], market_descriptions: str) -> str:
    """Summarize scraped articles via OpenAI."""
    all_articles = ""
    for i, c in enumerate(contents, 1):
        all_articles += (
            f"# Article {i}\n"
            f"Title: {c['title']}\n"
            f"Body: {c['body']}\n"
            f"Source URL: {c['href']}\n"
            f"Full Content: {c['article']}\n\n"
        )

    prompt = f"""
The following are markets under the event titled "{event['title']}".
{market_descriptions}

{all_articles}

# Instructions
Generate one paragraph per relevant article summarizing factual insights or context related to these markets. 
Avoid subjective statements. Include the article date and source URL at the end of each paragraph.
Exclude articles that are entirely unrelated.
"""
    return run_openai(prompt)

def process_query(query: str, event: Dict[str, any], market_descriptions: str) -> Tuple[str, List[Dict[str, str]]]:
    """Run full pipeline for a single search query."""
    results = search_ddgs(query)
    contents = scrape_urls(results)
    filtered_contents = filter_contents(contents, market_descriptions)
    summary = summarize_articles(filtered_contents, event, market_descriptions)
    return summary, filtered_contents

def get_ddgs_report(event: Dict[str, any]) -> Tuple[str, List[List[Dict[str, str]]]]:
    """Generate combined DDGS research report for one event."""
    market_descriptions = get_market_descriptions(event)
    queries = generate_search_queries(event, market_descriptions)
    summaries, all_contents = [], []
    for q in queries:
        summary, contents = process_query(q, event, market_descriptions)
        summaries.append(summary)
        all_contents.append(contents)
    report = "\n\n".join(f"# Research Report {i+1}\n{summary}" for i, summary in enumerate(summaries))
    return report.strip(), all_contents
