# BacktestForecast

## Abstract

The rise of large language models (LLMs) has made scalable forecasting increasingly feasible, as these models have access to massive amounts of context. Yet evaluating their forecasting ability presents three methodological challenges. Standard benchmarks are vulnerable to **temporal contamination**, where outcomes are already known before the model’s training cutoff, and to **staleness confounds**, where newer models gain unfair advantage from fresher data. Dynamic benchmarks address temporal leakage by tracking unresolved questions, but this results in **long evaluation delays**, since evaluators must wait for outcomes to resolve before judging the accuracy. We address these issues with a forward-only, backtestable evaluation framework built on frozen context snapshots: contemporaneous, structured summaries of web search results paired with forecasting questions. Our pipeline continuously scrapes unresolved questions from prediction markets and captures their supporting context at the time of scraping, eliminating temporal contamination and mitigating staleness effects. Once questions resolve, these snapshots enable rapid backtesting of diverse forecasting strategies, substantially accelerating research cycles. This framework provides a rigorous, reproducible, and open-source foundation for studying the forecasting capabilities of LLMs. Through two experiments, we demonstrate that our approach enables the rapid identification of effective forecasting strategies.

## Architecture

This repository leverages [GitHub Actions](https://github.com/features/actions) to automate daily cron jobs. Each day, two main tasks are executed: the Daily Kalshi Scraper and the Daily Report Generator. 

The scraper runs within a single workflow, while the report generation is split across multiple workflows to prevent timeouts, as each GitHub Actions job is capped at 6 hours. To efficiently stay within this limit, we generate approximately 70 reports per workflow, which takes up to 2 hours each. By running three separate workflows for report generation, we produce a total of 210 reports daily while safely avoiding timeout constraints.

### Task 1: Daily Kalshi Scraper

The code for the **Daily Kalshi Scraper** task is in `scrape-kalshi.py`.

**1. Fetch all active events from Kalshi**

- Use the [Kalshi API](https://docs.kalshi.com/api-reference/market/get-events) to retrieve all events with `open` status.
- Filter for simpler events by excluding those with 6 or more markets.
- Normalize and store event metadata (e.g., event ticker, title, domain/category).

**2. Load existing active and resolved event records**

- Load the current datasets from the `data` directory:
- `data/active_events.json` – tracking events currently monitored.
- `data/resolved_events.json` – tracking events no longer active.

**3. Reconcile current API results with existing data**

- Retain events that are still active. Check if context snapshots exist for each event.
- Identify dropped events (present previously, but absent in API results). Move these to the resolved events list. Update timestamps or resolution status if necessary.

**4. Append newly discovered active events**

- Detect events that appear in the fresh API results but are not in existing records.
- Add these new active events to the active dataset. Check if context snapshots exist for each event.

**5. Generate a stratified random sample of active events**

- From the final active event set, draw a sample of 210 events.
- Use stratified sampling to ensure balanced representation across event domains.


### Task 2: Daily Report Generator

The codebase for the **Daily Report Generator** is located in the `kalshi_ddgs_rag` directory. The primary entry point is `kalshi_ddgs_rag/main.py`.

**1. Fetch the Sample of Events**

- Load a sample of **210 events** from `data/sampled_events.json`.
- Based on the workflow index, process **70 events** per run. For example, the 3rd workflow handles the last 70 events.
- Implemented in: `kalshi_ddgs_rag/events.py`.

**2. Generate Search Queries via OpenAI API**

- For each event, generate **6 search queries** using OpenAI (customizable via `NUM_QUERIES` in `kalshi_ddgs_rag/config.py`.)
- Each query must be **< 7 words** (customizable via `MAX_QUERY_WORDS` in `kalshi_ddgs_rag/config.py`.)
- Prompt includes the market descriptions (title, subtitle, resolution rules) and instructions to generate queries that would meaningfully improve the accuracy and confidence of a forecast regarding the market outcomes.
- Implemented in: `kalshi_ddgs_rag/summarization.py` and `kalshi_ddgs_rag/openai_utils.py`

**3. Retrieve URLs with DDGS Search**

- Perform a DuckDuckGo search for each query using the [DDGS library](https://github.com/deedy5/ddgs).
- We retrieve a total of 10 URLs, which corresponds to twice the value of `NUM_URLS` (customizable via `NUM_URLS` in kalshi_ddgs_rag/config.py).
- Deduplicate all fetched URLs.
- Implemented in: `kalshi_ddgs_rag/search_utils.py`.

**4. Scrape URL Content with BeautifulSoup**

- Parse each URL’s HTML.
- Extract textual content from `<p>` tags using **BeautifulSoup**.
- Implemented in: `kalshi_ddgs_rag/search_utils.py`.

**5. Filter URLs via Cosine Similarity**

- Compute semantic similarity between scraped content and **market metadata**.
- Select **top 5 URLs** (customizable via `NUM_URLS` in `kalshi_ddgs_rag/config.py`.)
- Implemented in: `kalshi_ddgs_rag/search_utils.py`.

**6. Summarize Filtered URLs via OpenAI API**

- For each of the final five URLs, we send the scraped text content to the OpenAI API and request a structured summary.
- Specifically, we instruct the model to: "Generate one paragraph per relevant article summarizing factual insights or context related to these markets. Avoid subjective statements. Include the article date and source URL at the end of each paragraph. Exclude articles that are entirely unrelated."
- This process is repeated for each of the six search queries, producing one summarized section per query.
- We then concatenate the six sections, resulting in a consolidated report covering 30 URLs in total (5 URLs × 6 queries). This aggregated report is referred to as a *context snapshot*.
- Implemented in:  `kalshi_ddgs_rag/summarization.py` and `kalshi_ddgs_rag/openai_utils.py`

**7. Save the Report to MongoDB**

- We store the current timestamp, final report, and the corresponding event ticker in the MongoDB database.
- Implemented in: `kalshi_ddgs_rag/db.py`

