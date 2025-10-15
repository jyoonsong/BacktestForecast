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
- **Active events file** – tracking events currently monitored.
- **Resolved events file** – tracking events no longer active.

**3. Reconcile current API results with existing data**

- **Retain events** that are still active. Check if context snapshots exist for each event.
- **Identify dropped events** (present previously, but absent in API results). Move these to the resolved events list. Update timestamps or resolution status if necessary.

**4. Append newly discovered active events**

- Detect events that appear in the fresh API results but are not in existing records.
- Add these new active events to the active dataset. Check if context snapshots exist for each event.

**5. Generate a stratified random sample of active events**

- From the final active event set, draw a sample of 210 events.
- Use **stratified sampling** to ensure balanced representation across event domains.


### Task 2: Daily Report Generator

The codes for the Daily Report Generator task is in `kalshi_ddgs_rag` directory.

**1. Calculate**

-   hi
-   hello

**2. Calculate**

-   hi
-   hello
