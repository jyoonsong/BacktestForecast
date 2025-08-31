# FutureBench

### TODOS

-   Archiving urls using `savepagenow`
-   Import bing reports
-   Move datasets to HuggingFace

```
jobs:
  build-and-push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install huggingface_hub datasets pyarrow fastparquet
      - name: Run scraper & build tables
        run: python scripts/build_tables.py --out data_out/
      - name: Push to HF Hub
        env: { HF_TOKEN: ${{ secrets.HF_TOKEN }} }
        run: |
          python scripts/push_to_hub.py --repo-id your-org/futurebench --path data_out/ --tag snapshot-$(date -u +%Y%m%d)
```
