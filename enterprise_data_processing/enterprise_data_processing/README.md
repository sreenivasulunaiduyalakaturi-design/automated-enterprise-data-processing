# Automated Enterprise Data Processing

Pipeline: Ingest -> Validate/Clean -> Transform -> Load (SQLite) -> Report -> Archive.
Requires Python 3.8+ (standard library only).

## Run
    python pipeline.py --demo       # create sample files and process them
    python pipeline.py              # process all CSV/JSON files in input/ once
    python pipeline.py --watch 30   # check input/ every 30 seconds

Input columns: order_id, customer, region, product, quantity, unit_price

## Output
- enterprise.db  : clean data (SQLite)
- reports/       : revenue-by-region CSV
- rejected/      : bad rows with reasons
- archive/       : processed input files
- pipeline.log   : activity log
