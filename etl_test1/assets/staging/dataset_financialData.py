"""@bruin
name: dataset.financial_data
type: python
connection: duckdb-default
description: Load financial sample CSV into DuckDB periodically

schedule: daily

materialization:
  type: table

@bruin"""

import os
import polars as pl
from datetime import datetime, timezone

FILE_PATH = os.getenv(
    "FINANCIAL_DATA_PATH",
    "/Users/robin/Desktop/bruin_test/bruin/etl_test1/FinancialSample.csv"
)

# Explicit list of valid columns from the CSV header
VALID_COLUMNS = [
    "Segment", "Country", "Product", "Discount Band", "Units Sold",
    "Manufacturing Price", "Sale Price", "Gross Sales", "Discounts",
    "Sales", "COGS", "Profit", "Date", "Month Number", "Month Name", "Year"
]


def materialize():
    if not os.path.exists(FILE_PATH):
        print(f"WARNING: CSV file not found at {FILE_PATH}")
        return pl.DataFrame({"col1": [], "_ingested_at": []})

    # Read CSV and keep only valid columns
    df = pl.read_csv(FILE_PATH, separator=";", truncate_ragged_lines=True)

    # Drop any auto-named ragged columns (e.g. C17, C18)
    valid_cols = [col for col in VALID_COLUMNS if col in df.columns]
    df = df.select(valid_cols)

    # Add ingestion timestamp
    df = df.with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("_ingested_at")
    )

    print(f"Loaded {len(df)} rows with columns: {df.columns}")
    print(df.head())

    return df
