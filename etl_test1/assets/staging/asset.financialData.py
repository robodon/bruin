"""@bruin
name: dataset.financial_data
type: python
connection: duckdb
description: this is a python asset

schedule: daily

materialization:
  type: table

@bruin"""

import os
import pandas as pd
from datetime import datetime, timezone

FILE_PATH = os.path.join(os.path.dirname(__file__), "Financial Sample.csv")


def materialize():
    if not os.path.exists(FILE_PATH):
        return pd.DataFrame(columns=["col1", "_ingested_at"])

    df = pd.read_csv(FILE_PATH)
    df["_ingested_at"] = datetime.now(timezone.utc)

    print(df.head())
    return df