"""@bruin

description: this is a python asset

@bruin"""

"""@bruin
name: dataset.financial_data
type: python
connection: duckdb

materialization:
  type: table

@bruin"""

import os
import pandas as pd

FILE_PATH = "./Financial Sample.csv"


def materialize():
    if not os.path.exists(FILE_PATH):
        # Return empty dataframe → transparent "no data" run
        return pd.DataFrame(columns=["col1"])

    df = pd.read_csv(FILE_PATH)
    print(df.head())

    return df
