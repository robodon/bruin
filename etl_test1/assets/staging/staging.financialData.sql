/* @bruin

name: staging.financial_data
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

depends:
  - dataset.financial_data

@bruin */

SELECT *
FROM dataset.financial_data