/* @bruin

name: staging.financial_data
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.asset.financialData

@bruin */

SELECT *
FROM staging.asset.financialData
