/* @bruin

name: tailored.union
type: duckdb.sql

materialization:
  type: table

depends:
  - curated.orders
  - staging.postgres_orders


custom_checks:
  - name: row count is greater than zero
    description: this check ensures that the table is not empty
    value: 1
    query: SELECT count(*) > 1 FROM dataset.postgres_orders

@bruin */

SELECT *
FROM staging.postgres_orders
UNION ALL
SELECT * 
FROM staging.postgres_orders
