/* @bruin

name: staging.financial_data
type: duckdb.sql
tags:
  - CDO

materialization:
  type: table
  strategy: create+replace

depends:
  - dataset.financial_data
owner: robi@bach.de

columns:
  - name: segment_country_product_discount_band_units_sold_manufacturing_price_sale_price_gross_sales_discounts_sales_cogs_profit_date_month_number_month_name_yearx
    type: VARCHAR
  - name: _ingested_at
    type: TIMESTAMP
  - name: segment
    type: VARCHAR
  - name: country
    type: VARCHAR
  - name: product
    type: VARCHAR
  - name: discount_band
    type: VARCHAR
  - name: units_sold
    type: VARCHAR
  - name: manufacturing_price
    type: VARCHAR
  - name: sale_price
    type: VARCHAR
  - name: gross_sales
    type: VARCHAR
  - name: discounts
    type: VARCHAR
  - name: sales
    type: VARCHAR
  - name: cogs
    type: VARCHAR
  - name: profit
    type: VARCHAR
  - name: date
    type: VARCHAR
  - name: month_number
    type: BIGINT
  - name: month_name
    type: VARCHAR
  - name: year
    type: BIGINT

@bruin */

SELECT *
FROM dataset.financial_data
