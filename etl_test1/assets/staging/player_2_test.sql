/* @bruin

name: staging.player_2_test
type: duckdb.sql

materialization:
  type: table

depends:
  - dataset.player_stats

columns:
  - name: name
    type: string
    description: this column contains the player names
    checks:
      - name: not_null
      - name: unique
  - name: player_count
    type: int
    description: the number of players with the given name
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: row count is greater than zero
    description: this check ensures that the table is not empty
    value: 1
    query: SELECT count(*) > 1 FROM dataset.player_stats

@bruin */

SELECT name
  ,count(*) AS player_count
  ,UPPER(name) as name_upper
FROM dataset.player_stats
GROUP BY 1
