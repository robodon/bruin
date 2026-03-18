/* @bruin
name: staging.player_2
type: duckdb.sql

depends:
    - assets.player_stats

materialization:
  type: table
  strategy: append


@bruin */

with load_player as (
    select * from staging.player_2
)

select *
    ,COALESCE(name,player_count) as new_field
from load_player
