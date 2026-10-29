-- Physical ranges. accepted_range lives in dbt_utils; this keeps the project dependency-free.
select event_id as id, 'quake' as kind
from {{ ref('fct_earthquake') }}
where magnitude < -1
   or magnitude >= 10
   or latitude < -90
   or latitude > 90
   or longitude < -180
   or longitude > 180
union all
select hotspot_id, 'hotspot'
from {{ ref('fct_hotspot') }}
where confidence < 0
   or confidence > 100
