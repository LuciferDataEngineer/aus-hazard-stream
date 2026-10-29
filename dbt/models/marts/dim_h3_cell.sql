with quakes as (
    select
        h3_res4,
        count(*) as quake_count,
        max(magnitude) as max_magnitude,
        max(origin_time) as last_quake_time
    from {{ ref('fct_earthquake') }}
    where h3_res4 is not null
    group by 1
),
hotspots as (
    select
        h3_res4,
        count(*) as hotspot_count,
        count(*) filter (where near_population) as hotspot_near_population,
        count(*) filter (where near_airport) as hotspot_near_airport,
        max(observed_at) as last_hotspot_time
    from {{ ref('fct_hotspot') }}
    where h3_res4 is not null
    group by 1
)
select
    coalesce(quakes.h3_res4, hotspots.h3_res4) as h3_res4,
    4 as h3_resolution,
    coalesce(quakes.quake_count, 0) as quake_count,
    quakes.max_magnitude,
    quakes.last_quake_time,
    coalesce(hotspots.hotspot_count, 0) as hotspot_count,
    coalesce(hotspots.hotspot_near_population, 0) as hotspot_near_population,
    coalesce(hotspots.hotspot_near_airport, 0) as hotspot_near_airport,
    hotspots.last_hotspot_time
from quakes
full outer join hotspots on quakes.h3_res4 = hotspots.h3_res4
