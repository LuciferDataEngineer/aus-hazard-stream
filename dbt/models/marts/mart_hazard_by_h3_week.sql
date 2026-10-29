with quakes as (
    select
        date_trunc('week', origin_time) as week_start,
        h3_res4,
        count(*) as quake_count,
        max(magnitude) as max_magnitude
    from {{ ref('fct_earthquake') }}
    where h3_res4 is not null
      and magnitude >= {{ var('mag_floor') }}
    group by 1, 2
),
hotspots as (
    select
        date_trunc('week', observed_at) as week_start,
        h3_res4,
        count(*) as hotspot_count,
        count(*) filter (where near_population) as hotspot_near_population,
        count(*) filter (where near_airport) as hotspot_near_airport
    from {{ ref('fct_hotspot') }}
    where h3_res4 is not null
    group by 1, 2
)
select
    coalesce(quakes.week_start, hotspots.week_start) as week_start,
    coalesce(quakes.h3_res4, hotspots.h3_res4) as h3_res4,
    coalesce(quakes.quake_count, 0) as quake_count,
    quakes.max_magnitude,
    coalesce(hotspots.hotspot_count, 0) as hotspot_count,
    coalesce(hotspots.hotspot_near_population, 0) as hotspot_near_population,
    coalesce(hotspots.hotspot_near_airport, 0) as hotspot_near_airport
from quakes
full outer join hotspots
    on quakes.week_start = hotspots.week_start
   and quakes.h3_res4 = hotspots.h3_res4
