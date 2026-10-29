-- Publication lag and catalogue freshness.
-- GA creation-minus-origin is the closest thing we have to first-alert latency.
-- USGS summary feeds only expose the last update, so that lag is an upper bound.
-- Lags longer than 7 days are dropped: older GA rows often carry a database
-- load time rather than the original alert time.
with quake_asof as (
    select max(origin_time) as asof_time, max(ingested_at) as ingested_at
    from {{ ref('fct_earthquake') }}
),
hot_asof as (
    select max(observed_at) as asof_time, max(ingested_at) as ingested_at
    from {{ ref('fct_hotspot') }}
),
ga as (
    select
        'geoscience_australia' as feed,
        'earthquake' as hazard,
        count(*) as n,
        median(publication_lag_seconds) / 60.0 as p50_minutes,
        quantile_cont(publication_lag_seconds, 0.95) / 60.0 as p95_minutes,
        max(q.ingested_at) as last_ingested_at,
        max(origin_time) as last_event_time,
        date_diff('minute', max(origin_time), max(q.ingested_at)) as freshness_minutes
    from {{ ref('fct_earthquake') }} as q
    cross join quake_asof
    where sources like '%ga%'
      and origin_time >= quake_asof.asof_time - interval 30 day
      and lag_kind = 'creation'
      and publication_lag_seconds between 0 and 7 * 24 * 3600
),
usgs as (
    select
        'usgs' as feed,
        'earthquake' as hazard,
        count(*) as n,
        median(publication_lag_seconds) / 60.0 as p50_minutes,
        quantile_cont(publication_lag_seconds, 0.95) / 60.0 as p95_minutes,
        max(q.ingested_at) as last_ingested_at,
        max(origin_time) as last_event_time,
        date_diff('minute', max(origin_time), max(q.ingested_at)) as freshness_minutes
    from {{ ref('fct_earthquake') }} as q
    cross join quake_asof
    where sources = 'usgs'
      and origin_time >= quake_asof.asof_time - interval 30 day
      and publication_lag_seconds between 0 and 7 * 24 * 3600
),
dea_lag as (
    select processing_lag_seconds
    from {{ ref('fct_hotspot') }}
    where processing_lag_seconds between 0 and 24 * 3600
),
dea as (
    select
        'dea_hotspots' as feed,
        'hotspot' as hazard,
        (select count(*) from dea_lag) as n,
        (select median(processing_lag_seconds) / 60.0 from dea_lag) as p50_minutes,
        (select quantile_cont(processing_lag_seconds, 0.95) / 60.0 from dea_lag) as p95_minutes,
        (select max(ingested_at) from {{ ref('fct_hotspot') }}) as last_ingested_at,
        (select max(observed_at) from {{ ref('fct_hotspot') }}) as last_event_time,
        (
            select date_diff('minute', max(observed_at), max(ingested_at))
            from {{ ref('fct_hotspot') }}
        ) as freshness_minutes
)
select * from ga
union all
select * from usgs
union all
select * from dea
