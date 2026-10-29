-- Equal-length window comparison for the South Gippsland box and for
-- Victoria / Australia. Baseline windows are non-overlapping in the sense
-- that each historical window ends before the current window starts. They
-- step every 7 days and include empty windows, so a quiet week counts.
-- Magnitude floor is 2.0: below that, the GA catalogue is not complete
-- uniformly back to 2006, and a rate comparison would mix detection
-- improvements with seismicity.
-- Status: unusual if the current count is at or above the 99th percentile
-- of those windows and above the median; elevated at the 95th. This is a
-- descriptive rank, not a hypothesis test and not a forecast.

with span as (
    select
        timestamp '{{ var("swarm_start") }}' as swarm_start,
        max(origin_time) as asof_time
    from {{ ref('fct_earthquake') }}
),
base as (
    select
        event_id,
        origin_time,
        magnitude,
        depth_km,
        mag_type,
        cluster_id,
        state_name = 'Victoria' as in_victoria,
        latitude between {{ var('gippsland_min_lat') }} and {{ var('gippsland_max_lat') }}
            and longitude between {{ var('gippsland_min_lon') }} and {{ var('gippsland_max_lon') }}
            as in_gippsland
    from {{ ref('fct_earthquake') }}
    where magnitude is not null
      and coalesce(event_type, 'earthquake') = 'earthquake'
),
series as (
    select window_start
    from generate_series(
        timestamp '2006-01-08',
        (select asof_time from span),
        interval 7 day
    ) as t(window_start)
),
scopes as (
    select * from (
        values
            ('south_gippsland_7d', 'south_gippsland', 7),
            ('south_gippsland_30d', 'south_gippsland', 30),
            ('victoria_7d', 'victoria', 7),
            ('victoria_30d', 'victoria', 30),
            ('australia_7d', 'australia', 7),
            ('australia_30d', 'australia', 30)
    ) as v(scope, region, window_days)
),
fixed_counts as (
    select
        s.scope,
        s.region,
        s.window_days,
        w.window_start,
        count(b.event_id) as n
    from scopes as s
    cross join series as w
    left join base as b
        on b.magnitude >= {{ var('mag_floor') }}
       and b.origin_time >= w.window_start
       and b.origin_time < w.window_start + (s.window_days * interval 1 day)
       and (
            (s.region = 'south_gippsland' and b.in_gippsland)
            or (s.region = 'victoria' and b.in_victoria)
            or s.region = 'australia'
       )
    cross join span
    where w.window_start + (s.window_days * interval 1 day) <= span.asof_time - (s.window_days * interval 1 day)
    group by 1, 2, 3, 4
),
fixed_baseline as (
    select
        scope,
        region,
        window_days,
        count(*) as baseline_windows,
        median(n) as baseline_median,
        quantile_cont(n, 0.95) as baseline_p95,
        quantile_cont(n, 0.99) as baseline_p99,
        avg(n) as baseline_mean
    from fixed_counts
    group by 1, 2, 3
),
fixed_current as (
    select
        s.scope,
        count(b.event_id) filter (where b.magnitude >= {{ var('mag_floor') }}) as event_count_m2,
        count(b.event_id) as event_count_all,
        max(b.magnitude) as max_magnitude,
        median(b.depth_km) filter (where b.depth_km > 0 and b.magnitude >= {{ var('mag_floor') }}) as median_depth_km
    from scopes as s
    cross join span
    left join base as b
        on b.origin_time > span.asof_time - (s.window_days * interval 1 day)
       and b.origin_time <= span.asof_time
       and (
            (s.region = 'south_gippsland' and b.in_gippsland)
            or (s.region = 'victoria' and b.in_victoria)
            or s.region = 'australia'
       )
    group by 1
),
fixed_rows as (
    select
        c.scope,
        b.region,
        b.window_days,
        span.asof_time - (b.window_days * interval 1 day) as window_start,
        span.asof_time as window_end,
        c.event_count_m2,
        c.event_count_all,
        c.max_magnitude,
        c.median_depth_km,
        b.baseline_windows,
        b.baseline_median,
        b.baseline_p95,
        b.baseline_p99,
        b.baseline_mean,
        case
            when b.baseline_median = 0 then null
            else c.event_count_m2 / b.baseline_median
        end as rate_ratio,
        case
            when b.baseline_windows < 20 then 'insufficient_baseline'
            when c.event_count_m2 >= b.baseline_p99 and c.event_count_m2 > b.baseline_median then 'unusual'
            when c.event_count_m2 >= b.baseline_p95 and c.event_count_m2 > b.baseline_median then 'elevated'
            else 'typical'
        end as status
    from fixed_current as c
    inner join fixed_baseline as b on c.scope = b.scope
    cross join span
),
swarm_series as (
    select window_start
    from series
    cross join span
    where window_start + (span.asof_time - span.swarm_start) <= span.swarm_start
),
swarm_counts as (
    select
        w.window_start,
        count(b.event_id) as n
    from swarm_series as w
    cross join span
    left join base as b
        on b.in_gippsland
       and b.magnitude >= {{ var('mag_floor') }}
       and b.origin_time >= w.window_start
       and b.origin_time < w.window_start + (span.asof_time - span.swarm_start)
    group by 1
),
swarm_baseline as (
    select
        count(*) as baseline_windows,
        median(n) as baseline_median,
        quantile_cont(n, 0.95) as baseline_p95,
        quantile_cont(n, 0.99) as baseline_p99,
        avg(n) as baseline_mean
    from swarm_counts
),
swarm_current as (
    select
        count(*) filter (where magnitude >= {{ var('mag_floor') }}) as event_count_m2,
        count(*) as event_count_all,
        max(magnitude) as max_magnitude,
        median(depth_km) filter (where depth_km > 0) as median_depth_km,
        count(distinct cluster_id) filter (where cluster_id >= 0) as n_clusters
    from base
    cross join span
    where in_gippsland
      and origin_time >= span.swarm_start
      and origin_time <= span.asof_time
),
swarm_ml as (
    select magnitude
    from base
    cross join span
    where in_gippsland
      and origin_time >= span.swarm_start
      and origin_time <= span.asof_time
      and mag_type ilike 'ml%'
),
mc_bin as (
    select mag_bin
    from (
        select
            floor(magnitude / {{ var('mag_bin') }}) * {{ var('mag_bin') }} as mag_bin,
            count(*) as n
        from swarm_ml
        group by 1
        order by n desc, mag_bin
        limit 1
    )
),
b_value as (
    select
        (select mag_bin + {{ var('mc_correction') }} from mc_bin) as mc,
        count(*) as b_n,
        case
            when count(*) < 2 then null
            when avg(magnitude) - ((select mag_bin + {{ var('mc_correction') }} from mc_bin) - {{ var('mag_bin') }} / 2) <= 0 then null
            else log10(exp(1)) / (
                avg(magnitude) - ((select mag_bin + {{ var('mc_correction') }} from mc_bin) - {{ var('mag_bin') }} / 2)
            )
        end as b_value
    from swarm_ml
    where magnitude >= (select mag_bin + {{ var('mc_correction') }} from mc_bin)
),
swarm_row as (
    select
        'south_gippsland_swarm' as scope,
        'south_gippsland' as region,
        date_diff('day', span.swarm_start, span.asof_time) as window_days,
        span.swarm_start as window_start,
        span.asof_time as window_end,
        c.event_count_m2,
        c.event_count_all,
        c.max_magnitude,
        c.median_depth_km,
        bl.baseline_windows,
        bl.baseline_median,
        bl.baseline_p95,
        bl.baseline_p99,
        bl.baseline_mean,
        case when bl.baseline_median = 0 then null else c.event_count_m2 / bl.baseline_median end as rate_ratio,
        case
            when bl.baseline_windows < 20 then 'insufficient_baseline'
            when c.event_count_m2 >= bl.baseline_p99 and c.event_count_m2 > bl.baseline_median then 'unusual'
            when c.event_count_m2 >= bl.baseline_p95 and c.event_count_m2 > bl.baseline_median then 'elevated'
            else 'typical'
        end as status,
        c.n_clusters,
        bv.mc,
        bv.b_value,
        bv.b_n,
        bv.b_n >= 50 as b_stable
    from swarm_current as c
    cross join swarm_baseline as bl
    cross join span
    cross join b_value as bv
)
select
    scope,
    region,
    window_days,
    window_start,
    window_end,
    event_count_m2,
    event_count_all,
    max_magnitude,
    median_depth_km,
    baseline_windows,
    baseline_median,
    baseline_p95,
    baseline_p99,
    baseline_mean,
    rate_ratio,
    status,
    null::bigint as n_clusters,
    null::double as mc,
    null::double as b_value,
    null::bigint as b_n,
    null::boolean as b_stable
from fixed_rows
union all
select
    scope,
    region,
    window_days,
    window_start,
    window_end,
    event_count_m2,
    event_count_all,
    max_magnitude,
    median_depth_km,
    baseline_windows,
    baseline_median,
    baseline_p95,
    baseline_p99,
    baseline_mean,
    rate_ratio,
    status,
    n_clusters,
    mc,
    b_value,
    b_n,
    b_stable
from swarm_row
