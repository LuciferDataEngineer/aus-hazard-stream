-- Incremental magnitude counts for Gutenberg-Richter plots.
-- Only ML-family magnitudes are binned together. USGS mb is a different scale.
with events as (
    select
        case
            when in_gippsland_box and origin_time >= timestamp '{{ var("swarm_start") }}'
                then 'south_gippsland_swarm'
            when in_gippsland_box and origin_time < timestamp '{{ var("swarm_start") }}'
                then 'south_gippsland_baseline'
            else null
        end as scope,
        magnitude
    from {{ ref('fct_earthquake') }}
    where mag_type ilike 'ml%'
      and in_gippsland_box
      and magnitude is not null
)
select
    scope,
    round(floor(magnitude / {{ var('mag_bin') }}) * {{ var('mag_bin') }}, 1) as mag_bin,
    count(*) as n,
    log10(count(*)) as log10_n
from events
where scope is not null
group by 1, 2
