select
    event_id,
    ga_id,
    usgs_id,
    sources,
    try_cast(origin_time as timestamp) as origin_time,
    try_cast(updated_time as timestamp) as updated_time,
    try_cast(created_time as timestamp) as created_time,
    version,
    latitude,
    longitude,
    depth_km,
    magnitude,
    mag_type,
    magnitude_usgs,
    place,
    status,
    event_type,
    felt_reports,
    state_name,
    h3_res4,
    h3_res5,
    h3_res6,
    in_gippsland_box,
    publication_lag_seconds,
    lag_kind,
    match_distance_km,
    match_time_delta_s,
    arrival_status,
    cluster_id,
    try_cast(ingested_at as timestamp) as ingested_at,
    url,
    cast(try_cast(origin_time as timestamp) as date) as event_date
from {{ source('silver', 'quakes') }}
where event_id is not null
  and origin_time is not null
  and magnitude is not null
qualify row_number() over (partition by event_id order by state_name nulls last) = 1
