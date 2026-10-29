select
    hotspot_id,
    try_cast(observed_at as timestamp) as observed_at,
    latitude,
    longitude,
    temp_kelvin,
    power,
    confidence,
    case trim(state_name)
        when 'NSW' then 'New South Wales'
        when 'VIC' then 'Victoria'
        when 'QLD' then 'Queensland'
        when 'SA' then 'South Australia'
        when 'WA' then 'Western Australia'
        when 'TAS' then 'Tasmania'
        when 'NT' then 'Northern Territory'
        when 'ACT' then 'Australian Capital Territory'
        else nullif(trim(state_name), '')
    end as state_name,
    satellite,
    sensor,
    algorithm,
    try_cast(processed_at as timestamp) as processed_at,
    try_cast(loaded_at as timestamp) as loaded_at,
    case
        when try_cast(processed_at as timestamp) >= try_cast(observed_at as timestamp)
            then date_diff('second', try_cast(observed_at as timestamp), try_cast(processed_at as timestamp))
        else null
    end as processing_lag_seconds,
    h3_res4,
    h3_res5,
    h3_res6,
    nearest_place,
    nearest_place_pop,
    nearest_place_km,
    nearest_airport,
    nearest_airport_ident,
    nearest_airport_km,
    near_population,
    near_airport,
    try_cast(ingested_at as timestamp) as ingested_at,
    version,
    cast(try_cast(observed_at as timestamp) as date) as event_date
from {{ source('silver', 'hotspots') }}
where hotspot_id is not null
  and observed_at is not null
qualify row_number() over (partition by hotspot_id order by state_name nulls last) = 1
