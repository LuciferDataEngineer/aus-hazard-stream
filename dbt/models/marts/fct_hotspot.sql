{{ config(
    materialized='incremental',
    unique_key='hotspot_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

select *
from {{ ref('stg_hotspots') }}
{% if is_incremental() %}
where ingested_at > (select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this }})
{% endif %}
