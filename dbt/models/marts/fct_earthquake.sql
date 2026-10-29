{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

-- Canonical earthquake fact. Incremental on ingested_at so a refresh that
-- rewrites the silver file with a newer ingest timestamp upserts revisions
-- without reprocessing the 2006-2026 backfill.
select *
from {{ ref('stg_earthquakes') }}
{% if is_incremental() %}
where ingested_at > (select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this }})
{% endif %}
