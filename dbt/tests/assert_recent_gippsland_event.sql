-- The October 2026 Leongatha sequence should survive cleaning.
-- Fails (returns a row) when the M3.5-class event is missing.
select 'missing recent south gippsland event' as failure
where (
    select count(*)
    from {{ ref('fct_earthquake') }}
    where in_gippsland_box
      and origin_time >= timestamp '2026-10-27'
      and magnitude >= 3
) = 0
