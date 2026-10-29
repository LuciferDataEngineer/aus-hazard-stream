-- State and territory polygons from Natural Earth 50m admin-1, simplified
-- to about 0.05 degrees. Not the ABS ASGS; good enough to label events and
-- too small to commit a full statistical-area shapefile.
select
    state_name as region_name,
    state_code,
    case state_code
        when 'AU-NSW' then 'NSW'
        when 'AU-VIC' then 'VIC'
        when 'AU-QLD' then 'QLD'
        when 'AU-SA' then 'SA'
        when 'AU-WA' then 'WA'
        when 'AU-TAS' then 'TAS'
        when 'AU-NT' then 'NT'
        when 'AU-ACT' then 'ACT'
        else state_code
    end as state_abbr,
    'natural_earth_50m_admin1' as boundary_source
from st_read('{{ env_var("AUS_HAZARD_ROOT") }}/data/reference/au_states.geojson')
