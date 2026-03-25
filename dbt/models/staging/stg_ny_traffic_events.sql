{{
  config(
    materialized='view'
  )
}}

with eventdata as (
  select *,
    row_number() over(partition by RoadwayName, ingestion_time, EventType) as rn
  from {{ source('raw', 'ny_traffic_events') }}
  where RoadwayName is not null
)

select
  -- Identifier
  {{ dbt_utils.generate_surrogate_key(['ID', 'ingestion_time', 'EventType']) }} as event_id,
  ID,

  -- Source information
  SourceId,
  Organization,

  -- Location information
  Latitude,
  Longitude,
  LatitudeSecondary,
  LongitudeSecondary,

  -- Road Imformation
  RoadwayName,
  DirectionOfTravel,
  County,
  State,
  lanesAffected,
  IsFullClosure,
  LastUpdated,
  EventType,
  EventSubType,
  Severity,

  -- UNIX timestamps converted to DATETIME
  {{ dbt_date.from_unixtimestamp('Reported') }} as Reported,
  {{ dbt_date.from_unixtimestamp('StartDate') }} as StartDate,
  {{ dbt_date.from_unixtimestamp('PlannedEndDate') }} as PlannedEndDate,
  
  -- Processing time-related
  ingestion_time,
  processing_time
from eventdata
where rn = 1

-- Remove default limit
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}
  limit 100
{% endif %}