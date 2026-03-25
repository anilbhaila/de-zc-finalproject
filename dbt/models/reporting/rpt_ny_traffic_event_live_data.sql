{{
  config(
    materialized='table'
  )
}}

WITH latest_data AS (
  SELECT
    ID,
    SourceId,
    Organization,
    Latitude,
    Longitude,
    LatitudeSecondary,
    LongitudeSecondary,
    RoadwayName,
    DirectionOfTravel,
    County,
    State,
    lanesAffected,
    IsFullClosure,
    EventType,
    EventSubType,
    Severity,
    Reported,
    LastUpdated,
    StartDate,
    PlannedEndDate,
    ingestion_time,
    processing_time,
    ROW_NUMBER() OVER(PARTITION BY ID, EventType ORDER BY ingestion_time DESC) AS rn
  FROM {{ ref('stg_ny_traffic_events') }}
),

aggregated_data AS (
  SELECT
    ID,
    COALESCE(RoadwayName, 'Not Specified') AS RoadwayName,
    Latitude,
    Longitude,
    EventType,
    COUNT(*) AS data_points,
    MAX(LastUpdated) AS max_date
  FROM {{ ref('stg_ny_traffic_events') }}
  GROUP BY
    ID,
    COALESCE(RoadwayName, 'Not Specified'),
    Latitude,
    Longitude,
    EventType
)

SELECT
  a.*,
  l.LastUpdated AS current_date
FROM
  aggregated_data a
LEFT JOIN
  latest_data l
ON
  a.ID = l.ID
  AND a.EventType = l.EventType
  AND l.rn = 1