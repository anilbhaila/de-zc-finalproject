{{
  config(
    materialized='table'
  )
}}

SELECT
  EventType,
  COALESCE(RoadwayName, 'Not Specified') AS RoadwayName,
  COUNT(*) AS data_points
FROM 
  {{ ref('stg_ny_traffic_events') }}
GROUP BY
  EventType,
  COALESCE(RoadwayName, 'Not Specified')
  