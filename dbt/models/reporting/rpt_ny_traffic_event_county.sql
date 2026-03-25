{{
  config(
    materialized='table'
  )
}}

SELECT
  EventType,
  COALESCE(County, 'Not Specified') AS County,
  COUNT(*) AS data_points
FROM 
  {{ ref('stg_ny_traffic_events') }}
GROUP BY
  EventType,
  COALESCE(County, 'Not Specified')
  