{{
  config(
    materialized='table'
  )
}}

SELECT
  {{ dbt_date.convert_timezone("ingestion_time", target_tz="America/Chicago") }} as DataRefreshedDate,
  EventType,
  COALESCE(County, 'Not Specified') AS County,
  COUNT(*) AS data_points
FROM 
  {{ ref('stg_ny_traffic_events') }}
GROUP BY
  DataRefreshedDate,
  EventType,
  COALESCE(County, 'Not Specified')
  
ORDER BY
  DataRefreshedDate,
  data_points DESC
  