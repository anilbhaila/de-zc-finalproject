{{
    config(
        materialized='table'
    )
}}

WITH categorized_traffic_events AS (
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
        ROW_NUMBER() OVER (PARTITION BY ID, EventType ORDER BY ingestion_time DESC) AS rn
    FROM {{ ref('stg_ny_traffic_events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'EventType']) }} as traffic_event_key,
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
    processing_time
FROM categorized_traffic_events
WHERE rn = 1