{{ config(materialized='table') }}

WITH trip_durations AS (
    -- Compute trip duration in seconds
    SELECT 
        year,
        month,
        PUlocationID AS pickup_location_id,
        DOlocationID AS dropoff_location_id,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL
),

p90_trip_durations AS (
    -- Compute p90 of trip_duration partitioned by year, month, pickup & dropoff locations
    SELECT 
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        APPROX_QUANTILES(trip_duration, 100)[SAFE_OFFSET(90)] AS p90_trip_duration
    FROM trip_durations
    GROUP BY year, month, pickup_location_id, dropoff_location_id
)

SELECT * FROM p90_trip_durations