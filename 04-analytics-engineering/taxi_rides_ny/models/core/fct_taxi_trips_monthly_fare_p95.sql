{{ config(materialized='table') }}

WITH filtered_trips AS (
    -- Step 1: Filter valid trips
    SELECT 
        *,
        -- EXTRACT(YEAR FROM pickup_datetime) AS year,
        -- EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM {{ ref('fact_trips') }}  -- Replace with your actual source table name
    WHERE 
        fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit card')
),

percentile_cte AS (
    -- Step 2: Compute percentiles (p97, p95, p90)
    SELECT 
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(90)] AS p90
    FROM filtered_trips
    GROUP BY service_type, year, month
)

SELECT * FROM percentile_cte