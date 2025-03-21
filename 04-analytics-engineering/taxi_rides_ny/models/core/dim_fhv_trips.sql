{{ config(materialized='table') }}

WITH base_trips AS (
    -- Get data from the staging model
    SELECT
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number,
        
        -- Extract year and month from pickup_datetime
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM {{ ref('stg_fhv_tripdata') }}  -- Use the staging model
),

joined_zones AS (
    -- Join with the zones table to get location names
    SELECT 
        bt.*,
        pu_zone.Borough AS pickup_borough,
        pu_zone.Zone AS pickup_zone,
        do_zone.Borough AS dropoff_borough,
        do_zone.Zone AS dropoff_zone
    FROM base_trips bt
    LEFT JOIN {{ ref('dim_zones') }} pu_zone 
        ON bt.PUlocationID = pu_zone.LocationID
    LEFT JOIN {{ ref('dim_zones') }} do_zone 
        ON bt.DOlocationID = do_zone.LocationID
)

SELECT * FROM joined_zones