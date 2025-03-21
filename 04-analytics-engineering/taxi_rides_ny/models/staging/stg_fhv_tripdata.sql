{{ config(materialized='view') }}

SELECT 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
FROM {{ source('staging', 'fhv_tripdata') }}  -- Replace with actual source table
WHERE dispatching_base_num IS NOT NULL 