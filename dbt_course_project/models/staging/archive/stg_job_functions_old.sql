{{ config(materialization='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_raw', 'job_functions') }}
),

renamed AS (
    SELECT
        _offset::BIGINT AS "OFFSET",
        job_function_id::VARCHAR AS id,
        base_name::VARCHAR AS base_name,
        category::VARCHAR AS category,
        is_active::BOOLEAN AS is_active,
        level::INT AS "LEVEL",
        track::VARCHAR AS track,
        seniority_level::VARCHAR AS seniority_level,
        seniority_index::INT AS seniority_index,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(_created_micros) / 1000000) AS created_at,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(_updated_micros) / 1000000) AS updated_at
    FROM source
),

latest_state AS (
    SELECT *
    FROM renamed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT *
FROM latest_state
