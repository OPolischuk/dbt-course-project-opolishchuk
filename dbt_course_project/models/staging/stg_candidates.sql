{{ config(materialization='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_raw', 'candidates') }}
),

renamed AS (
    SELECT
        _offset::BIGINT AS "OFFSET",
        candidate_id::VARCHAR AS id,
        primary_skill_id::VARCHAR AS primary_skill_id,
        staffing_status::VARCHAR AS staffing_status,
        english_level::VARCHAR AS english_level,
        job_function_id::VARCHAR AS job_function_id,
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
