{{ config(materialization='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_raw', 'interviews') }}
),

renamed AS (
    SELECT
        _offset::BIGINT AS "OFFSET",
        id::VARCHAR AS id,
        candidate_type::VARCHAR AS candidate_type,
        candidate_id::VARCHAR AS candidate_id,
        status::VARCHAR AS status,
        interviewer_id::VARCHAR AS interviewer_id,
        location::VARCHAR AS location,
        -- change the VARCHAR instead of BOOLEAN to upload data in Snowflake
        logged::VARCHAR AS is_logged,
        media_available::VARCHAR AS is_media_available,
        run_type::VARCHAR AS run_type,
        "TYPE"::VARCHAR AS "TYPE",
        media_status::VARCHAR AS media_status,
        invite_answer_status::VARCHAR AS invite_answer_status,
        TO_TIMESTAMP(TRY_TO_NUMBER(_created_micros)::BIGINT, 6) AS created_at,
        TO_TIMESTAMP(TRY_TO_NUMBER(_updated_micros)::BIGINT, 6) AS updated_at
    FROM source
),

latest_state AS (
    SELECT *
    FROM renamed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT *
FROM latest_state
