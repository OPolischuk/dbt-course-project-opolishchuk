{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_raw', 'skills') }}
),

renamed AS (
    SELECT
        _offset::BIGINT AS "OFFSET",
        id::VARCHAR AS id,
        is_active::BOOLEAN AS is_active,
        is_primary::BOOLEAN AS is_primary,
        is_key::BOOLEAN AS is_key,
        -- change the VARCHAR instaed of BOOLEAN to upload data into Snowflake
        is_key_reason::VARCHAR AS is_key_reason,
        "TYPE"::VARCHAR AS "TYPE",
        name::VARCHAR AS name,
        url::VARCHAR AS url,
        parent_id::VARCHAR AS parent_id,
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
