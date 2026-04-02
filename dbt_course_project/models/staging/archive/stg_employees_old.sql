{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('snowflake_raw', 'employees') }}
),

transformed AS (
    SELECT
        TRY_TO_NUMBER(TRIM(REPLACE(_offset, '"', ''))) AS "OFFSET",
        TRIM(REPLACE(employee_id, '"', ''))::VARCHAR AS id,
        TRIM(REPLACE(job_function_id, '"', ''))::VARCHAR AS job_function_id,
        TRIM(REPLACE(primary_skill_id, '"', ''))::VARCHAR AS primary_skill_id,
        production_category::VARCHAR AS production_category,
        employment_status::VARCHAR AS employment_status,
        org_category::VARCHAR AS org_category,
        org_category_type::VARCHAR AS org_category_type,
        TO_DATE(TO_TIMESTAMP(TRY_TO_NUMBER(TRIM(work_start_micros))::BIGINT, 6))
            AS work_start_date,
        TO_DATE(TO_TIMESTAMP(TRY_TO_NUMBER(TRIM(work_end_micros))::BIGINT, 6))
            AS work_end_date,
        is_active::BOOLEAN AS is_active,
        TO_TIMESTAMP(TRY_TO_NUMBER(TRIM(_created_micros))::BIGINT, 6)
            AS created_at,
        TO_TIMESTAMP(TRY_TO_NUMBER(TRIM(_updated_micros))::BIGINT, 6)
            AS updated_at
    FROM source

    {% if is_incremental() %}
    WHERE
        TO_TIMESTAMP(TRY_TO_NUMBER(TRIM(_updated_micros)) / 1000000)
        > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

latest_state AS (
    SELECT *
    FROM transformed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT *
FROM latest_state
