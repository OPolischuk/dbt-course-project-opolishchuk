{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('job_functions') }}
)

SELECT * FROM base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY job_function_id, updated_at
    ORDER BY updated_at DESC
) = 1
