{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('job_functions') }}
)

SELECT
    *
FROM base
WHERE
    -- only 19 symbols are passed
    LENGTH(job_function_id) = 19
-- deduplication - removed technical duplicates (same ID + same time), preserving history
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY job_function_id, updated_at
    ORDER BY updated_at DESC
) = 1
