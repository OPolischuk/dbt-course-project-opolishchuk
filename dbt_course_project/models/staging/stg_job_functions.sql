{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('job_functions') }}
)

SELECT * FROM base
WHERE length(job_function_id) = 19
