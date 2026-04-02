{{ config(materialized='view') }}

SELECT
    job_function_id AS id,
    job_function_name AS base_name,
    category,
    is_active,
    level,
    track,
    seniority_level,
    seniority_index
FROM {{ ref('stg_job_functions') }}
{{ latest_staging_model() }}
