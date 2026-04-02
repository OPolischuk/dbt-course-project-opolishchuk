{{ config(materialized='view') }}

SELECT
    offset AS _offset,
    candidate_id AS id,
    primary_skill_id,
    staffing_status,
    english_level,
    job_function_id,
    valid_from_datetime,
    valid_to_datetime
FROM {{ ref('stg_candidates') }}
{{ latest_staging_model() }}
