{{ config(materialized='view') }}

SELECT
    offset AS _offset,
    employee_id AS id,
    job_function_id,
    primary_skill_id,
    production_category,
    employment_status,
    org_category,
    org_category_type,
    CAST(work_start_at AS DATE) AS work_start_date,
    CAST(work_end_at AS DATE) AS work_end_date,
    is_active,
    valid_from_datetime,
    valid_to_datetime
FROM {{ ref('stg_employees') }}
{{ latest_staging_model() }}
