{{ config(materialized='view') }}

SELECT
    skill_id AS id,
    is_active,
    skill_type AS type,
    skill_name AS name,
    url,
    parent_skill_id AS parent_id
FROM {{ ref('stg_skills') }}
{{ latest_staging_model() }}
