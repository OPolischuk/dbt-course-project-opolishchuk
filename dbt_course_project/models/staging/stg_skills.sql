{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('skills') }}
)

SELECT * FROM base
WHERE length(skill_id) = 19 -- cut off incorrect IDs
