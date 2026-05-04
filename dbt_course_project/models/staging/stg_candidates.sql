{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('candidates') }}
)

-- in this model the duplicates are absent
SELECT * FROM base
