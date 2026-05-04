{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('candidates') }}
)

SELECT * FROM base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY candidate_id
    ORDER BY _updated_at DESC
) = 1
