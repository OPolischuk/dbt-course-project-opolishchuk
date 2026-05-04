{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('candidates') }}
)

SELECT * FROM base
