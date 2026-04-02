{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('skills') }}
)

SELECT * FROM base
