{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('employees') }}
)

SELECT * FROM base
