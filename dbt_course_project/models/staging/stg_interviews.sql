{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('interviews') }}
)

SELECT * FROM base
