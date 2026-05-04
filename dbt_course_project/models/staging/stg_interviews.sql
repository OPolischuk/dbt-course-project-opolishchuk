{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('interviews') }}
)

SELECT * FROM base
WHERE UPPER(status) IN ('SCHEDULED', 'IN_PROGRESS', 'PENDING_FEEDBACK', 'COMPLETED', 'CANCELLED', 'REQUESTED', 'DRAFT')
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY _updated_at DESC
) = 1
