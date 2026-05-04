{{ config(materialized='view') }}

WITH base AS (
    {{ staging_model('interviews') }}
)

SELECT
    *
FROM base
WHERE UPPER(status) IN ('SCHEDULED', 'IN_PROGRESS', 'PENDING_FEEDBACK', 'COMPLETED', 'CANCELLED', 'REQUESTED', 'DRAFT')
-- We remove technical duplicates (same ID + same time), preserving history
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY interview_id, updated_at
    ORDER BY updated_at
) = 1
