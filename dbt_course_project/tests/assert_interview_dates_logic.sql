-- This test will find records where the end date is less than the start date
SELECT
    interview_id,
    created_at,
    updated_at
FROM {{ ref('stg_interviews') }}
WHERE updated_at < created_at
