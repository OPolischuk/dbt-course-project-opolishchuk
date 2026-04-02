{{ config(
    materialized='table',
    schema='mart'
) }}

WITH pivoted AS (
    SELECT * FROM {{ ref('int_interviews_pivoted') }}
),

final AS (
    SELECT
        calc.id,
        stg.candidate_type,
        calc.candidate_id AS candidate_offset,
        stg.status,
        stg.interviewer_id AS interviewer_offset,
        stg.location,
        stg.is_logged,
        stg.is_media_available,
        stg.run_type,
        stg.interview_type AS type,
        stg.media_status,



        -- pivot
        stg.created_at AS created_datetime,
        calc.requested_datetime,
        calc.scheduled_datetime,
        calc.started_datetime AS in_progress_datetime,
        calc.feedback_provided_datetime AS pending_feedback_datetime,
        calc.finished_datetime AS completed_datetime,
        calc.cancelled_datetime,
        cand.english_level,

        -- 1. interview_duration: IN_PROGRESS -> PENDING_FEEDBACK (online only)
        cand.staffing_status,

        -- 2. feedback_delay: PENDING_FEEDBACK -> COMPLETED
        jobs.base_name,

        -- (Silver)
        jobs.category,
        CAST(stg.created_at AS DATE) AS created_date,
        GREATEST(
            CASE
                WHEN LOWER(stg.run_type) = 'online'
                    THEN
                        DATEDIFF(
                            'minute',
                            calc.started_datetime,
                            calc.feedback_provided_datetime
                        )
                ELSE 0
            END,
            0
        ) AS interview_duration,
        GREATEST(
            DATEDIFF(
                'minute',
                calc.feedback_provided_datetime,
                calc.finished_datetime
            ),
            0
        ) AS feedback_delay

    FROM pivoted AS calc
    LEFT JOIN {{ ref('stg_interviews') }} AS stg
        ON calc.id = stg.interview_id

    LEFT JOIN {{ ref('dim_candidates') }} AS cand
        ON calc.candidate_id = cand.id

    LEFT JOIN {{ ref('dim_job_functions') }} AS jobs
        ON cand.job_function_id = jobs.id

    -- last record of staging
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY calc.id ORDER BY stg.updated_at DESC)
        = 1
)

SELECT * FROM final
