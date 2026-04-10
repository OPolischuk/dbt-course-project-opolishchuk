{{ config(
    materialized='table',
    schema='mart'
) }}

WITH pivoted AS (
    SELECT * FROM {{ ref('int_interviews_pivoted') }}
),

-- all history for candidate
candidates_history AS (
    SELECT * FROM {{ ref('stg_candidates') }}
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
        stg.created_at AS created_datetime,
        calc.requested_datetime,
        calc.scheduled_datetime,
        calc.started_datetime AS in_progress_datetime,
        calc.feedback_provided_datetime AS pending_feedback_datetime,
        calc.finished_datetime AS completed_datetime,
        calc.cancelled_datetime,
        cand.english_level,
        cand.staffing_status,
        jobs.job_function_name AS base_name,
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

    -- Join with all candidate's history
    LEFT JOIN candidates_history AS cand
        ON
            calc.candidate_id = cand.candidate_id
            AND stg.created_at >= cand.valid_from_datetime
            AND stg.created_at
            < COALESCE(cand.valid_to_datetime, CAST('9999-12-31' AS TIMESTAMP))

    LEFT JOIN {{ ref('stg_job_functions') }} AS jobs
        ON
            cand.job_function_id = jobs.job_function_id
            AND stg.created_at >= jobs.valid_from_datetime
            AND stg.created_at
            < COALESCE(jobs.valid_to_datetime, CAST('9999-12-31' AS TIMESTAMP))

    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY calc.id ORDER BY stg.updated_at DESC)
        = 1
)

SELECT * FROM final
