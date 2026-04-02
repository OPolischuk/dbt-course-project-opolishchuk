SELECT
    interview_id AS id,
    candidate_id,
    MAX(CASE WHEN UPPER(status) = 'REQUESTED' THEN valid_from_datetime END) AS requested_datetime,
    MAX(CASE WHEN UPPER(status) = 'SCHEDULED' THEN valid_from_datetime END) AS scheduled_datetime,
    MAX(CASE WHEN UPPER(status) = 'IN_PROGRESS' THEN valid_from_datetime END) AS started_datetime,
    MAX(CASE WHEN UPPER(status) = 'PENDING_FEEDBACK' THEN valid_from_datetime END) AS feedback_provided_datetime,
    MAX(CASE WHEN UPPER(status) = 'COMPLETED' THEN valid_from_datetime END) AS finished_datetime,
    MAX(CASE WHEN UPPER(status) = 'CANCELLED' THEN valid_from_datetime END) AS cancelled_datetime
FROM {{ ref('stg_interviews') }}
GROUP BY 1, 2
