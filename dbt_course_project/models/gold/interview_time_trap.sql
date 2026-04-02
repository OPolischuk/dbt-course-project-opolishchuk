{% set traps = var('time_traps', []) %}

{% if traps %}
    WITH events AS (
        SELECT
            candidate_id,
            interview_id,
            valid_from_datetime AS event_ts,
            UPPER(TRIM(status)) AS status
        FROM {{ ref('stg_interviews') }}
    )

    {% for trap in traps %}
        SELECT
            s.candidate_id,
            '{{ trap.name }}' AS interview_time_trap_name,
            DATEDIFF('minute', s.event_ts, e.event_ts) AS trap_duration,
            s.event_ts AS trap_start_datetime,
            e.event_ts AS trap_end_datetime,
            s.interview_id AS trap_start_interview_id
        FROM events AS s
        INNER JOIN events AS e
            ON
                s.candidate_id = e.candidate_id
                AND s.interview_id = e.interview_id
        WHERE
            s.status = UPPER(TRIM('{{ trap.start_status }}'))
            AND e.status = UPPER(TRIM('{{ trap.end_status }}'))
            AND e.event_ts >= s.event_ts

        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
{% else %}
    SELECT
        NULL::VARCHAR AS candidate_id,
        NULL::VARCHAR AS interview_time_trap_name,
        NULL::FLOAT AS trap_duration,
        NULL::DATETIME AS trap_start_datetime,
        NULL::DATETIME AS trap_end_datetime,
        NULL::VARCHAR AS trap_start_interview_id
    WHERE 1=0
{% endif %}
