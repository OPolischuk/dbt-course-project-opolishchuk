WITH job_functions AS (
    SELECT * FROM {{ source('snowflake_raw', 'job_functions') }}
),

final AS (
    SELECT * FROM job_functions
)

SELECT * FROM final
