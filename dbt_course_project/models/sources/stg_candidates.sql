WITH candidates AS (
    SELECT * FROM {{ source('snowflake_raw', 'candidates') }}
),

final AS (
    SELECT * FROM candidates
)

SELECT * FROM final
