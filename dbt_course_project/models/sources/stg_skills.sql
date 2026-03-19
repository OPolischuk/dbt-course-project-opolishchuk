WITH skills AS (
    SELECT * FROM {{ source('snowflake_raw', 'skills') }}
),

final AS (
    SELECT * FROM skills
)

SELECT * FROM final
