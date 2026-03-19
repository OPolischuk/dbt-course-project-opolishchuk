WITH interviews AS (
    SELECT * FROM {{ source('snowflake_raw', 'interviews') }}
),

final AS (
    SELECT * FROM interviews
)

SELECT * FROM final
