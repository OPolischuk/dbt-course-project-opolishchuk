WITH employees AS (
    SELECT * FROM {{ source('snowflake_raw', 'employees') }}
),

final AS (
    SELECT * FROM employees
)

SELECT * FROM final
