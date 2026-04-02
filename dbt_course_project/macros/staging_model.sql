{% macro staging_model(table_name) %}

{% set column_query %}
    SELECT raw_column_name, target_column_name, target_data_type
    FROM {{ ref('columns_mapping') }}
    WHERE raw_table_name = '{{ table_name }}'
    ORDER BY target_order_num
{% endset %}

{% set results = run_query(column_query) %}

{% if execute and results %}
    {% set columns = results.rows %}
    {% set primary_key = columns[1][1] %}
{% else %}
    {% set columns = [] %}
    {% set primary_key = 'id' %}
{% endif %}

WITH source_data AS (
    SELECT * FROM {{ source('snowflake_raw', table_name) }}
),

renamed AS (
    SELECT
        {% if columns|length > 0 %}
            {% for col in columns %}
                {% if col[2] == 'timestamp' %}
                    TO_TIMESTAMP(TRY_TO_NUMBER(TRIM({{ col[0] }}))::BIGINT, 6) AS {{ col[1] }}
                {% elif col[2] == 'boolean' %}
                    {{ col[0] }}::VARCHAR AS {{ col[1] }}
                {% elif 'id' in col[1]|lower or col[2] == 'bigint' %}
                    {{ col[0] }}::VARCHAR AS {{ col[1] }}
                {% else %}
                    {{ col[0] }}::{{ col[2] }} AS {{ col[1] }}
                {% endif %}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        {% else %}
            *
        {% endif %}
    FROM source_data
),

historical AS (
    SELECT
        *,
        {% if columns|length > 0 %}
            updated_at AS valid_from_datetime,
            LEAD(updated_at) OVER (PARTITION BY {{ primary_key }} ORDER BY updated_at ASC) AS next_row_ts
        {% else %}
            CURRENT_TIMESTAMP() AS valid_from_datetime,
            CAST(NULL AS TIMESTAMP_NTZ) AS next_row_ts
        {% endif %}
    FROM renamed
),

final AS (
    SELECT
        * EXCLUDE (next_row_ts),
        COALESCE(next_row_ts, '9558-06-13 13:43:59'::TIMESTAMP_NTZ) AS valid_to_datetime,
        CASE WHEN next_row_ts IS NULL THEN 1 ELSE 0 END AS ROW_IS_ACTIVE
    FROM historical
)

SELECT * FROM final
{% endmacro %}
