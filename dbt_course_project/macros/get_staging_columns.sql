-- To keep the main macro from being too cumbersome, was created a macro that retrieves
-- the column settings for a specific table

{% macro get_staging_columns(table_name) %}
    {% set query %}
        SELECT raw_column_name, target_column_name, target_data_type
        FROM {{ ref('columns_mapping') }}
        WHERE raw_table_name = '{{ table_name }}'
        ORDER BY target_order_num
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}
{% endmacro %}
