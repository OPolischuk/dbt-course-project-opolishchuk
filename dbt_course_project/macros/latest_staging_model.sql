-- macro to get the current status

{% macro latest_staging_model() %}
    WHERE ROW_IS_ACTIVE = 1
{% endmacro %}
