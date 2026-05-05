{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    date_day AS date,
    FALSE AS is_holiday,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(WEEK FROM date_day) AS week,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Month') AS month_name,
    COALESCE (EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6), FALSE) AS is_weekend
FROM date_spine
