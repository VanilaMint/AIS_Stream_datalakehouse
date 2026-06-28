{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT CAST(d AS DATE) AS full_date
    FROM UNNEST(SEQUENCE(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1' DAY)) AS t(d)
)

SELECT
    CAST(date_format(full_date, '%Y%m%d') AS BIGINT) AS date_id,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    CASE 
        WHEN day_of_week(full_date) IN (6, 7) THEN true 
        ELSE false 
    END AS is_weekend
FROM date_spine