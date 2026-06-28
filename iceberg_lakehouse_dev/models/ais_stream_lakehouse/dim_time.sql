{{ config(materialized='table') }}

WITH hours AS (SELECT * FROM UNNEST(SEQUENCE(0, 23)) AS t(h)),
     minutes AS (SELECT * FROM UNNEST(SEQUENCE(0, 59)) AS t(m)),
     seconds AS (SELECT * FROM UNNEST(SEQUENCE(0, 59)) AS t(s))

SELECT
    CAST(
        lpad(CAST(h AS VARCHAR), 2, '0') ||
        lpad(CAST(m AS VARCHAR), 2, '0') ||
        lpad(CAST(s AS VARCHAR), 2, '0') AS BIGINT
    ) AS time_id,
    h AS hour,
    m AS minute,
    s AS second,
    CASE
        WHEN h BETWEEN 6 AND 11 THEN 'Morning'
        WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN h BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day_shift
FROM hours
CROSS JOIN minutes
CROSS JOIN seconds