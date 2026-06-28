{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['mmsi']
) }}

WITH unprocessed_data AS (
    SELECT *
    FROM {{ source('ais_stream', 'landing_basestation_reports') }}
    {% if is_incremental() %}
        WHERE recorded_time >= (SELECT MAX(recorded_time) FROM {{ this }})
    {% endif %}
),

deduplicated_data AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY recorded_time DESC) AS row_num
        FROM unprocessed_data
    ) AS ranked_data
    WHERE row_num = 1
),

final AS (
    SELECT 
        mmsi,
        country_name,
        recorded_time,
        fix_type_code,
        fix_type_desc,
        latitude,
        longitude,
        position_accuracy,
        raim,
        long_range_enable
    FROM deduplicated_data
)

SELECT *
FROM final