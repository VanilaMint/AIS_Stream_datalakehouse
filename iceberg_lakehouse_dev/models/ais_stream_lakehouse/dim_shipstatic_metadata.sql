{{ config(materialized='view') }}

WITH calculated_timeline AS (
    SELECT 
        mmsi,
        imo_number,
        call_sign,
        ship_name,
        country_name,
        ship_type_code,
        ship_type_description,
        dimension,
        maximum_static_draught,
        destination,
        eta,
        fix_type_code,
        fix_type_description,
        ais_version,
        dte,
        valid,
        repeat_indicator,
        message_id,
        recorded_time AS valid_from,
        LEAD(recorded_time) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS valid_to
    FROM {{ ref('ship_metadata_change') }}
)

SELECT * FROM calculated_timeline