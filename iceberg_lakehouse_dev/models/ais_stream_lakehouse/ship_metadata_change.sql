/*
This models filter the ship static metadata stream
only keep the records where change occurs to the ship metadata
*/
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['change_id'],
    partitioned_by= ['day(recorded_time)']
) }}

{% set state_source = source('state_tables', 'ship_latest_state') %}
{% set state_relation = adapter.get_relation(database=state_source.database, schema=state_source.schema, identifier=state_source.name) %}

{% if execute and is_incremental() and state_relation is not none %}
    {% set get_watermark_query %}
        SELECT CAST(MAX(trip_end_time) AS VARCHAR) FROM {{ state_source }}
    {% endset %}
    
    {% set watermark_result = run_query(get_watermark_query) %}
    
    {% if execute %}
        {% set high_watermark = watermark_result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH
recent_events AS (
    SELECT *
    FROM {{ source('ais_stream', 'landing_shipstatic_metadata') }}
    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
        WHERE recorded_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
),
add_change_detect_fields AS (
    SELECT 
        *,
        LAG(recorded_time) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_recorded_time,
        LAG(imo_number) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_imo_number,
        LAG(call_sign) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_call_sign,
        LAG(ship_name) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_ship_name,
        LAG(ship_type_code) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_ship_type_code,
        LAG(dimension) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_dimension,
        LAG(destination) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_destination,
        LAG(eta) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_eta,
        LAG(fix_type_code) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_fix_type_code,
        LAG(dte) OVER (PARTITION BY mmsi ORDER BY recorded_time) AS prev_dte,
        CAST(mmsi AS VARCHAR) || '-' || CAST(to_unixtime(recorded_time) AS VARCHAR) AS change_id
    FROM recent_events
)

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
    recorded_time,
    ais_version,
    dte,
    valid,
    repeat_indicator,
    message_id,
    change_id
FROM add_change_detect_fields
WHERE 
    -- check for no previous record (first record for this mmsi in this patch)
    prev_recorded_time IS NULL
    OR imo_number IS DISTINCT FROM prev_imo_number
    OR call_sign IS DISTINCT FROM prev_call_sign
    OR ship_name IS DISTINCT FROM prev_ship_name
    OR ship_type_code IS DISTINCT FROM prev_ship_type_code
    OR dimension IS DISTINCT FROM prev_dimension
    OR destination IS DISTINCT FROM prev_destination
    OR eta IS DISTINCT FROM prev_eta
    OR fix_type_code IS DISTINCT FROM prev_fix_type_code
    OR dte IS DISTINCT FROM prev_dte
