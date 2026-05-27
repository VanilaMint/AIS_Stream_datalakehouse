/*
 Map pings into their respective trips.
 Has watermark logic to account for late arriving data
 Can only handle out of order data when it arrives within the same batch
*/
{{ config(
    materialized='incremental',
    incremental_strategy='append',
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
    FROM {{ source('ais_stream', 'landing_position_reports') }}
    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
        WHERE recorded_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
),

ship_history AS (
    {% if state_relation is not none %}
        SELECT 
        mmsi, 
        trip_end_time AS latest_recorded_time, 
        latest_trip_id, 
        end_latitude AS latest_latitude, 
        end_longitude AS latest_longitude, 
        speed_over_ground AS latest_speed_over_ground
        FROM {{ state_source }}
    {% else %}
        SELECT 
            CAST(NULL AS BIGINT) AS mmsi, 
            CAST(NULL AS TIMESTAMP) AS latest_recorded_time, 
            CAST(NULL AS BIGINT) AS latest_trip_id,
            CAST(NULL AS DOUBLE) AS latest_latitude,
            CAST(NULL AS DOUBLE) AS latest_longitude,
            CAST(NULL AS DOUBLE) AS latest_speed_over_ground
        WHERE 1 = 0
    {% endif %}
),

unprocessed_data AS (
    SELECT
        r.*,
        sh.latest_recorded_time,
        sh.latest_trip_id,
        sh.latest_latitude,
        sh.latest_longitude,
        sh.latest_speed_over_ground
    FROM recent_events r
    LEFT JOIN ship_history sh
        ON r.mmsi = sh.mmsi
    WHERE sh.latest_recorded_time IS NULL
       OR r.recorded_time >= sh.latest_recorded_time
),

added_lag_fields AS (
    SELECT 
        *,
        COALESCE(
            LAG(speed_over_ground) OVER (PARTITION BY mmsi ORDER BY recorded_time), 
            latest_speed_over_ground
        ) AS previous_speed_over_ground,
        
        COALESCE(
            LAG(latitude) OVER (PARTITION BY mmsi ORDER BY recorded_time), 
            latest_latitude
        ) AS previous_latitude,
        
        COALESCE(
            LAG(longitude) OVER (PARTITION BY mmsi ORDER BY recorded_time), 
            latest_longitude
        ) AS previous_longitude,

        COALESCE(
            LAG(recorded_time) OVER (PARTITION BY mmsi ORDER BY recorded_time), 
            latest_recorded_time
        ) AS previous_recorded_time

    FROM unprocessed_data
),

seperated_into_trips AS (
    SELECT *,
        CASE
            --new trip if no previous data
            WHEN previous_speed_over_ground IS NULL THEN 1
            WHEN previous_latitude IS NULL OR previous_longitude IS NULL THEN 1
            --check for speed change
            --low threshhold to seperate when the engine is off but still transfering pings
            WHEN speed_over_ground > 1.5 AND previous_speed_over_ground <= 1.5 THEN 1
            WHEN speed_over_ground <= 0.5 AND previous_speed_over_ground > 0.5 THEN 1
            --check for time gap (assign new trip when pipeline goes off)
            WHEN date_diff('hour', previous_recorded_time, recorded_time) >= 4 THEN 1
            --check for impossible distance gap (may mess with data because mmsi is not unique)
            WHEN (6371 * acos(cos(radians(latitude)) * cos(radians(previous_latitude)) * cos(radians(longitude) - radians(previous_longitude)) + sin(radians(latitude)) * sin(radians(previous_latitude)))) > 50 THEN 1
            ELSE 0
        END AS trip_start_flag
    FROM added_lag_fields
    WHERE recorded_time > latest_recorded_time OR latest_recorded_time IS NULL
),

added_trip_id AS (
    SELECT *,
        SUM(trip_start_flag) OVER (PARTITION BY mmsi ORDER BY recorded_time)
        + COALESCE(latest_trip_id, 0) AS trip_id
    FROM seperated_into_trips
)

SELECT
    mmsi,
    ship_name,
    country_name,
    recorded_time,
    message_id,
    latitude,
    longitude,
    position_accuracy,
    speed_over_ground,
    course_over_ground,
    true_heading,
    rate_of_turn,
    navigational_status,
    nav_status_desc,
    special_manoeuvre_indicator,
    valid,
    raim,
    repeat_indicator,
    communication_state,
    trip_id
FROM added_trip_id