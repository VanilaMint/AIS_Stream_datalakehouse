{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['mmsi', 'trip_id'],
    partitioned_by= ['day(trip_start_time)']
) }}

{% set state_source = source('state_tables', 'ship_latest_state') %}
{% set state_relation = adapter.get_relation(database=state_source.database, schema=state_source.schema, identifier=state_source.name) %}
    
{% if execute and state_relation is not none %}
    {% set get_watermark_query %}
        SELECT CAST(MAX(trip_end_time) AS VARCHAR) FROM {{ state_source }}
    {% endset %}
        
    {% set watermark_result = run_query(get_watermark_query) %}
        
    {% if execute %}
           {% set high_watermark = watermark_result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH previous_trip_data AS (
    {% if state_relation is not none %}
        SELECT 
            mmsi, 
            latest_trip_id, 
            trip_end_time AS latest_recorded_time, 
            trip_start_time, 
            start_latitude, 
            start_longitude, 
            trip_duration AS previous_trip_duration, 
            trip_avg_speed AS previous_trip_avg_speed, 
            trip_distance AS previous_trip_distance, 
            trip_max_speed AS previous_trip_max_speed, 
            trip_ping_count AS previous_trip_ping_count,
            end_latitude AS previous_end_latitude,
            end_longitude AS previous_end_longitude
        FROM {{ state_source }}
    {% else %}
        SELECT 
            CAST(NULL AS BIGINT) AS mmsi, 
            CAST(NULL AS BIGINT) AS latest_trip_id,
            CAST(NULL AS TIMESTAMP) AS latest_recorded_time, 
            CAST(NULL AS TIMESTAMP) AS trip_start_time,
            CAST(NULL AS DOUBLE) AS start_latitude,
            CAST(NULL AS DOUBLE) AS start_longitude,
            CAST(NULL AS DOUBLE) AS previous_trip_duration,
            CAST(NULL AS DOUBLE) AS previous_trip_avg_speed,
            CAST(NULL AS DOUBLE) AS previous_trip_distance,
            CAST(NULL AS DOUBLE) AS previous_trip_max_speed,
            CAST(NULL AS BIGINT) AS previous_trip_ping_count,
            CAST(NULL AS DOUBLE) AS previous_end_latitude,
            CAST(NULL AS DOUBLE) AS previous_end_longitude
        WHERE 1 = 0
    {% endif %}
),
recent_pings AS (
    SELECT r.*
    FROM {{ ref('fact_position_reports') }} r
    LEFT JOIN previous_trip_data ptd 
        ON r.mmsi = ptd.mmsi
    WHERE 1 = 1
    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
        AND r.recorded_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
    AND (ptd.latest_recorded_time IS NULL OR r.recorded_time > ptd.latest_recorded_time)
),
pings_with_distance AS (
    SELECT 
        *,
        LAG(latitude) OVER (PARTITION BY mmsi, trip_id ORDER BY recorded_time) as prev_lat,
        LAG(longitude) OVER (PARTITION BY mmsi, trip_id ORDER BY recorded_time) as prev_lon
    FROM recent_pings
),

new_trips AS (
    SELECT 
        mmsi,
        trip_id,
        MIN(recorded_time) AS trip_start_time,
        MAX(recorded_time) AS trip_end_time,
        date_diff('second', MIN(recorded_time), MAX(recorded_time)) / 3600.0 AS trip_duration_hours,
        MIN_BY(latitude, recorded_time) AS start_latitude,
        MIN_BY(longitude, recorded_time) AS start_longitude,
        MAX_BY(latitude, recorded_time) AS end_latitude,
        MAX_BY(longitude, recorded_time) AS end_longitude,
        
        SUM(CASE 
            WHEN prev_lat IS NOT NULL AND prev_lon IS NOT NULL THEN
                (6371 * acos(cos(radians(latitude)) * cos(radians(prev_lat)) * cos(radians(longitude) - radians(prev_lon)) + sin(radians(latitude)) * sin(radians(prev_lat))))
            ELSE 0
        END) AS total_distance_km,
        
        AVG(speed_over_ground) AS average_speed_over_ground,
        MAX(speed_over_ground) AS max_speed_over_ground,
        COUNT(*) AS ping_count
    FROM pings_with_distance
    GROUP BY mmsi, trip_id

),

final_merged_trips AS(
    SELECT
        nt.mmsi,
        nt.trip_id,
        COALESCE(ptd.trip_start_time, nt.trip_start_time) AS trip_start_time,
        nt.trip_end_time,
        
        date_diff('second', COALESCE(ptd.trip_start_time, nt.trip_start_time), nt.trip_end_time) / 3600.0 AS trip_duration_hours,
        
        COALESCE(ptd.start_latitude, nt.start_latitude) AS start_latitude,
        COALESCE(ptd.start_longitude, nt.start_longitude) AS start_longitude,
        nt.end_latitude,
        nt.end_longitude,
        nt.total_distance_km 
        + COALESCE(ptd.previous_trip_distance, 0)
        + COALESCE(
            (6371 * acos(cos(radians(nt.start_latitude)) * cos(radians(ptd.previous_end_latitude)) * cos(radians(nt.start_longitude) - radians(ptd.previous_end_longitude)) + sin(radians(nt.start_latitude)) * sin(radians(ptd.previous_end_latitude)))), 
            0
          ) AS total_distance_km,
        (nt.average_speed_over_ground * nt.ping_count + COALESCE(ptd.previous_trip_avg_speed * ptd.previous_trip_ping_count, 0)) / NULLIF(nt.ping_count + COALESCE(ptd.previous_trip_ping_count, 0), 0) AS average_speed_over_ground,
        GREATEST(nt.max_speed_over_ground, COALESCE(ptd.previous_trip_max_speed, 0)) AS max_speed_over_ground,
        nt.ping_count + COALESCE(ptd.previous_trip_ping_count, 0) AS ping_count
    FROM new_trips nt
    LEFT JOIN previous_trip_data ptd
        ON nt.mmsi = ptd.mmsi AND nt.trip_id = ptd.latest_trip_id
)

SELECT * FROM final_merged_trips