/*
Store states to avoid infinite lookups
Assuming limited number of unique ships (mmsi) so data is small
*/
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['mmsi']
) }}

{% if execute and is_incremental() %}
    {% set get_watermark_query %}
        SELECT CAST(MAX(trip_end_time) AS VARCHAR) FROM {{ this }}
    {% endset %}
    
    {% set watermark_result = run_query(get_watermark_query) %}
    
    {% if execute %}
        {% set high_watermark = watermark_result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH ping_latest_state AS(
    SELECT 
        mmsi,
        MAX(recorded_time) AS recorded_time,
        MAX(trip_id) AS trip_id,
        MAX_BY(latitude, recorded_time) AS latitude,
        MAX_BY(longitude, recorded_time) AS longitude,
        MAX_BY(speed_over_ground, recorded_time) AS speed_over_ground
    FROM {{ ref('fact_position_reports') }}
    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
        WHERE recorded_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
    GROUP BY mmsi
),
trip_latest_state AS (
    SELECT 
        mmsi,
        MAX_BY(trip_start_time, trip_id) AS trip_start_time,
        MAX_BY(start_latitude, trip_id) AS start_latitude,
        MAX_BY(start_longitude, trip_id) AS start_longitude,
        MAX_BY(trip_duration_hours, trip_id) AS trip_duration,
        MAX_BY(average_speed_over_ground, trip_id) AS trip_avg_speed,
        MAX_BY(total_distance_km, trip_id) AS trip_distance,
        MAX_BY(max_speed_over_ground, trip_id) AS trip_max_speed,
        MAX_BY(ping_count, trip_id) AS trip_ping_count
    FROM {{ ref('fact_ship_trips') }}
    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
        WHERE trip_end_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
    GROUP BY mmsi
),
final_latest_state AS (
    SELECT 
        ps.mmsi,
        ps.trip_id AS latest_trip_id,
        ts.trip_start_time,
        ps.recorded_time AS trip_end_time,
        ts.start_latitude,
        ts.start_longitude,
        ps.latitude AS end_latitude,
        ps.longitude AS end_longitude,
        ps.speed_over_ground,
        ts.trip_duration,
        ts.trip_avg_speed,
        ts.trip_distance,
        ts.trip_max_speed,
        ts.trip_ping_count
    FROM ping_latest_state ps
    LEFT JOIN trip_latest_state ts
    ON ps.mmsi = ts.mmsi
)
SELECT * FROM final_latest_state;