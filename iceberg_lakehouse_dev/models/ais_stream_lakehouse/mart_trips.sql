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

SELECT 
    fst.mmsi,
    fst.trip_id,
    fst.trip_start_time,
    fst.trip_end_time,
    fst.trip_duration_hours,
    fst.start_latitude,
    fst.start_longitude,
    fst.end_latitude,
    fst.end_longitude,
    fst.total_distance_km,
    fst.average_speed_over_ground,
    fst.max_speed_over_ground,
    fst.ping_count,
    dssm.imo_number,
    dssm.call_sign,
    dssm.ship_name,
    dssm.country_name,
    dssm.ship_type_code,
    dssm.ship_type_description,
    dssm.dimension.a AS distance_to_bow,
    dssm.dimension.b AS distance_to_stern,
    dssm.dimension.c AS distance_to_port,
    dssm.dimension.d AS distance_to_starboard,
    (dssm.dimension.a + dssm.dimension.b) AS total_length_meters,
    (dssm.dimension.c + dssm.dimension.d) AS total_width_meters,
    dssm.maximum_static_draught,
    dssm.destination,
    dssm.eta,
    dssm.fix_type_code,
    dssm.fix_type_description,
    dssm.dte
FROM {{ref('fact_ship_trips')}} fst
LEFT JOIN {{ ref('dim_shipstatic_metadata') }} dssm
    ON fst.mmsi = dssm.mmsi
    AND fst.trip_end_time >= dssm.valid_from
    AND fst.trip_end_time < COALESCE(dssm.valid_to, CAST('2099-12-31' AS TIMESTAMP))
{% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
WHERE fst.trip_end_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
{% endif %}
