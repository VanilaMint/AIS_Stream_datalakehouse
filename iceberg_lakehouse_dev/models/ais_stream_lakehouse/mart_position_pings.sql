{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ping_id'],
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

WITH joined_data AS (
    SELECT 
        fpr.mmsi,
        fpr.trip_id,
        fpr.recorded_time,
        dssm.imo_number,
        dssm.call_sign,
        dssm.ship_name,
        dssm.country_name,
        fpr.latitude,
        fpr.longitude,
        fpr.position_accuracy,
        fpr.speed_over_ground,
        fpr.course_over_ground,
        fpr.true_heading,
        fpr.rate_of_turn,
        fpr.navigational_status,
        fpr.nav_status_desc,
        fpr.special_manoeuvre_indicator,
        fpr.raim,
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
        dssm.dte,
        ROW_NUMBER() OVER (
            PARTITION BY fpr.mmsi, fpr.recorded_time 
            ORDER BY dssm.valid_from DESC
        ) as merge_dedup_rn,
        CAST(fpr.mmsi AS VARCHAR) || '-' || CAST(to_unixtime(fpr.recorded_time) AS VARCHAR) AS ping_id
        
    FROM {{ ref('fact_position_reports') }} AS fpr
    LEFT JOIN {{ ref('dim_shipstatic_metadata') }} AS dssm
        ON fpr.mmsi = dssm.mmsi
        AND fpr.recorded_time >= dssm.valid_from
        AND fpr.recorded_time < COALESCE(dssm.valid_to, CAST('2099-12-31' AS TIMESTAMP))

    {% if is_incremental() and high_watermark is defined and high_watermark != 'None' %}
    WHERE fpr.recorded_time >= cast('{{ high_watermark }}' as timestamp) - interval '{{ var("watermark_delay") }}' day
    {% endif %}
)

SELECT 
    mmsi,
    trip_id,
    recorded_time,
    imo_number,
    call_sign,
    ship_name,
    country_name,
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
    raim,
    ship_type_code,
    ship_type_description,
    distance_to_bow,
    distance_to_stern,
    distance_to_port,
    distance_to_starboard,
    total_length_meters,
    total_width_meters,
    maximum_static_draught,
    destination,
    eta,
    fix_type_code,
    fix_type_description,
    dte,
    ping_id
FROM joined_data
WHERE merge_dedup_rn = 1