DROP TABLE IF EXISTS raw_shipstatic_metadata;
CREATE TABLE raw_shipstatic_metadata(
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    ais_version INT,
    call_sign STRING,
    destination STRING,
    dimension ROW<`A` INT, `B` INT, `C` INT, `D` INT>,
    dte BOOLEAN,
    eta ROW<`Day` INT, `Hour` INT, `Minute` INT, `Month` INT>,
    fix_type INT,
    imo_number BIGINT,
    maximum_static_draught DOUBLE,
    message_id INT,
    name STRING,
    repeat_indicator INT,
    type INT,
    user_id BIGINT,
    valid BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_SHIPSTATIC_METADATA_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'shipstatic_metadata_cleaners',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

DROP TABLE IF EXISTS cleaned_shipstatic_metadata;
CREATE TABLE cleaned_shipstatic_metadata WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_SHIPSTATIC_METADATA_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
) AS
WITH cleaned_shipstatic_metadata AS (
    SELECT 
        mmsi,
        CAST(mmsi / 1000000 AS INT) AS mid_code,
        ship_name,
        CAST(SPLIT_INDEX(time_utc, ' +', 0) AS TIMESTAMP(9)) 
        AS recorded_time,
        CASE
            WHEN call_sign = '@@@@@@@' THEN NULL
            ELSE call_sign
        END as call_sign,
        CASE
            WHEN destination = '@@@@@@@@@@@@@@@@@@@@' THEN NULL
            ELSE destination
        END as destination,
        CASE
            WHEN dimension.A = 0 AND dimension.B = 0 AND dimension.C = 0 AND dimension.D = 0 THEN NULL
            ELSE dimension
        END as dimension,
        CASE
            WHEN dte = TRUE THEN 'Not Available'
            ELSE 'Available'
        END as dte,
        CASE
            WHEN eta.`Day` = 0 AND eta.`Hour` = 24 AND eta.`Minute` = 60 AND eta.`Month` = 0 THEN NULL
            ELSE eta
        END as eta,
        fix_type,
        CASE imo_number
            WHEN 0 THEN NULL
            ELSE imo_number
        END AS imo_number,
        CASE
            WHEN maximum_static_draught = 0 THEN NULL
            ELSE maximum_static_draught
        END as maximum_static_draught,
        message_id,
        repeat_indicator,
        type AS ship_type,
        valid,
        ais_version,
        PROCTIME() AS proctime
    FROM raw_shipstatic_metadata
    WHERE mmsi IS NOT NULL AND  mmsi >= 201000000 AND mmsi <= 755999999
), parsed_eta AS (
    SELECT *,
        CASE
            WHEN eta.`Month` BETWEEN 1 AND 12 
            AND eta.`Day` BETWEEN 1 AND 31 
            AND eta.`Hour` BETWEEN 0 AND 23 
            AND eta.`Minute` BETWEEN 0 AND 59
            THEN TO_TIMESTAMP(
                CAST(EXTRACT(YEAR FROM recorded_time) AS STRING) || '-' ||
                LPAD(CAST(eta.`Month` AS STRING), 2, '0') || '-' ||
                LPAD(CAST(eta.`Day` AS STRING), 2, '0') || ' ' ||
                LPAD(CAST(eta.`Hour` AS STRING), 2, '0') || ':' ||
                LPAD(CAST(eta.`Minute` AS STRING), 2, '0') || ':00'
            )
            ELSE NULL
        END AS parsed_eta
    FROM cleaned_shipstatic_metadata
), 
joined_with_lookup AS (
    SELECT csm.*,
        mcd.country_name,
        ftd.fix_type_description,
        std.type_description AS ship_type_description
    FROM parsed_eta csm
    LEFT JOIN mid_country_dictionary FOR SYSTEM_TIME AS OF csm.proctime AS mcd
        ON csm.mid_code = mcd.mid_code
    LEFT JOIN fix_type_dictionary FOR SYSTEM_TIME AS OF csm.proctime AS ftd
        ON csm.fix_type = ftd.fix_type_code
    LEFT JOIN ship_type_dictionary FOR SYSTEM_TIME AS OF csm.proctime AS std
        ON csm.ship_type = std.type_code
), final AS (
    SELECT 
    mmsi,
    imo_number,
    call_sign,
    ship_name,
    country_name,
    ship_type as ship_type_code,
    ship_type_description,
    dimension,
    maximum_static_draught,
    destination,
    parsed_eta as eta,
    fix_type as fix_type_code,
    fix_type_description,
    recorded_time,
    ais_version,
    dte,
    valid,
    repeat_indicator,
    message_id
FROM joined_with_lookup
) SELECT * FROM final;
