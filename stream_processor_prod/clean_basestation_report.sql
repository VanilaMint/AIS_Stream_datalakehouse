DROP TABLE IF EXISTS raw_basestation_reports;
CREATE TABLE raw_basestation_reports (
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    communication_state INT,
    fix_type INT,
    latitude DOUBLE,
    long_range_enable BOOLEAN,
    longitude DOUBLE,
    message_id INT,
    position_accuracy BOOLEAN,
    raim BOOLEAN,
    repeat_indicator INT,
    user_id BIGINT,
    utc_day INT,
    utc_hour INT,
    utc_minute INT,
    utc_month INT,
    utc_second INT,
    utc_year INT,
    valid BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_BASESTATION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'basestation_report_cleaners',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

DROP TABLE IF EXISTS clean_basestation_reports;
CREATE TABLE clean_basestation_reports WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_BASESTATION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'basestation_report_handlers',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
) AS
WITH raw_table AS (
    SELECT 
        mmsi,
        CAST(mmsi / 10000 AS INT) AS mid_code,
        ship_name,
        latitude,
        longitude,
        time_utc AS recorded_time,
        communication_state,
        fix_type,
        long_range_enable,
        position_accuracy,
        raim,
        repeat_indicator,
        user_id,
        utc_day,
        utc_hour,
        utc_minute,
        utc_month,
        utc_second,
        utc_year,
        message_id,
        valid,
        PROCTIME() AS proctime
    FROM raw_basestation_reports
    WHERE mmsi IS NOT NULL AND mmsi <> 0
), joined_with_lookup AS (
    SELECT r.*,
        mcd.country_name,
        ftd.fix_type_description AS fix_type_desc
    FROM raw_table r
    LEFT JOIN mid_country_dictionary FOR SYSTEM_TIME AS OF r.proctime AS mcd
    ON r.mid_code = mcd.mid_code
    LEFT JOIN fix_type_dictionary FOR SYSTEM_TIME AS OF r.proctime AS ftd
    ON r.fix_type = ftd.fix_type_code
), cleaned_data AS (
    SELECT 
        mmsi,
        ship_name,
        country_name,
        CAST(SPLIT_INDEX(recorded_time, ' +', 0) AS TIMESTAMP(9)) 
        AS recorded_time,
        communication_state,
        fix_type AS fix_type_code,
        fix_type_desc,
        latitude,
        longitude,
        CASE 
            WHEN position_accuracy = TRUE THEN 'High (<= 10m)'
            ELSE 'Low (> 10m)'
        END AS position_accuracy,
        raim,
        repeat_indicator,
        long_range_enable,
        message_id,
        valid
    FROM joined_with_lookup
    WHERE message_id = 4
) SELECT * FROM cleaned_data;