CREATE TABLE clean_basestation_reports (
    mmsi BIGINT,
    ship_name STRING,
    country_name STRING,
    recorded_time TIMESTAMP,
    communication_state INT,
    fix_type_code INT,
    fix_type_desc STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    position_accuracy STRING,
    raim BOOLEAN,
    repeat_indicator INT,
    long_range_enable BOOLEAN,
    message_id INT,
    valid BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_BASESTATION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'data presentation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE presentation_basestations (
    mmsi BIGINT,
    country_name STRING,
    recorded_time TIMESTAMP,
    communication_state INT,
    fix_type_code INT,
    fix_type_desc STRING,
    longtitude DOUBLE,
    latitude DOUBLE,
    position_accuracy STRING,
    raim BOOLEAN,
    long_range_enable BOOLEAN,
    PRIMARY KEY (mmsi) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '{KAFKA_PRESENTATION_BASESTATION_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO presentation_basestations
SELECT 
    mmsi,
    country_name,
    recorded_time,
    communication_state,
    fix_type_code,
    fix_type_desc,
    longitude,
    latitude,
    position_accuracy,
    raim,
    long_range_enable
FROM clean_basestation_reports