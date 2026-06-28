/*
This file is for creating data landing tables on apache athena
REPLACE WITH YOUR OWN BUCKET NAME
*/
CREATE TABLE ais_stream_data.landing_basestation_reports (
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
) 
PARTITIONED BY (day(recorded_time))
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/landing_basestation_reports/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);