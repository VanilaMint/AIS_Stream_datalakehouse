/*
This file is for creating data landing tables on apache athena
REPLACE WITH YOUR OWN BUCKET NAME
*/
CREATE TABLE ais_stream_data.raw_basestation_reports (
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
) 
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/raw_basestation_reports/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);