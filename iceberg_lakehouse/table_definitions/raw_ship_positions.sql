/*
This file is for creating data landing tables on apache athena
REPLACE WITH YOUR OWN BUCKET NAME
*/
CREATE TABLE ais_stream_data.raw_position_reports (
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    cog DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    navigational_status INT,
    rate_of_turn INT,
    sog DOUBLE,
    true_heading INT,
    user_id BIGINT,
    valid BOOLEAN,
    position_accuracy BOOLEAN,
    raim BOOLEAN,
    repeat_indicator INT,
    communication_state INT,
    special_manoeuvre_indicator INT,
    message_id INT,
    timestamp_val INT
) 
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/raw_position_reports/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);