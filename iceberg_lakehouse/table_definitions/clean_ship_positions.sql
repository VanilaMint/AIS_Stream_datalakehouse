/*
This file is for creating data landing tables on apache athena
REPLACE WITH YOUR OWN BUCKET NAME
*/
CREATE TABLE ais_stream_data.landing_position_reports (
    mmsi BIGINT,
    ship_name STRING,
    country_name STRING,
    recorded_time TIMESTAMP,
    message_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    position_accuracy STRING,
    speed_over_ground DOUBLE,
    course_over_ground DOUBLE,
    true_heading INT,
    rate_of_turn DOUBLE,
    navigational_status INT,
    nav_status_desc STRING,
    special_manoeuvre_indicator STRING,
    valid BOOLEAN,
    raim BOOLEAN,
    repeat_indicator INT,
    communication_state INT
) 
PARTITIONED BY (day(recorded_time))
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/landing_position_reports/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);
