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


CREATE TABLE ais_stream_data.landing_shipstatic_metadata (
    mmsi BIGINT,
    imo_number BIGINT,
    call_sign STRING,
    ship_name STRING,
    country_name STRING,
    ship_type_code INT,
    ship_type_description STRING,
    dimension STRUCT<A: INT, B: INT, C: INT, D: INT>,
    maximum_static_draught DOUBLE,
    destination STRING,
    eta TIMESTAMP,
    fix_type_code INT,
    fix_type_description STRING,
    recorded_time TIMESTAMP,
    ais_version INT,
    dte STRING,
    valid BOOLEAN,
    repeat_indicator INT,
    message_id INT
) 
PARTITIONED BY (day(recorded_time))
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/landing_shipstatic_metadata/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);

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