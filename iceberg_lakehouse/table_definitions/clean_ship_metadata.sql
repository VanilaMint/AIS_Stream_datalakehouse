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