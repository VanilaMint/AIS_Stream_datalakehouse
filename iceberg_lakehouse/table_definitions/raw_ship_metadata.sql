/*
This file is for creating data landing tables on apache athena
REPLACE WITH YOUR OWN BUCKET NAME
*/
CREATE TABLE ais_stream_data.raw_shipstatic_metadata (
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    ais_version INT,
    call_sign STRING,
    destination STRING,
    dimension STRUCT<A: INT, B: INT, C: INT, D: INT>,
    dte BOOLEAN,
    eta STRUCT<Day: INT, Hour: INT, Minute: INT, Month: INT>,
    fix_type INT,
    imo_number BIGINT,
    maximum_static_draught DOUBLE,
    message_id INT,
    name STRING,
    repeat_indicator INT,
    type INT,
    user_id BIGINT,
    valid BOOLEAN
) 
LOCATION 's3://ais-stream-data-304161164368-ap-southeast-2-an/ais_stream_data/raw_shipstatic_metadata/'
TBLPROPERTIES (
    'table_type' ='ICEBERG',
    'format'='parquet',
    'write_compression'='zstd'
);