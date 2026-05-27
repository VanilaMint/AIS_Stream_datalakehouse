CREATE TABLE starting_shipstatic_metadata (
    mmsi BIGINT,
    imo_number BIGINT,
    call_sign STRING,
    ship_name STRING,
    country_name STRING,
    ship_type_code INT,
    ship_type_description STRING,
    dimension ROW<A INT, B INT, C INT, D INT>,
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
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_SHIPSTATIC_METADATA_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 's3_ingestors',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

DROP CATALOG IF EXISTS glue_catalog;
CREATE CATALOG glue_catalog WITH (
  'type'='iceberg',
  'warehouse' = '{AWS_S3_WAREHOUSE_LINK}',
  'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  'client.region'='{AWS_REGION}',
  's3.access-key-id'='{AWS_INGESTOR_ACCESS_KEY_ID}',
  's3.secret-access-key'='{AWS_INGESTOR_SECRET_ACCESS_KEY}'
);


INSERT INTO glue_catalog.ais_stream_data.landing_shipstatic_metadata
SELECT * FROM starting_shipstatic_metadata;
