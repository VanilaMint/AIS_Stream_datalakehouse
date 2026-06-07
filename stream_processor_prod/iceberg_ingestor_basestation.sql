CREATE TABLE starting_basestation_reports (
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
  's3.access-key-id'='{AWS_ACCESS_KEY_ID}',
  's3.secret-access-key'='{AWS_SECRET_ACCESS_KEY}'
);

INSERT INTO glue_catalog.ais_stream_data.landing_basestation_reports
SELECT * FROM starting_basestation_reports;