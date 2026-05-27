CREATE TABLE starting_position_reports (
    mmsi BIGINT,
    ship_name STRING,
    country_name STRING,
    recorded_time TIMESTAMP(9),
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
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_POSITION_REPORT_TOPIC}',
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

INSERT INTO glue_catalog.ais_stream_data.landing_position_reports
SELECT * FROM starting_position_reports;
