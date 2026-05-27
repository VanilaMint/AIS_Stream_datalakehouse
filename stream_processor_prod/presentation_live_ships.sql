CREATE TABLE clean_position_reports (
    mmsi BIGINT,
    ship_name STRING,
    country_name STRING,
    recorded_time TIMESTAMP(3),
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
    communication_state INT,
    WATERMARK FOR recorded_time AS recorded_time - INTERVAL '5' SECOND
) WITH(
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_POSITION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'data presentation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
CREATE TABLE clean_shipstatic_metadata (
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
    recorded_time TIMESTAMP(3),
    ais_version INT,
    dte STRING,
    valid BOOLEAN,
    repeat_indicator INT,
    message_id INT,
    WATERMARK FOR recorded_time AS recorded_time - INTERVAL '5' SECOND
) WITH(
    'connector' = 'kafka',
    'topic' = '{KAFKA_CLEAN_SHIPSTATIC_METADATA_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'data presentation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE VIEW versioed_clean_shipstatic_metadata AS
SELECT *
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY recorded_time DESC) AS rownum
    FROM clean_shipstatic_metadata)
WHERE rownum = 1;

CREATE TABLE presentation_live_ships(
    mmsi BIGINT,
    ship_name STRING,
    country_name STRING,
    recorded_time TIMESTAMP(9),
    message_id INT,
    longitude DOUBLE,
    latitude DOUBLE,
    position_accuracy STRING,
    speed_over_ground DOUBLE,
    course_over_ground DOUBLE,
    true_heading INT,
    rate_of_turn DOUBLE,
    navigational_status INT,
    nav_status_desc STRING,
    special_manoeuvre_indicator STRING,
    raim BOOLEAN,
    imo_number BIGINT,
    call_sign STRING,
    ship_type_code INT,
    ship_type_description STRING,
    dimension_a INT,
    dimension_b INT,
    dimension_c INT,
    dimension_d INT,
    ship_width INT,
    ship_length INT,
    maximum_static_draught DOUBLE,
    destination STRING,
    eta TIMESTAMP,
    fix_type_code INT,
    fix_type_description STRING,
    dte STRING,
    PRIMARY KEY (mmsi) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '{KAFKA_PRESENTATION_LIVE_SHIP_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO presentation_live_ships
SELECT
        cpr.mmsi,
        cpr.ship_name,
        cpr.country_name,
        cpr.recorded_time,
        cpr.message_id,
        cpr.longitude,
        cpr.latitude,
        cpr.position_accuracy,
        cpr.speed_over_ground,
        cpr.course_over_ground,
        cpr.true_heading,
        cpr.rate_of_turn,
        cpr.navigational_status,
        cpr.nav_status_desc,
        cpr.special_manoeuvre_indicator,
        cpr.raim,
        csm.imo_number,
        csm.call_sign,
        csm.ship_type_code,
        csm.ship_type_description,
        csm.dimension.A AS dimension_a,
        csm.dimension.B AS dimension_b,
        csm.dimension.C AS dimension_c,
        csm.dimension.D AS dimension_d,
        (COALESCE(csm.dimension.A, 0) + COALESCE(csm.dimension.B, 0)) AS ship_length,
        (COALESCE(csm.dimension.C, 0) + COALESCE(csm.dimension.D, 0)) AS ship_width,
        csm.maximum_static_draught,
        csm.destination,
        csm.eta,
        csm.fix_type_code,
        csm.fix_type_description,
        csm.dte
FROM clean_position_reports cpr
LEFT JOIN versioed_clean_shipstatic_metadata FOR SYSTEM_TIME AS OF cpr.recorded_time csm
    ON cpr.mmsi = csm.mmsi;