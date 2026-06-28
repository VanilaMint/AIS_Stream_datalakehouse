CREATE TABLE raw_position_reports (
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
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_POSITION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'position_report_cleaners',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

DROP TABLE IF EXISTS clean_position_reports;
CREATE TABLE clean_position_reports (
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
    'properties.group.id' = 'position_report_handlers',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

INSERT INTO clean_position_reports
WITH parsed_position_reports AS(
    SELECT 
        mmsi,
        CAST(mmsi / 1000000 AS INT) AS mid_code,
        ship_name,
        latitude,
        longitude,
        time_utc AS recorded_time,
        cog AS course_over_ground,
        sog AS speed_over_ground,
        true_heading AS true_heading,
        navigational_status AS navigational_status,
        rate_of_turn AS rate_of_turn,
        valid AS valid,
        position_accuracy AS position_accuracy,
        raim AS raim,
        repeat_indicator AS repeat_indicator,
        communication_state AS communication_state,
        special_manoeuvre_indicator AS special_manoeuvre_indicator,
        message_id AS message_id,
        PROCTIME() AS proctime
    FROM raw_position_reports
    WHERE mmsi IS NOT NULL AND  mmsi >= 201000000 AND mmsi <= 755999999
),
joined_with_lookup_position_reports AS (
    SELECT p.*,
        mcd.country_name,
        nsd.status_code_description AS nav_status_desc
    FROM parsed_position_reports p
    LEFT JOIN mid_country_dictionary FOR SYSTEM_TIME AS OF p.proctime AS mcd
        ON p.mid_code = mcd.mid_code
    LEFT JOIN nav_status_dictionary FOR SYSTEM_TIME AS OF p.proctime AS nsd
        ON p.navigational_status = nsd.status_code
),
reverted_encoding_position_reports AS (
    SELECT mmsi,
        ship_name,
        CAST(SPLIT_INDEX(recorded_time, ' +', 0) AS TIMESTAMP(9)) 
        AS recorded_time,
        navigational_status,
        valid,
        raim,
        repeat_indicator,
        communication_state,
        message_id,
        country_name,
        nav_status_desc,
        CASE 
            WHEN rate_of_turn = -128 THEN NULL
            WHEN rate_of_turn = 127 THEN 709.0
            WHEN rate_of_turn = -127 THEN -709.0
            ELSE SIGN(rate_of_turn) * POWER( (ABS(rate_of_turn) / 4.733), 2)
        END AS rate_of_turn,
        CASE
            WHEN speed_over_ground = 102.3 THEN NULL
            ELSE speed_over_ground
        END AS speed_over_ground,
        CASE
            WHEN course_over_ground = 360.0 THEN NULL
            ELSE course_over_ground
        END AS course_over_ground,
        CASE
            WHEN longitude = 181.0 THEN NULL
            ELSE longitude
        END AS longitude,
        CASE
            WHEN latitude = 91.0 THEN NULL
            ELSE latitude
        END AS latitude,
        CASE
            WHEN true_heading = 511 THEN NULL
            ELSE true_heading
        END AS true_heading,
        CASE
            WHEN special_manoeuvre_indicator = 0 THEN 'Not available (default)'
            WHEN special_manoeuvre_indicator = 1 THEN 'Not engaged in special manoeuvre'
            WHEN special_manoeuvre_indicator = 2 THEN 'Engaged in special manoeuvre'
            ELSE 'Unknown'
        END AS special_manoeuvre_indicator,
        CASE 
            WHEN position_accuracy THEN 'High (<= 10m)'
            ELSE 'Low (> 10m)'
        END AS position_accuracy,
        proctime
    FROM joined_with_lookup_position_reports
)
SELECT 
    mmsi, 
    ship_name, 
    country_name, 
    recorded_time, 
    message_id, 
    latitude, 
    longitude, 
    position_accuracy, 
    speed_over_ground, 
    course_over_ground, 
    true_heading, 
    rate_of_turn, 
    navigational_status as nav_status_code, 
    nav_status_desc, 
    special_manoeuvre_indicator, 
    valid, 
    raim, 
    repeat_indicator, 
    communication_state
FROM reverted_encoding_position_reports;

