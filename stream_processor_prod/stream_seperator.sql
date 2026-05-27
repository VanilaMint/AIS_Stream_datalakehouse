DROP TABLE IF EXISTS raw_ais_stream;
CREATE TABLE raw_ais_stream (
    `MessageType` STRING,
    `MetaData` ROW<
        `MMSI` BIGINT,
        `ShipName` STRING,
        `latitude` DOUBLE,
        `longitude` DOUBLE,
        `time_utc` STRING     
    >,
    `Message` ROW<
        `PositionReport` ROW<
            `Cog` DOUBLE,
            `Latitude` DOUBLE,
            `Longitude` DOUBLE,
            `NavigationalStatus` INT,
            `RateOfTurn` INT,
            `Sog` DOUBLE,
            `TrueHeading` INT,
            `UserID` BIGINT,
            `Valid` BOOLEAN,
            `PositionAccuracy` BOOLEAN,
            `Raim` BOOLEAN,
            `RepeatIndicator` INT,
            `CommunicationState` INT,
            `SpecialManoeuvreIndicator` INT,
            `MessageID` INT,
            `Timestamp` INT  
        >,
        `ShipStaticData` ROW<
            `AisVersion` INT,
            `CallSign` STRING,
            `Destination` STRING,
            `Dimension` ROW<
                `A` INT, 
                `B` INT, 
                `C` INT, 
                `D` INT
            >,
            `Dte` BOOLEAN,
            `Eta` ROW<
                `Day` INT, 
                `Hour` INT, 
                `Minute` INT, 
                `Month` INT
            >,
            `FixType` INT,
            `ImoNumber` BIGINT,
            `MaximumStaticDraught` DOUBLE,
            `MessageID` INT,
            `Name` STRING,
            `RepeatIndicator` INT,
            `Type` INT,
            `UserID` BIGINT,
            `Valid` BOOLEAN
        >,
        
        `BaseStationReport` ROW<
            `CommunicationState` INT,
            `FixType` INT,
            `Latitude` DOUBLE,
            `LongRangeEnable` BOOLEAN,
            `Longitude` DOUBLE,
            `MessageID` INT,
            `PositionAccuracy` BOOLEAN,
            `Raim` BOOLEAN,
            `RepeatIndicator` INT,
            `UserID` BIGINT,
            `UtcDay` INT,
            `UtcHour` INT,
            `UtcMinute` INT,
            `UtcMonth` INT,
            `UtcSecond` INT,
            `UtcYear` INT,
            `Valid` BOOLEAN
        >
    >,
    `proctime` AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'json_parsers',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);


DROP TABLE IF EXISTS target_position_reports;
CREATE TABLE target_position_reports (
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
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

DROP TABLE IF EXISTS target_shipstatic_metadata;
CREATE TABLE target_shipstatic_metadata (
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    ais_version INT,
    call_sign STRING,
    destination STRING,
    dimension ROW<`A` INT, `B` INT, `C` INT, `D` INT>,
    dte BOOLEAN,
    eta ROW<`Day` INT, `Hour` INT, `Minute` INT, `Month` INT>,
    fix_type INT,
    imo_number BIGINT,
    maximum_static_draught DOUBLE,
    message_id INT,
    name STRING,
    repeat_indicator INT,
    type INT,
    user_id BIGINT,
    valid BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_SHIPSTATIC_METADATA_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);

DROP TABLE IF EXISTS target_basestation_reports;
CREATE TABLE target_basestation_reports (
    mmsi BIGINT,
    ship_name STRING,
    time_utc STRING,
    communication_state INT,
    fix_type INT,
    latitude DOUBLE,
    long_range_enable BOOLEAN,
    longitude DOUBLE,
    message_id INT,
    position_accuracy BOOLEAN,
    raim BOOLEAN,
    repeat_indicator INT,
    user_id BIGINT,
    utc_day INT,
    utc_hour INT,
    utc_minute INT,
    utc_month INT,
    utc_second INT,
    utc_year INT,
    valid BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_RAW_BASESTATION_REPORT_TOPIC}',
    'properties.bootstrap.servers' = 'broker:19092',
    'format' = 'json',
    'key.format' = 'json',
    'key.fields' = 'mmsi',
    'value.fields-include' = 'ALL'
);


EXECUTE STATEMENT SET
BEGIN
    INSERT INTO target_position_reports
    SELECT 
        MetaData.MMSI,
        MetaData.ShipName,
        MetaData.time_utc,
        Message.PositionReport.Cog,
        Message.PositionReport.Latitude,
        Message.PositionReport.Longitude,
        Message.PositionReport.NavigationalStatus,
        Message.PositionReport.RateOfTurn,
        Message.PositionReport.Sog,
        Message.PositionReport.TrueHeading,
        Message.PositionReport.UserID,
        Message.PositionReport.Valid,
        Message.PositionReport.PositionAccuracy,
        Message.PositionReport.Raim,
        Message.PositionReport.RepeatIndicator,
        Message.PositionReport.CommunicationState,
        Message.PositionReport.SpecialManoeuvreIndicator,
        Message.PositionReport.MessageID,
        Message.PositionReport.`Timestamp`
    FROM raw_ais_stream
    WHERE MessageType = 'PositionReport' 
      AND Message.PositionReport IS NOT NULL;

    INSERT INTO target_shipstatic_metadata
    SELECT 
        MetaData.MMSI,
        MetaData.ShipName,
        MetaData.time_utc,
        Message.ShipStaticData.AisVersion,
        Message.ShipStaticData.CallSign,
        Message.ShipStaticData.Destination,
        Message.ShipStaticData.Dimension,
        Message.ShipStaticData.Dte,
        Message.ShipStaticData.Eta,
        Message.ShipStaticData.FixType,
        Message.ShipStaticData.ImoNumber,
        Message.ShipStaticData.MaximumStaticDraught,
        Message.ShipStaticData.MessageID,
        Message.ShipStaticData.Name,
        Message.ShipStaticData.RepeatIndicator,
        Message.ShipStaticData.Type,
        Message.ShipStaticData.UserID,
        Message.ShipStaticData.Valid
    FROM raw_ais_stream
    WHERE MessageType = 'ShipStaticData' 
      AND Message.ShipStaticData IS NOT NULL;

    INSERT INTO target_basestation_reports
    SELECT 
        MetaData.MMSI,
        MetaData.ShipName,
        MetaData.time_utc,
        Message.BaseStationReport.CommunicationState,
        Message.BaseStationReport.FixType,
        Message.BaseStationReport.Latitude,
        Message.BaseStationReport.LongRangeEnable,
        Message.BaseStationReport.Longitude,
        Message.BaseStationReport.MessageID,
        Message.BaseStationReport.PositionAccuracy,
        Message.BaseStationReport.Raim,
        Message.BaseStationReport.RepeatIndicator,
        Message.BaseStationReport.UserID,
        Message.BaseStationReport.UtcDay,
        Message.BaseStationReport.UtcHour,
        Message.BaseStationReport.UtcMinute,
        Message.BaseStationReport.UtcMonth,
        Message.BaseStationReport.UtcSecond,
        Message.BaseStationReport.UtcYear,
        Message.BaseStationReport.Valid
    FROM raw_ais_stream
    WHERE MessageType = 'BaseStationReport' 
      AND Message.BaseStationReport IS NOT NULL;

END;