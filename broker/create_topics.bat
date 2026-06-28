@echo off
setlocal enabledelayedexpansion


set ENV_FILE= ../.env
for /f "tokens=1,* delims==" %%a in (%ENV_FILE%) do (
    set "%%a=%%b"
)

set BROKER_CONTAINER=broker
set BOOTSTRAP_SERVER=%HOST_IP%:%HOST_KAFKA_BROKER_PORT%
set MAX_SIZE=214748364


set STANDARD_TOPICS=%KAFKA_RAW_TOPIC% %KAFKA_RAW_POSITION_REPORT_TOPIC% %KAFKA_RAW_SHIPSTATIC_METADATA_TOPIC% %KAFKA_RAW_BASESTATION_REPORT_TOPIC% %KAFKA_CLEAN_POSITION_REPORT_TOPIC% %KAFKA_CLEAN_SHIPSTATIC_METADATA_TOPIC% %KAFKA_CLEAN_BASESTATION_REPORT_TOPIC%
set PRESENTATION_TOPICS=%KAFKA_PRESENTATION_LIVE_SHIP_TOPIC% %KAFKA_PRESENTATION_BASESTATION_TOPIC%

echo ========================================
echo Bootstrapping Kafka Environment...
echo ========================================

echo.
echo Creating Standard Topics (Policy: delete)...
for %%t in (%STANDARD_TOPICS%) do (
    docker exec %BROKER_CONTAINER% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP_SERVER% --create --if-not-exists --topic %%t --partitions 3 --replication-factor 1 --config retention.bytes=%MAX_SIZE% --config cleanup.policy=delete
    echo   [+] Created: %%t
)

echo.
echo Creating Presentation Topics (Policy: compact,delete)...
for %%t in (%PRESENTATION_TOPICS%) do (
    docker exec %BROKER_CONTAINER% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP_SERVER% --create --if-not-exists --topic %%t --partitions 3 --replication-factor 1 --config retention.bytes=%MAX_SIZE% --config cleanup.policy=compact,delete
    echo   [+] Created: %%t
)

echo.
echo ========================================
echo All topics successfully provisioned!
echo.
pause