@echo off
chcp 65001 > nul

echo 🛑 Waking up the Pauser Sidecar...
docker compose --env-file ../.env run --rm pauser  

echo ⏳ Waiting for the Orchestrator to detect stopped jobs and shut down the JobManager...

:WAIT_LOOP
for /f "tokens=*" %%i in ('docker inspect -f "{{.State.Status}}" jobmanager 2^>nul') do set STATUS=%%i

if "%STATUS%"=="running" (
    timeout /t 2 /nobreak > nul
    goto WAIT_LOOP
)

echo 🧹 Cleaning up remaining containers (TaskManager, Network)...
docker compose stop

echo ✅ Flink Application Cluster completely shut down!

pause