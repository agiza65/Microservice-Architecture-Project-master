@echo off
setlocal enabledelayedexpansion

echo =====================================
echo Enhanced Kafka Directory Locator and Setup
echo =====================================

REM Check current directory first
echo Current directory: %CD%
echo.

REM Check if Kafka files exist in current directory
if exist "bin\windows\kafka-server-start.bat" (
    echo ✓ Found Kafka installation in current directory
    goto :start_kafka
)

REM Check common Kafka installation locations
set "kafka_found=false"
set "kafka_path="

echo Searching for Kafka installation...

REM Check C:\kafka
if exist "C:\kafka\bin\windows\kafka-server-start.bat" (
    set "kafka_found=true"
    set "kafka_path=C:\kafka"
    echo ✓ Found Kafka at: C:\kafka
)

REM Check for kafka folders in current directory
for /d %%d in (kafka*) do (
    if exist "%%d\bin\windows\kafka-server-start.bat" (
        set "kafka_found=true"
        set "kafka_path=%CD%\%%d"
        echo ✓ Found Kafka at: %CD%\%%d
    )
)

REM Check Downloads folder
if exist "%USERPROFILE%\Downloads\kafka*" (
    for /d %%d in ("%USERPROFILE%\Downloads\kafka*") do (
        if exist "%%d\bin\windows\kafka-server-start.bat" (
            set "kafka_found=true"
            set "kafka_path=%%d"
            echo ✓ Found Kafka at: %%d
        )
    )
)

if "!kafka_found!"=="false" (
    echo.
    echo ✗ Kafka installation not found!
    echo.
    echo Please ensure you have:
    echo 1. Downloaded Kafka from https://kafka.apache.org/downloads
    echo 2. Extracted it to a directory
    echo 3. The directory contains: bin\windows\kafka-server-start.bat
    echo.
    echo Common locations to check:
    echo - C:\kafka\
    echo - %USERPROFILE%\Downloads\kafka_*
    echo - Current directory subfolders
    echo.
    dir /ad kafka*
    echo.
    pause
    exit /b 1
)

echo.
echo Using Kafka installation at: !kafka_path!
cd /d "!kafka_path!"

:start_kafka
echo.
echo Current working directory: %CD%
echo Verifying Kafka files...

if not exist "bin\windows\zookeeper-server-start.bat" (
    echo ✗ Missing: bin\windows\zookeeper-server-start.bat
    goto :missing_files
)

if not exist "bin\windows\kafka-server-start.bat" (
    echo ✗ Missing: bin\windows\kafka-server-start.bat
    goto :missing_files
)

if not exist "bin\windows\kafka-topics.bat" (
    echo ✗ Missing: bin\windows\kafka-topics.bat
    goto :missing_files
)

if not exist "config\zookeeper.properties" (
    echo ✗ Missing: config\zookeeper.properties
    goto :missing_files
)

if not exist "config\server.properties" (
    echo ✗ Missing: config\server.properties
    goto :missing_files
)

echo ✓ All required Kafka files found!
echo.

REM Check Java
echo Checking Java installation...
java -version
if !errorlevel! neq 0 (
    echo ✗ Java is not installed or not in PATH
    echo Please install Java 8 or later from https://adoptium.net/
    pause
    exit /b 1
)
echo ✓ Java is available!
echo.

REM Check for port conflicts
echo Checking for port conflicts...
netstat -an | findstr :2181 > nul
if !errorlevel! equ 0 (
    echo ⚠ Port 2181 is already in use! Zookeeper might already be running.
)

netstat -an | findstr :9092 > nul
if !errorlevel! equ 0 (
    echo ⚠ Port 9092 is already in use! Kafka might already be running.
)

REM Clean up any existing processes
echo Cleaning up any existing Kafka/Java processes...
taskkill /f /im java.exe 2>nul
echo Waiting for processes to close...
timeout /t 5 /nobreak >nul

REM Create log directories if they don't exist
if not exist "kafka-logs" mkdir kafka-logs
if not exist "zookeeper-logs" mkdir zookeeper-logs

echo Starting Zookeeper...
REM Set Java heap size for Zookeeper
set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
start "Zookeeper Server" cmd /c "cd /d %CD% && set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"

echo Waiting for Zookeeper to start (30 seconds)...
timeout /t 30 /nobreak >nul

REM Test Zookeeper connection
echo Testing Zookeeper connection...
netstat -an | findstr :2181 > nul
if !errorlevel! equ 0 (
    echo ✓ Zookeeper is listening on port 2181
) else (
    echo ✗ Zookeeper is not responding on port 2181
    echo Please check the Zookeeper window for errors
    pause
    exit /b 1
)

echo Starting Kafka Server...
REM Set Java heap size for Kafka
start "Kafka Server" cmd /c "cd /d %CD% && set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G && bin\windows\kafka-server-start.bat config\server.properties"

echo Waiting for Kafka Server to start (45 seconds)...
timeout /t 45 /nobreak >nul

echo.
echo Testing Kafka connection (attempt 1)...
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 --request-timeout-ms 10000
set kafka_test_result=!errorlevel!

if !kafka_test_result! neq 0 (
    echo First attempt failed, waiting 15 more seconds...
    timeout /t 15 /nobreak >nul
    echo Testing Kafka connection (attempt 2)...
    bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 --request-timeout-ms 15000
    set kafka_test_result=!errorlevel!
)

if !kafka_test_result! equ 0 (
    echo ✓ Kafka is responding!
) else (
    echo ✗ Kafka connection failed after multiple attempts
    echo Please check the Kafka Server window for errors
    echo Common issues:
    echo - Java OutOfMemoryError (increase heap size)
    echo - Port conflicts (check if other services use port 9092)
    echo - Missing write permissions for log directories
    echo.
    echo Attempting to show recent Kafka logs...
    if exist "logs\server.log" (
        echo --- Last 10 lines of server.log ---
        powershell "Get-Content logs\server.log -Tail 10"
    )
    pause
    exit /b 1
)

echo.
echo Creating topics...

echo Creating log-events topic...
bin\windows\kafka-topics.bat --create --topic log-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo Creating metrics-events topic...
bin\windows\kafka-topics.bat --create --topic metrics-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo Creating processed-analytics topic...
bin\windows\kafka-topics.bat --create --topic processed-analytics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo.
echo Listing created topics:
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

echo.
echo =====================================
echo Setup Complete!
echo =====================================
echo.
echo Kafka is running at: localhost:9092
echo Zookeeper is running at: localhost:2181
echo Working directory: %CD%
echo.
echo Keep the Zookeeper and Kafka windows open!
echo.
echo To stop Kafka properly, run:
echo   bin\windows\kafka-server-stop.bat
echo   bin\windows\zookeeper-server-stop.bat
echo.
pause
goto :end

:missing_files
echo.
echo ✗ Some required Kafka files are missing!
echo Please ensure you downloaded the correct Kafka binary distribution
echo from https://kafka.apache.org/downloads
echo.
echo You need the binary version (like kafka_2.13-3.x.x.tgz), not the source code
echo.
pause
exit /b 1

:end