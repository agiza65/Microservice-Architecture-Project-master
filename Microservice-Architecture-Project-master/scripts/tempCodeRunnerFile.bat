@echo off
setlocal enabledelayedexpansion

echo =====================================
echo Kafka Directory Locator and Setup
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
    dir /ad kafka* 2>nul
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
java -version 2>nul
if !errorlevel! neq 0 (
    echo ✗ Java is not installed or not in PATH
    echo Please install Java 8 or later from https://adoptium.net/
    pause
    exit /b 1
)
echo ✓ Java is available!
echo.

REM Clean up any existing processes
echo Cleaning up any existing Java processes...
taskkill /f /im java.exe 2>nul
timeout /t 3

echo Starting Zookeeper...
start "Zookeeper Server" cmd /c "cd /d %CD% && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"

echo Waiting for Zookeeper to start...
timeout /t 15

echo Starting Kafka Server...
start "Kafka Server" cmd /c "cd /d %CD% && bin\windows\kafka-server-start.bat config\server.properties"

echo Waiting for Kafka Server to start...
timeout /t 20

echo.
echo Testing Kafka connection...
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 2>nul
if !errorlevel! equ 0 (
    echo ✓ Kafka is responding!
) else (
    echo ⚠ Kafka connection test failed, but this might be normal during startup
    echo   Please check the Kafka and Zookeeper windows for errors
)

echo.
echo Creating topics...

bin\windows\kafka-topics.bat --create --topic log-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin\windows\kafka-topics.bat --create --topic metrics-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin\windows\kafka-topics.bat --create --topic processed-analytics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

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