@echo off
echo Starting Real-Time Analytics Engine...

rem Check if Kafka is running
echo Checking Kafka status...
netstat -an | findstr "9092"
if %ERRORLEVEL% NEQ 0 (
    echo Kafka is not running. Please start Kafka first using start-kafka.bat
    pause
    exit /b 1
)

rem Get the parent directory (AnalysisWebApp root)
set ROOT_DIR=%~dp0..
echo Root directory: %ROOT_DIR%

rem Check if virtual environment exists
if not exist "%ROOT_DIR%\venv" (
    echo Virtual environment not found. Please run build.bat first.
    pause
    exit /b 1
)

rem Wait a bit longer for Kafka to be fully ready
echo Waiting for Kafka to be fully ready...
ping 127.0.0.1 -n 11 > nul

rem Test Kafka connectivity before starting components
echo Testing Kafka connectivity...
cd /d "%ROOT_DIR%"
C:\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
if %ERRORLEVEL% NEQ 0 (
    echo Kafka is not responding properly. Please check Kafka and Zookeeper windows for errors.
    pause
    exit /b 1
)

rem Start Java Producer
echo Starting Java Data Producer...
if exist "%ROOT_DIR%\java-producer\target\realtime-analytics-1.0.0.jar" (
    echo Trying to run JAR file...
    start "Java Producer" cmd /k "cd /d "%ROOT_DIR%\java-producer" && java -jar target\realtime-analytics-1.0.0.jar"
) else if exist "%ROOT_DIR%\java-producer\target\classes" (
    echo Running from compiled classes...
    start "Java Producer" cmd /k "cd /d "%ROOT_DIR%\java-producer" && java -cp "target\classes;target\dependency\*;lib\*" com.analytics.producer.DataProducer"
) else (
    echo Java producer not built. Attempting to compile...
    cd /d "%ROOT_DIR%\java-producer"
    if exist "pom.xml" (
        mvn clean compile exec:java -Dexec.mainClass="com.analytics.producer.DataProducer" -Dexec.args="&" > nul
        if %ERRORLEVEL% EQU 0 (
            echo Java producer started via Maven
        ) else (
            echo Failed to start Java producer. Please run build.bat first.
            pause
        )
    ) else (
        echo Maven project not found. Please run build.bat first.
        pause
        exit /b 1
    )
)

echo Waiting for Java producer to initialize...
ping 127.0.0.1 -n 11 > nul

rem Start Python Processor with better error handling
echo Starting Python Real-Time Processor...
if exist "%ROOT_DIR%\python-processor\src\realtime_processor.py" (
    start "Python Processor" cmd /k "cd /d "%ROOT_DIR%" && venv\Scripts\activate && cd python-processor\src && echo Starting Python processor... && python realtime_processor.py"
) else (
    echo Python processor not found: %ROOT_DIR%\python-processor\src\realtime_processor.py
    pause
)

echo Waiting for Python processor to initialize...
ping 127.0.0.1 -n 11 > nul

rem Start API Server with better error handling
echo Starting API Server and Dashboard...
if exist "%ROOT_DIR%\api-server\src\api_server.py" (
    start "API Server" cmd /k "cd /d "%ROOT_DIR%" && venv\Scripts\activate && cd api-server\src && echo Starting API server... && python api_server.py"
) else (
    echo API server not found: %ROOT_DIR%\api-server\src\api_server.py
    pause
)

echo Waiting for API server to start...
ping 127.0.0.1 -n 16 > nul

rem Open dashboard in browser
echo Opening dashboard in browser...
start http://localhost:5000

echo System startup initiated!
echo.
echo Components should be starting:
echo - Kafka (localhost:9092)
echo - Java Data Producer (generating sample data)
echo - Python Real-Time Processor (processing and aggregating)
echo - API Server and Dashboard (http://localhost:5000)
echo.
echo Press any key to view system status...
pause

rem Show system status
echo.
echo =================================
echo        SYSTEM STATUS
echo =================================
echo.

rem Check Kafka
echo Checking Kafka...
C:\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
if %ERRORLEVEL% EQU 0 (
    echo [OK] Kafka is running and responding
) else (
    echo [ERROR] Kafka is not responding
)

rem Check Java Producer
tasklist /FI "IMAGENAME eq java.exe" | findstr /I "java.exe"
if %ERRORLEVEL% EQU 0 (
    echo [OK] Java processes are running
) else (
    echo [ERROR] Java processes are not running
)

rem Check Python processes
tasklist /FI "IMAGENAME eq python.exe" | findstr /I "python.exe"
if %ERRORLEVEL% EQU 0 (
    echo [OK] Python processes are running
) else (
    echo [ERROR] Python processes are not running
)

rem Check API Server with timeout
echo Checking API Server...
ping 127.0.0.1 -n 6 > nul
curl -s http://localhost:5000/api/health
if %ERRORLEVEL% EQU 0 (
    echo [OK] API Server is responding
) else (
    echo [WARNING] API Server may still be starting up
)

echo.
echo Dashboard URL: http://localhost:5000
echo API Endpoint: http://localhost:5000/api/metrics
echo Health Check: http://localhost:5000/api/health
echo.
echo Check the individual component windows for detailed status and errors.
echo To stop the system, close all the opened command windows.
pause