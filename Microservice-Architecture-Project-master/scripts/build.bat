@echo off
echo Building Real-Time Analytics Engine...

rem Build Java Producer
echo Building Java Producer...
if exist java-producer (
    cd java-producer
    call mvn clean compile package
    if %ERRORLEVEL% NEQ 0 (
        echo Java build failed!
        pause
        exit /b 1
    )
    cd ..
) else (
    echo Java producer directory not found!
    pause
    exit /b 1
)

echo -------------------------------------
echo Setting up Python 3.11 virtual environment...
echo -------------------------------------

rem Create virtual environment if it doesn't exist
if not exist venv (
    python -m venv venv
    if %ERRORLEVEL% NEQ 0 (
        echo Failed to create virtual environment!
        pause
        exit /b 1
    )
)

rem Activate virtual environment
call venv\Scripts\activate.bat

rem Install Python dependencies for processor
echo Installing Python dependencies for processor...
if exist python-processor\requirements.txt (
    pip install -r python-processor\requirements.txt
    if %ERRORLEVEL% NEQ 0 (
        echo Processor dependencies installation failed!
        pause
        exit /b 1
    )
) else (
    echo python-processor\requirements.txt not found, installing basic dependencies...
    pip install kafka-python==2.0.2 pandas==2.0.3 numpy==1.24.3
)

echo -------------------------------------
echo Installing Python dependencies for API server...
if exist api-server\requirements.txt (
    pip install -r api-server\requirements.txt
    if %ERRORLEVEL% NEQ 0 (
        echo API server dependencies installation failed!
        pause
        exit /b 1
    )
) else (
    echo api-server\requirements.txt not found, installing basic dependencies...
    pip install flask==2.3.2 flask-cors==4.0.0 plotly==5.15.0 requests==2.31.0
)

echo -------------------------------------
echo Build completed successfully!
echo -------------------------------------
echo.
echo To start the system, run: start-system.bat
echo Make sure Kafka is running first: start-kafka.bat
echo.
pause