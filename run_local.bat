@echo off
echo ===================================================
echo Starting Phantom V2 Local Environment...
echo ===================================================

:: Check if docker-compose exists and start Postgres
where docker-compose >nul 2>nul
if %ERRORLEVEL% equ 0 (
    echo Starting local PostgreSQL via Docker Compose...
    docker-compose up -d
    echo Waiting 5 seconds for database to start...
    timeout /t 5 /nobreak >nul
) else (
    echo Docker Compose not found. Please ensure you have a local PostgreSQL running on port 5432.
)

:: Set up .env if missing
if not exist .env (
    echo Creating .env from .env.example...
    copy .env.example .env
)

:: Removed aggressive findstr that corrupts DATABASE_URL

:: Ensure python packages are installed
echo Installing dependencies...
python -m pip install -r requirements.txt

:: Open browser after 3 seconds in parallel
echo Launching dashboard in browser...
start "" http://localhost:8000

:: Start the bot
echo Running main.py...
python main.py
