#!/bin/bash
echo "==================================================="
echo "Starting Phantom V2 Local Environment..."
echo "==================================================="

# Start Postgres if docker-compose is available
if command -v docker-compose &> /dev/null
then
    echo "Starting local PostgreSQL via Docker Compose..."
    docker-compose up -d
    echo "Waiting 5 seconds for database to start..."
    sleep 5
else
    echo "Docker Compose not found. Please ensure you have a local PostgreSQL running on port 5432."
fi

# Set up .env if missing
if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
fi

# Check if DATABASE_URL is set
if ! grep -q "DATABASE_URL=postgresql" .env; then
    echo "Setting default local DATABASE_URL in .env..."
    echo "" >> .env
    echo 'DATABASE_URL="postgresql://postgres:postgres@localhost:5432/phantom"' >> .env
fi

# Install dependencies
echo "Installing dependencies..."
python3 -m pip install -r requirements.txt

# Open browser
echo "Launching dashboard..."
if command -v xdg-open &> /dev/null; then
    (sleep 2 && xdg-open http://localhost:8000) &
elif command -v open &> /dev/null; then
    (sleep 2 && open http://localhost:8000) &
else
    echo "Please open http://localhost:8000 in your browser manually."
fi

# Run the bot
echo "Running main.py..."
python3 main.py
