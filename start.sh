#!/bin/bash

echo "=================================================="
echo "  Parking Division API - Startup Script"
echo "=================================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating..."
    python -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate || venv\Scripts\activate

# Check if dependencies are installed
echo "Checking dependencies..."
pip install -q -r requirements.txt

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found!"
    echo "Please copy .env.example to .env and configure your database settings."
    exit 1
fi

# Check if database is seeded
echo "Database setup..."
python scripts/seed_data.py

echo ""
echo "=================================================="
echo "  Starting FastAPI Application"
echo "=================================================="
echo ""
echo "Application will be available at:"
echo "  - Web Interface: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs"
echo ""

# Start the application
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
