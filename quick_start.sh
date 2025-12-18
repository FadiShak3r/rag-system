#!/bin/bash

# Quick start script for MySQL RAG System

echo "MySQL RAG System - Quick Start"
echo "=============================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo ".env file not found!"
    echo "Please create a .env file with your configuration:"
    echo ""
    echo "MYSQL_HOST="
    echo "MYSQL_PORT="
    echo "MYSQL_USER="
    echo "MYSQL_PASSWORD="
    echo "MYSQL_DATABASE="
    echo "OPENAI_API_KEY="
    echo ""
    exit 1
fi

echo "✓ .env file found"
echo ""

# Check if dependencies are installed
if ! python3 -c "import openai" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
    echo ""
fi

echo "Starting initial indexing..."
python3 indexer.py

echo ""
echo "✓ Setup complete!"
echo ""
echo "You can now start the chatbot with:"
echo "  python3 app.py"
echo ""
echo "Then open your browser and go to: http://localhost:4100"
echo ""

