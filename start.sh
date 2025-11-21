#!/bin/bash

# SENG550-A3 Quick Start Script
# This script helps you get the entire system up and running

echo "=== SENG550-A3 Data Engineering Pipeline Setup ==="
echo

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Navigate to project directory
cd "$(dirname "$0")"

echo "📁 Current directory: $(pwd)"

# Step 1: Start Docker environment
echo
echo "🐳 Starting Docker environment..."
cd docker
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "✅ Docker containers started successfully"
else
    echo "❌ Failed to start Docker containers"
    exit 1
fi

cd ..

# Wait for services to be ready
echo
echo "⏳ Waiting for services to initialize..."
sleep 30

# Step 2: Check service health
echo
echo "🔍 Checking service health..."

# Check Spark Master
if curl -s http://localhost:8080 > /dev/null; then
    echo "✅ Spark Master UI accessible at http://localhost:8080"
else
    echo "⚠️  Spark Master UI not yet ready at http://localhost:8080"
fi

# Check Airflow
if curl -s http://localhost:8081 > /dev/null; then
    echo "✅ Airflow Web UI accessible at http://localhost:8081"
    echo "   Login: admin / admin"
else
    echo "⚠️  Airflow Web UI not yet ready at http://localhost:8081"
fi

# Check Redis
if docker exec redis redis-cli ping > /dev/null 2>&1; then
    echo "✅ Redis is accessible"
else
    echo "⚠️  Redis not yet ready"
fi

echo
echo "📋 System Status Summary:"
echo "   - Spark Master: http://localhost:8080"
echo "   - Airflow Web UI: http://localhost:8081 (admin/admin)"
echo "   - Redis: localhost:6379"
echo
echo "🚀 Next Steps:"
echo "   1. Wait 2-3 minutes for all services to fully initialize"
echo "   2. Access Airflow at http://localhost:8081"
echo "   3. Enable the DAGs:"
echo "      - incremental_processing_dag (runs every 4 seconds)"
echo "      - ml_training_dag (runs every 20 seconds)" 
echo "      - redis_cache_dag (runs every 20 seconds)"
echo "   4. Monitor the processing in Airflow UI"
echo
echo "📖 For detailed instructions, see README_IMPLEMENTATION.md"
echo

# Optional: Run initial data split if not done yet
if [ ! -f "part1/data/raw/0/orders_0.csv" ]; then
    echo "📊 Running initial data split..."
    python part0_data_split.py
    if [ $? -eq 0 ]; then
        echo "✅ Data split completed"
    else
        echo "⚠️  Data split failed - you may need to run it manually"
    fi
fi

echo "🎉 Setup complete! Your data engineering pipeline is ready."