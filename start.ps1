# SENG550-A3 Data Engineering Pipeline - Quick Start for Windows

Write-Host "=== SENG550-A3 Data Engineering Pipeline Setup ===" -ForegroundColor Green
Write-Host ""

# Check if Docker is running
try {
    docker info | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Navigate to project directory
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectDir
Write-Host "📁 Current directory: $(Get-Location)" -ForegroundColor Blue

# Step 1: Start Docker environment
Write-Host ""
Write-Host "🐳 Starting Docker environment..." -ForegroundColor Yellow
Set-Location "docker"

docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Docker containers started successfully" -ForegroundColor Green
} else {
    Write-Host "❌ Failed to start Docker containers" -ForegroundColor Red
    exit 1
}

Set-Location ".."

# Wait for services to be ready
Write-Host ""
Write-Host "⏳ Waiting for services to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Step 2: Check service health
Write-Host ""
Write-Host "🔍 Checking service health..." -ForegroundColor Yellow

# Check Spark Master
try {
    $sparkResponse = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ Spark Master UI accessible at http://localhost:8080" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Spark Master UI not yet ready at http://localhost:8080" -ForegroundColor Yellow
}

# Check Airflow
try {
    $airflowResponse = Invoke-WebRequest -Uri "http://localhost:8081" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ Airflow Web UI accessible at http://localhost:8081" -ForegroundColor Green
    Write-Host "   Login: admin / admin" -ForegroundColor Cyan
} catch {
    Write-Host "⚠️  Airflow Web UI not yet ready at http://localhost:8081" -ForegroundColor Yellow
}

# Check Redis
try {
    $redisCheck = docker exec redis redis-cli ping 2>$null
    if ($redisCheck -eq "PONG") {
        Write-Host "✅ Redis is accessible" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Redis not yet ready" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠️  Redis not yet ready" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "📋 System Status Summary:" -ForegroundColor Cyan
Write-Host "   - Spark Master: http://localhost:8080"
Write-Host "   - Airflow Web UI: http://localhost:8081 (admin/admin)"
Write-Host "   - Redis: localhost:6379"
Write-Host ""
Write-Host "🚀 Next Steps:" -ForegroundColor Green
Write-Host "   1. Wait 2-3 minutes for all services to fully initialize"
Write-Host "   2. Access Airflow at http://localhost:8081"
Write-Host "   3. Enable the DAGs:"
Write-Host "      - incremental_processing_dag (runs every 4 seconds)"
Write-Host "      - ml_training_dag (runs every 20 seconds)" 
Write-Host "      - redis_cache_dag (runs every 20 seconds)"
Write-Host "   4. Monitor the processing in Airflow UI"
Write-Host ""
Write-Host "📖 For detailed instructions, see README_IMPLEMENTATION.md" -ForegroundColor Cyan
Write-Host ""

# Optional: Run initial data split if not done yet
if (!(Test-Path "part1\data\raw\0\orders_0.csv")) {
    Write-Host "📊 Running initial data split..." -ForegroundColor Yellow
    python part0_data_split.py
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Data split completed" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Data split failed - you may need to run it manually" -ForegroundColor Yellow
    }
}

Write-Host "🎉 Setup complete! Your data engineering pipeline is ready." -ForegroundColor Green

# Test the setup
Write-Host ""
Write-Host "🔍 Running system health check..." -ForegroundColor Yellow
python test_setup.py

Write-Host ""
Write-Host "🌐 Opening web interfaces..." -ForegroundColor Blue
Start-Process "http://localhost:8080"  # Spark UI
Start-Process "http://localhost:8081"  # Airflow UI