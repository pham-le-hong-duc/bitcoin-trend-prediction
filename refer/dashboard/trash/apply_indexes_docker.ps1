# ============================================================================
# PowerShell Script: Apply Database Optimization via Docker
# ============================================================================

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host " Applying Database Optimization (Indexes) via Docker" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Load environment variables from .env
$envFile = "../../.env"
if (Test-Path $envFile) {
    Write-Host "Loading environment variables from .env..." -ForegroundColor Yellow
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
    Write-Host "✅ Environment variables loaded" -ForegroundColor Green
} else {
    Write-Host "⚠️  Warning: .env file not found at $envFile" -ForegroundColor Yellow
}

# Get database connection info
$DB_HOST = "timescaledb"  # Use container name
$DB_PORT = "5432"          # Internal port
$DB_NAME = $env:TIMESCALE_DB
$DB_USER = $env:TIMESCALE_USER
$DB_PASSWORD = $env:TIMESCALE_PASSWORD

if (-not $DB_NAME) { $DB_NAME = "okx" }
if (-not $DB_USER) { $DB_USER = "okx_user" }
if (-not $DB_PASSWORD) { $DB_PASSWORD = "okx_password" }

Write-Host ""
Write-Host "Database Connection Info:" -ForegroundColor Cyan
Write-Host "  Host:     $DB_HOST (via Docker network)" -ForegroundColor White
Write-Host "  Port:     $DB_PORT" -ForegroundColor White
Write-Host "  Database: $DB_NAME" -ForegroundColor White
Write-Host "  User:     $DB_USER" -ForegroundColor White
Write-Host ""

# SQL file path
$SQL_FILE = "setup_index.sql"

if (-not (Test-Path $SQL_FILE)) {
    Write-Host "❌ ERROR: SQL file not found: $SQL_FILE" -ForegroundColor Red
    exit 1
}

Write-Host "SQL File: $SQL_FILE" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is available
try {
    docker --version | Out-Null
    Write-Host "✅ Docker found" -ForegroundColor Green
} catch {
    Write-Host "❌ ERROR: Docker not found!" -ForegroundColor Red
    exit 1
}

# Find TimescaleDB container
Write-Host "Finding TimescaleDB container..." -ForegroundColor Yellow
$container = docker ps --filter "name=timescaledb" --format "{{.Names}}" | Select-Object -First 1

if (-not $container) {
    Write-Host "❌ ERROR: TimescaleDB container not found or not running!" -ForegroundColor Red
    Write-Host "   Please start the container first:" -ForegroundColor Yellow
    Write-Host "   docker-compose -f docker/docker-compose.infrastructure.yml up -d" -ForegroundColor White
    exit 1
}

Write-Host "✅ Found container: $container" -ForegroundColor Green
Write-Host ""
Write-Host "----------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host " Applying optimization via Docker..." -ForegroundColor Yellow
Write-Host "----------------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ""
Write-Host "⚠️  WARNING: This may take several minutes depending on table sizes!" -ForegroundColor Yellow
Write-Host ""

# Copy SQL file to container
Write-Host "Copying SQL file to container..." -ForegroundColor Yellow
docker cp $SQL_FILE "${container}:/tmp/setup_index.sql"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ ERROR: Failed to copy SQL file to container" -ForegroundColor Red
    exit 1
}

Write-Host "✅ SQL file copied" -ForegroundColor Green
Write-Host ""

# Execute SQL via docker exec
Write-Host "Executing optimization SQL (this may take a while)..." -ForegroundColor Yellow
Write-Host ""

$env:PGPASSWORD = $DB_PASSWORD
docker exec -e PGPASSWORD=$DB_PASSWORD $container psql -h localhost -U $DB_USER -d $DB_NAME -f /tmp/setup_index.sql

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================================================" -ForegroundColor Green
    Write-Host " ✅ Database optimization applied successfully!" -ForegroundColor Green
    Write-Host "============================================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Total indexes created: 26" -ForegroundColor Cyan
    Write-Host "  - Spot trades:        5 indexes (ts_ms only)" -ForegroundColor White
    Write-Host "  - Perpetual trades:   5 indexes (ts_ms only)" -ForegroundColor White
    Write-Host "  - Index klines:       5 indexes (ts_ms only)" -ForegroundColor White
    Write-Host "  - Mark klines:        5 indexes (ts_ms only)" -ForegroundColor White
    Write-Host "  - Orderbook:          5 indexes (ts_ms only)" -ForegroundColor White
    Write-Host "  - Funding rate:       1 index (funding_time)" -ForegroundColor White
    Write-Host ""
    Write-Host "Note: timestamp_dt indexes NOT created (1-1 mapping with ts_ms)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Expected improvements:" -ForegroundColor Yellow
    Write-Host "  ✅ JOIN queries: 10-50x faster (from 12s to ~1-2s)" -ForegroundColor White
    Write-Host "  ✅ Time range queries: 5-20x faster" -ForegroundColor White
    Write-Host "  ✅ ORDER BY ts_ms: 10x faster" -ForegroundColor White
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Test query performance in your dashboard" -ForegroundColor White
    Write-Host "  2. Monitor index usage with pg_stat_user_indexes" -ForegroundColor White
    Write-Host ""
    
    # Cleanup
    Write-Host "Cleaning up..." -ForegroundColor Yellow
    docker exec $container rm /tmp/setup_index.sql
    Write-Host "✅ Cleanup complete" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "❌ ERROR: Failed to apply optimization (exit code: $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host ""
