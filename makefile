# Makefile for Data Pipeline Project
# Usage: make <target>

# Workflow order:
# 0. build              -> Build Docker images
# 1. infra-up           -> Start infrastructure and initialize services (docker/docker-compose.infrastructure.yml)
# 2. consumer.minio-up  -> Start MinIO consumer (docker/docker-compose.streaming.consumer.minio.yml)
# 3. producer-up        -> Start streaming producer and wait for reddit first-run status
# 4. batch_minio        -> Trigger batch_minio DAG (dags/batch_minio.py)
# 5. consumer.timescaledb-up -> Start TimescaleDB dashboard consumer (docker/docker-compose.streaming.consumer.timescaledb.yml)
# 6. batch_timescaledb  -> Trigger batch_timescaledb DAG (dags/batch_timescaledb.py)

.PHONY: infra-up producer-up consumer.minio-up batch_minio consumer.timescaledb-up batch_timescaledb dashboard_prediction verify up down start-all build

# Build target
build:
	@echo "========================================================="
	@echo "STEP 0: Building Docker images..."
	@echo "========================================================="
	@echo "Building shared application image: thesis-pipeline:latest"
	@echo "Note: Docker will cache layers for faster rebuilds"
	docker build -t thesis-pipeline:latest -f docker/Dockerfile .
	@echo "[OK] Shared application image built successfully!"

# Infrastructure targets
infra-up:
	@echo "========================================================="
	@echo "STEP 1: Starting infrastructure..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.infrastructure.yml up -d --force-recreate
	@echo "[OK] Infrastructure started!"



# MinIO consumer targets
consumer.minio-up:
	@echo "========================================================="
	@echo "STEP 2: Starting MinIO consumer..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.infrastructure.yml -f docker/docker-compose.streaming.consumer.minio.yml up -d --force-recreate streaming-consumer-minio-binance streaming-consumer-minio-reddit
	@echo "[OK] MinIO consumer started!"

# Producer targets
producer-up:
	@echo "========================================================="
	@echo "STEP 3: Starting producers..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.infrastructure.yml -f docker/docker-compose.streaming.producer.yml up -d --force-recreate streaming-producer-binance streaming-producer-reddit
	docker-compose -f docker/docker-compose.infrastructure.yml exec -T airflow-webserver python -m src.wait.reddit_status
	@echo "[OK] Producers started!"
	

# Airflow MinIO DAG targets
batch_minio:
	@echo "========================================================="
	@echo "STEP 4: Running DAG batch_minio..."
	@echo "========================================================="
	@powershell -Command "& { \$$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'); docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags trigger -e \$$timestamp batch_minio 2>&1 | Select-String -Pattern 'Created|triggered' -CaseSensitive; do { Start-Sleep -Seconds 5; \$$status = (docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags state batch_minio \$$timestamp 2>&1 | Select-String -Pattern 'queued|running|success|failed' -CaseSensitive).ToString().Trim(); } while (\$$status -match 'running|queued'); if (\$$status -ne 'success') { Write-Host ('[ERROR] DAG Failed: ' + \$$status) -ForegroundColor Red; exit 1 } }"
	@echo "[OK] DAG batch_minio finished!"

# TimescaleDB consumer targets
consumer.timescaledb-up:
	@echo "========================================================="
	@echo "STEP 5: Starting TimescaleDB consumer..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.infrastructure.yml -f docker/docker-compose.streaming.consumer.timescaledb.yml up -d --force-recreate streaming-consumer-timescaledb-dashboard
	@echo "[OK] TimescaleDB consumer started!"

# Airflow TimescaleDB DAG targets
batch_timescaledb:
	@echo "========================================================="
	@echo "STEP 6: Running DAG batch_timescaledb..."
	@echo "========================================================="
	@powershell -Command "& { \$$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'); docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags trigger -e \$$timestamp batch_timescaledb 2>&1 | Select-String -Pattern 'Created|triggered' -CaseSensitive; do { Start-Sleep -Seconds 5; \$$status = (docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags state batch_timescaledb \$$timestamp 2>&1 | Select-String -Pattern 'queued|running|success|failed' -CaseSensitive).ToString().Trim(); } while (\$$status -match 'running|queued'); if (\$$status -ne 'success') { Write-Host ('[ERROR] DAG Failed: ' + \$$status) -ForegroundColor Red; exit 1 } }"
	@echo "[OK] DAG batch_timescaledb finished!"

dashboard_prediction:
	@echo "========================================================="
	@echo "Running DAG dashboard_prediction..."
	@echo "========================================================="
	@powershell -Command "& { \$$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'); docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags trigger -e \$$timestamp dashboard_prediction 2>&1 | Select-String -Pattern 'Created|triggered' -CaseSensitive; do { Start-Sleep -Seconds 5; \$$status = (docker exec -e PYTHONWARNINGS=ignore airflow-webserver airflow dags state dashboard_prediction \$$timestamp 2>&1 | Select-String -Pattern 'queued|running|success|failed' -CaseSensitive).ToString().Trim(); } while (\$$status -match 'running|queued'); if (\$$status -ne 'success') { Write-Host ('[ERROR] DAG Failed: ' + \$$status) -ForegroundColor Red; exit 1 } }"
	@echo "[OK] DAG dashboard_prediction finished!"

verify:
	@echo "========================================================="
	@echo "STEP 7: Verifying stack health and dashboard data..."
	@echo "========================================================="
	@powershell -Command "& { \$$containers = @('timescaledb','grafana','airflow-webserver','airflow-scheduler','streaming.producer.binance','streaming.producer.reddit','streaming.consumer.timescaledb.dashboard'); foreach (\$$name in \$$containers) { \$$state = (docker inspect -f '{{.State.Status}}' \$$name 2>\$$null); if (\$$state -ne 'running') { Write-Host ('[ERROR] Container not running: ' + \$$name) -ForegroundColor Red; exit 1 } }; \$$checks = @(\"SELECT 'futures_index_price_klines_1m' AS dataset, MAX(close_time) AS latest_ts, COUNT(*) AS row_count FROM dashboard.futures_index_price_klines_1m\",\"SELECT 'futures_metrics_5m' AS dataset, MAX(create_time) AS latest_ts, COUNT(*) AS row_count FROM dashboard.futures_metrics_5m\",\"SELECT 'sentiment_1h' AS dataset, MAX(create_time) AS latest_ts, COUNT(*) AS row_count FROM dashboard.sentiment_1h\",\"SELECT 'predictions_1h' AS dataset, MAX(target_time) AS latest_ts, COUNT(*) AS row_count FROM dashboard.predictions_1h\"); foreach (\$$sql in \$$checks) { docker exec timescaledb psql -U admin -d base -t -A -c \$$sql; if (\$$LASTEXITCODE -ne 0) { Write-Host '[ERROR] Verification query failed' -ForegroundColor Red; exit 1 } } }"
	@echo "[OK] Stack verification finished!"

# Combined operations
up: infra-up consumer.minio-up producer-up batch_minio consumer.timescaledb-up batch_timescaledb dashboard_prediction verify

down:
	@docker-compose -f docker/docker-compose.infrastructure.yml -f docker/docker-compose.streaming.producer.yml -f docker/docker-compose.streaming.consumer.minio.yml -f docker/docker-compose.streaming.consumer.timescaledb.yml down

start-all: build up
