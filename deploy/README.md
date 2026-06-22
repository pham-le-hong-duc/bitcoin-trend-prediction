# Production deploy on GCP VM

This deployment bundle is designed for a single Compute Engine VM where:

- `Grafana` is public through `Caddy` on `80/443`
- `Airflow`, `MinIO`, `TimescaleDB`, and `Redpanda` stay private on localhost-only ports
- the application code is pulled from Docker Hub via `APP_IMAGE`
- runtime data stays on VM Docker volumes instead of inside the image
- Grafana uses the dashboards already defined in `src/dashboard/dashboards`

## 1. Build and push the application image

Run these commands from the repository root on your build machine:

```bash
docker build -t your-dockerhub-user/thesis-pipeline:latest -f docker/Dockerfile .
docker push your-dockerhub-user/thesis-pipeline:latest
```

If you want immutable deploys, push a dated tag too, for example:

```bash
docker tag your-dockerhub-user/thesis-pipeline:latest your-dockerhub-user/thesis-pipeline:2026-06-22
docker push your-dockerhub-user/thesis-pipeline:2026-06-22
```

## 2. Recommended GCP setup

This recommendation is based on the current GCP Compute Engine documentation for general-purpose machine families, static external IPs, and VPC firewall rules:

- General-purpose machines: https://cloud.google.com/compute/docs/general-purpose-machines
- Static external IPs: https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address
- Firewall rules: https://cloud.google.com/firewall/docs/using-firewalls

Recommended starting point for this repo:

- VM type: `e2-standard-4`
- Disk: `pd-balanced`, `100 GB`
- OS: Ubuntu LTS
- Network: one static external IP

Why this is the lean starting point:

- it is enough for `Grafana + Airflow + TimescaleDB + MinIO + Redpanda + Binance realtime`
- it avoids paying for a larger VM before you confirm sustained load
- you can scale up to `e2-standard-8` if you later enable the Reddit pipeline and its NLP models

If your goal is only a public market dashboard, keep the `reddit` profile disabled to save RAM, CPU, and operational risk.

## 3. VM bootstrap

SSH into the VM and install Docker Engine with the Compose plugin, then copy or clone this repository to the VM.

From the repo root on the VM:

```bash
cd deploy
cp .env.example .env
```

Edit `.env` and set at least:

- `APP_IMAGE`
- `GRAFANA_DOMAIN`
- `ACME_EMAIL`
- all passwords
- `AIRFLOW_FERNET_KEY`
- `AIRFLOW_WEBSERVER_SECRET_KEY`

Before starting the stack, point your domain's `A` record to the VM static IP.

## 4. Public and private boundary

This compose file is intentionally split like this:

- Public: `Caddy -> Grafana`
- Private by Docker network only: `TimescaleDB`, `Postgres`, `Redpanda`
- Private by localhost-only port binding: `Airflow`, `MinIO`, `Redpanda Console`

On GCP, create firewall rules that allow only:

- `tcp:22` for SSH
- `tcp:80` for ACME HTTP challenge
- `tcp:443` for Grafana over HTTPS

Do not open `8081`, `8082`, `9000`, `9001`, `5433`, or `19092` to the internet.

## 5. Start the core stack

Pull images and start the always-on services:

```bash
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

At this point, Grafana, TimescaleDB, MinIO, Redpanda, and Airflow should all be up. Grafana will become public once Caddy finishes certificate issuance.

## 6. Seed MinIO with historical data

If you already have downloaded historical parquet files, copy them to:

- `deploy/seed-data/binance/...`
- `deploy/seed-data/reddit/...`

Expected example layout:

```text
deploy/seed-data/binance/futures/um/daily/aggTrades/BTCUSDT/2026-01-01.parquet
deploy/seed-data/binance/futures/um/daily/klines/BTCUSDT/1m/2026-01-01.parquet
deploy/seed-data/binance/spot/daily/klines/BTCUSDT/1m/2026-01-01.parquet
```

Then upload everything into MinIO:

```bash
docker compose -f docker-compose.prod.yml --profile seed run --rm minio-seed
```

This avoids repeated heavy backfill downloads on the VM.

## 7. Build dashboard tables in TimescaleDB

Trigger the Airflow DAGs from the VM after MinIO contains history:

```bash
docker compose -f docker-compose.prod.yml exec airflow-webserver airflow dags trigger batch_minio
docker compose -f docker-compose.prod.yml exec airflow-webserver airflow dags trigger batch_timescaledb
docker compose -f docker-compose.prod.yml exec airflow-webserver airflow dags trigger dashboard_prediction
```

If you already seeded MinIO completely and do not want to download more Binance history, skip `batch_minio` and run only:

```bash
docker compose -f docker-compose.prod.yml exec airflow-webserver airflow dags trigger batch_timescaledb
docker compose -f docker-compose.prod.yml exec airflow-webserver airflow dags trigger dashboard_prediction
```

## 8. Start realtime ingestion

For the lighter market-only deployment:

```bash
docker compose -f docker-compose.prod.yml --profile realtime up -d
```

This enables:

- Binance producer
- MinIO Binance consumer
- TimescaleDB dashboard consumer

## 9. Optional Reddit profile

The Reddit pipeline is intentionally optional because it costs more RAM/CPU and depends on extra runtime assets not embedded in the Docker image.

Before enabling it, place these directories under `deploy/`:

- `reddit-models/`
- `reddit-cookies/`

These runtime folders are intentionally ignored by git and should be copied to the VM manually only when needed.

Then set `REDDIT_PROXY` in `.env` if your crawler needs a proxy, and start:

```bash
docker compose -f docker-compose.prod.yml --profile reddit up -d
```

## 10. Operations

Useful commands:

```bash
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs -f grafana
docker compose -f docker-compose.prod.yml logs -f airflow-scheduler
docker compose -f docker-compose.prod.yml logs -f streaming-producer-binance
```

To reach private admin interfaces securely, SSH tunnel from your local machine:

```bash
ssh -L 8082:127.0.0.1:8082 -L 9001:127.0.0.1:9001 -L 8081:127.0.0.1:8081 USER@VM_IP
```

Then open locally:

- Airflow: `http://127.0.0.1:8082`
- MinIO Console: `http://127.0.0.1:9001`
- Redpanda Console: `http://127.0.0.1:8081`

## 11. Updating the application

Push a new image tag to Docker Hub, update `APP_IMAGE` in `.env`, then:

```bash
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

Because runtime state lives in Docker volumes, your MinIO, Grafana, and TimescaleDB data will remain on the VM.
