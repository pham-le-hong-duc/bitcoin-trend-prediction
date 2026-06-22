# Hướng dẫn triển khai trên GCP VM

Tài liệu này hướng dẫn triển khai project dưới dạng Docker Compose trên máy ảo Google Compute Engine.
Thiết kế này chỉ public `Grafana`, còn `TimescaleDB`, `MinIO`, `Redpanda`, và `Airflow`
được giữ private phía sau VM. Đồng thời, cách chia profile cũng giúp tránh lãng phí tài nguyên
bằng cách không chạy `Airflow` và `Reddit` liên tục nếu chưa cần.

## 1. Kiến trúc đề xuất

Phần public:

- `Caddy` chạy trên cổng `80/443`
- `Grafana` nằm sau `Caddy`

Phần private bên trong VM hoặc Docker network:

- `TimescaleDB`
- `MinIO`
- `Redpanda`
- `Redpanda Console`
- `Airflow webserver`
- `Airflow scheduler`
- các producer và consumer

Nguyên tắc vận hành:

- chỉ public `Grafana`
- tất cả giao diện quản trị khác chỉ bind nội bộ
- khi cần truy cập UI private thì dùng SSH tunnel

## 2. Cấu hình máy GCP nên dùng

Bạn có thể chọn một trong hai mức sau:

1. `e2-standard-2` với `80-120 GB` balanced persistent disk
   Phù hợp cho `Grafana + TimescaleDB + MinIO + Redpanda + Binance realtime`.

2. `e2-standard-4` với `120-200 GB` balanced persistent disk
   Dùng khi cần backfill ban đầu nặng hơn, bật `Airflow`, hoặc bật `Reddit`.

Để tránh lãng phí tài nguyên, nên bắt đầu với `e2-standard-2` nếu mục tiêu chính là public dashboard.
Nếu giai đoạn nạp dữ liệu lịch sử chạy chậm, bạn có thể tắt VM, resize lên `e2-standard-4`,
chạy xong backfill nặng rồi hạ lại cấu hình.

## 3. Chuẩn bị VM

Tạo một VM Ubuntu và chỉ mở các cổng firewall sau:

- `22` cho SSH
- `80` cho HTTP
- `443` cho HTTPS

Không mở các cổng `3000`, `5433`, `9000`, `9001`, `8081`, hoặc `8082` ra internet.

Cài Docker và Docker Compose plugin:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```

Sau khi thêm user vào group `docker`, hãy logout rồi SSH vào lại.

## 4. Build và push application image

Chạy các lệnh này trên máy local:

```bash
docker build -t your-dockerhub-user/thesis-pipeline:latest -f docker/Dockerfile .
docker login
docker push your-dockerhub-user/thesis-pipeline:latest
```

Lưu ý:

- image chỉ chứa code ứng dụng và dependencies
- image không chứa data runtime của `MinIO` hoặc `TimescaleDB`
- `src/model/` được loại khỏi image để giảm dung lượng
- nếu cần Reddit thì model và cookies sẽ mount riêng trên VM khi bật profile `reddit`

## 5. Chép các file deploy lên VM

Clone repository trên VM:

```bash
git clone <your-repo-url> bitcoin-trend-prediction
cd bitcoin-trend-prediction
```

Tạo các thư mục host để lưu persistent data:

```bash
sudo mkdir -p /srv/bitcoin-trend-prediction/{redpanda,timescaledb,minio,grafana,caddy_data,caddy_config,airflow-postgres,seed-data}
sudo chown -R $USER:$USER /srv/bitcoin-trend-prediction
```

Tạo file môi trường:

```bash
cp deploy/.env.example deploy/.env
```

Chỉnh `deploy/.env` và thay toàn bộ password, key, domain mẫu bằng giá trị thật.

Sinh key an toàn cho Airflow:

```bash
python3 - <<'PY'
import base64
import secrets
print("AIRFLOW_FERNET_KEY=" + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())
print("AIRFLOW_WEBSERVER_SECRET_KEY=" + secrets.token_urlsafe(32))
PY
```

## 6. Khởi động stack chạy thường trực

Khởi động các service nền nên luôn online:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml up -d
```

Lệnh này sẽ chạy:

- `redpanda`
- `redpanda-console`
- `timescaledb`
- `minio`
- `grafana`
- `caddy`
- các init container
- Binance realtime producer
- MinIO Binance consumer
- TimescaleDB dashboard consumer

## 7. Nạp sẵn data vào MinIO để giảm backfill

Nếu bạn đã có sẵn file parquet hoặc raw object ở local, hãy copy chúng lên VM trước.

Cấu trúc thư mục seed nên là:

```text
/srv/bitcoin-trend-prediction/seed-data/
  binance/
    futures/
    spot/
  reddit/
    comments/
    submissions/
```

Sau đó chạy profile seed một lần:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml --profile seed run --rm minio-seed
```

Lệnh này sẽ mirror dữ liệu seed vào các bucket `binance` và `reddit` do `minio-init` đã tạo sẵn.

## 8. Chỉ bật Airflow khi cần chạy DAG

Các DAG hiện tại của bạn đang là chạy thủ công (`schedule_interval=None`), nên nếu bật Airflow 24/7 thì thường sẽ tốn CPU và RAM không cần thiết.
Hãy chỉ bật nó khi cần chạy `batch_minio` hoặc `batch_timescaledb`.

Bật Airflow:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml --profile airflow up -d
```

Trigger DAG từ VM:

```bash
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver airflow dags trigger batch_minio
docker exec airflow-webserver airflow dags trigger batch_timescaledb
```

Sau khi batch xong, có thể tắt Airflow để tiết kiệm tài nguyên:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml --profile airflow stop airflow-webserver airflow-scheduler postgres
```

## 9. Chỉ bật Reddit khi thực sự cần

Reddit cần model local và cookies, và các file này không được đóng gói sẵn trong Docker image.

Chuẩn bị các thư mục trên VM:

```bash
mkdir -p /srv/bitcoin-trend-prediction/reddit-models
mkdir -p /srv/bitcoin-trend-prediction/reddit-cookies
```

Copy vào đó:

- toàn bộ nội dung của thư mục local `src/model/`
- các file cookie Reddit cần thiết

Sau đó đặt các path này trong `deploy/.env`:

- `REDDIT_MODEL_DIR=/srv/bitcoin-trend-prediction/reddit-models`
- `REDDIT_COOKIES_DIR=/srv/bitcoin-trend-prediction/reddit-cookies`

Bật profile Reddit:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml --profile reddit up -d
```

Nếu bạn không cần sentiment chạy live, hãy để profile này tắt để tiết kiệm CPU và disk.

## 10. Mô hình truy cập public và private

Phần public:

- `https://your-domain`
- domain này do `Caddy` phục vụ và reverse proxy tới `Grafana`

Phần admin private dùng SSH tunnel:

```bash
ssh -L 8082:127.0.0.1:8082 -L 9001:127.0.0.1:9001 -L 8081:127.0.0.1:8081 your-user@your-vm-ip
```

Sau khi SSH:

- Airflow UI: `http://127.0.0.1:8082`
- MinIO console: `http://127.0.0.1:9001`
- Redpanda console: `http://127.0.0.1:8081`

Cách này giúp các giao diện quản trị vẫn private mà không cần dựng VPN riêng.

## 11. Quy trình update

Mỗi khi cần phát hành version mới:

1. build và push image tag mới lên Docker Hub
2. cập nhật `APP_IMAGE` trong `deploy/.env`
3. pull image mới và restart trên VM

Các lệnh:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml pull
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml up -d
```

Persistent data trong `/srv/bitcoin-trend-prediction` sẽ được giữ nguyên.

## 12. Kiểm tra sau khi chạy

Kiểm tra trạng thái service:

```bash
docker compose --env-file deploy/.env -f docker/docker-compose.prod.yml ps
```

Kiểm tra public dashboard:

```bash
curl -I https://your-domain
```

Kiểm tra các bảng dashboard đã có dữ liệu:

```bash
docker exec timescaledb psql -U admin -d base -c "SELECT COUNT(*) FROM dashboard.futures_klines_1m;"
docker exec timescaledb psql -U admin -d base -c "SELECT COUNT(*) FROM dashboard.futures_metrics_5m;"
```

## 13. Gợi ý tối ưu chi phí

Để tránh lãng phí tài nguyên:

- chỉ bật `Airflow` khi cần chạy DAG
- chỉ bật `Reddit` khi thực sự cần sentiment live
- nếu đã có history thì seed vào MinIO trước
- dùng `e2-standard-2` cho chế độ dashboard luôn online
- chỉ resize lên cấu hình cao hơn trong giai đoạn backfill nặng

## 14. Những điểm cần lưu ý

- repo hiện vẫn có các file cookie Reddit local; không nên public các file đó
- nếu muốn chạy đầy đủ sentiment pipeline live, bạn phải tự quản lý model và cookie trên VM
- hướng dẫn này giữ nguyên logic pipeline hiện tại, không refactor orchestration hay schedule của code hiện có
