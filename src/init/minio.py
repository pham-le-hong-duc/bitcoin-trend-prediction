from minio import Minio
from minio.error import S3Error
import sys
import time

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

BUCKETS = {
    "binance": [
        # "futures/um/monthly/fundingRate/BTCUSDT/",
        "futures/um/daily/aggTrades/BTCUSDT/",
        "futures/um/daily/klines/BTCUSDT/1m/",
        # "futures/um/daily/indexPriceKlines/BTCUSDT/1m/",
        # "futures/um/daily/markPriceKlines/BTCUSDT/1m/",
        "futures/um/daily/premiumIndexKlines/BTCUSDT/1m/",
        "futures/um/daily/metrics/BTCUSDT/",
        # "spot/daily/aggTrades/BTCUSDT/",
        "spot/daily/klines/BTCUSDT/1m/",
    ],
    "reddit": [
        "comments/",
        "submissions/",
    ],
}

def wait_for_minio(client, max_retries=30):
    for i in range(max_retries):
        try:
            client.list_buckets()
            return True
        except Exception as e:
            time.sleep(2)
    return False

def create_bucket(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        return True
    except S3Error as e:
        print(f"Error creating bucket: {e}", file=sys.stderr)
        return False

def create_folder_structure(client, bucket_name, folders):
    from io import BytesIO
    
    for folder in folders:
        try:
            object_name = f"{folder}.keep"
            client.put_object(
                bucket_name,
                object_name,
                BytesIO(b""),
                length=0
            )
        except S3Error as e:
            print(f"Error creating {folder}: {e}", file=sys.stderr)

def main():
    print("MINIO INITIALIZATION")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    if not wait_for_minio(client):
        print("MinIO is not ready", file=sys.stderr)
        sys.exit(1)
    
    for bucket_name, folders in BUCKETS.items():
        if not create_bucket(client, bucket_name):
            sys.exit(1)
        create_folder_structure(client, bucket_name, folders)
    
    print("MINIO INITIALIZATION COMPLETED!")

if __name__ == "__main__":
    main()
