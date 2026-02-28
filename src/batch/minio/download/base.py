import signal
import zipfile
from datetime import datetime, timedelta
from io import BytesIO

import polars as pl
import requests
from dateutil.relativedelta import relativedelta

from src.utils.s3_client import MinIOWriter


class Download:
    """Download and convert Binance historical data to Parquet format in MinIO."""
    
    def __init__(
        self,
        data_type,
        url_template,
        frequency="daily",
        base_start_date="2026-01-01",
        column_names=None,
        has_header=True
    ):
        """
        Initialize downloader.
        
        Args:
            data_type: Full data type path including symbol (e.g., "spot/daily/aggTrades/BTCUSDT")
            url_template: URL template with placeholders (symbol/interval already hardcoded)
            frequency: Download frequency ("daily" or "monthly")
            base_start_date: Start date for downloads
            column_names: Column names to use (if CSV has no header or needs renaming)
            has_header: Whether CSV file has header row (default: True)
        """
        self.data_type = data_type
        self.url_template = url_template
        self.frequency = frequency
        self.base_start_date = base_start_date
        self.column_names = column_names
        self.has_header = has_header
        self.is_interrupted = False
        
        self.minio_writer = MinIOWriter()
        
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
    
    # ==================== Signal Handling ====================
    
    def _handle_shutdown_signal(self, signum, frame):
        """Handle graceful shutdown on SIGINT/SIGTERM."""
        print(f"\\nShutting down...")
        self.is_interrupted = True
    
    # ==================== Date Iteration ====================
    
    def _get_date_iterator(self, start_date, end_date):
        """Get appropriate date iterator based on frequency."""
        if self.frequency == "daily":
            return self._iterate_daily(start_date, end_date)
        return self._iterate_monthly(start_date, end_date)
    
    @staticmethod
    def _iterate_daily(start_date, end_date):
        """Iterate through dates day by day."""
        current_date = start_date
        while current_date <= end_date:
            yield (current_date, current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)
    
    @staticmethod
    def _iterate_monthly(start_date, end_date):
        """Iterate through dates month by month."""
        current_date = start_date.replace(day=1)
        end_month = end_date.replace(day=1)
        while current_date <= end_month:
            yield (current_date, current_date.strftime("%Y-%m"))
            current_date += relativedelta(months=1)
    
    # ==================== URL Formatting ====================
    
    def _build_url_params(self, date_obj):
        """Build URL parameters based on frequency."""
        if self.frequency == "daily":
            return {"YYYY_MM_DD": date_obj.strftime("%Y-%m-%d")}
        else:  # monthly
            return {"YYYY_MM": date_obj.strftime("%Y-%m")}
    
    # ==================== MinIO Operations ====================
    
    def _list_existing_files(self):
        """List existing parquet files in MinIO for this symbol and data type."""
        try:
            # data_type already includes full path with symbol
            object_prefix = f"{self.data_type}/"
            objects = self.minio_writer.client.list_objects(
                self.minio_writer.bucket,
                prefix=object_prefix,
                recursive=False
            )
            
            existing_filenames = set()
            for obj in objects:
                filename = obj.object_name.split('/')[-1]
                if filename.endswith('.parquet'):
                    existing_filenames.add(filename)
            
            return existing_filenames
        except Exception as e:
            print(f"Error listing existing files: {e}")
            return set()
    
    # ==================== Download and Conversion ====================
    
    def _download_and_convert_to_parquet(self, url, period_str):
        """
        Download ZIP file, extract CSV, convert to Parquet, and upload to MinIO.
        
        Args:
            url: URL to download from
            period_str: Period string for filename (e.g., "2025-01-12" or "2025-01")
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                return False
            
            # Extract CSV from ZIP in-memory
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                csv_filenames = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if not csv_filenames:
                    return False
                
                # Read CSV directly from ZIP
                with zip_file.open(csv_filenames[0]) as csv_file:
                    if self.has_header:
                        # CSV has header row
                        dataframe = pl.read_csv(csv_file, has_header=True)
                        # Rename columns if custom names provided
                        if self.column_names and len(dataframe.columns) == len(self.column_names):
                            dataframe = dataframe.rename(dict(zip(dataframe.columns, self.column_names)))
                    else:
                        # CSV has no header - must provide column names
                        if not self.column_names:
                            raise ValueError("column_names must be provided when has_header=False")
                        dataframe = pl.read_csv(csv_file, has_header=False, new_columns=self.column_names)
                    
                    if dataframe.is_empty():
                        return False
            
            # Write to MinIO as Parquet
            # data_type already includes full path with symbol
            object_path = f"{self.data_type}/{period_str}.parquet"
            success = self.minio_writer.write_parquet(dataframe, object_path)
            return success
                
        except Exception as e:
            return False
    
    # ==================== Main Run Method ====================
    
    def run(self):
        """Download all missing files from start_date to today."""
        # List existing files in MinIO
        existing_files = self._list_existing_files()
        print(f"Found {len(existing_files)} existing files in MinIO")
        
        # Parse dates
        start_date = datetime.strptime(self.base_start_date, "%Y-%m-%d")
        end_date = datetime.now()
        
        # Determine file type for messaging
        print(f"Downloading missing {self.frequency} files to MinIO...\n")
        
        # Download statistics
        total_downloaded = 0
        total_skipped = 0
        total_failed = 0
        
        # Iterate through dates and download missing files
        for date_obj, period_str in self._get_date_iterator(start_date, end_date):
            # Check for interruption
            if self.is_interrupted:
                print("\n⚠️  Download interrupted")
                break
            
            # Skip if file already exists
            parquet_filename = f"{period_str}.parquet"
            if parquet_filename in existing_files:
                total_skipped += 1
                continue
            
            # Build URL and download
            url_params = self._build_url_params(date_obj)
            download_url = self.url_template.format(**url_params)
            
            if self._download_and_convert_to_parquet(download_url, period_str):
                print(f"✓ {parquet_filename} → MinIO")
                total_downloaded += 1
            else:
                print(f"✗ {parquet_filename}")
                total_failed += 1
        
        # Print summary
        print(f"Downloaded: {total_downloaded}, Skipped: {total_skipped}, Failed: {total_failed}")
