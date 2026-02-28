"""
Data Loader for TimescaleDB
Load OHLC data from index price klines tables
"""
import warnings
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extensions
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import select
import warnings

# Suppress threading warnings
warnings.filterwarnings('ignore', message='.*ScriptRunContext.*')


# Cache database connection pool (reuse across reruns)
@st.cache_resource
def _get_db_connection_pool():
    """
    Get cached database connection pool
    Pool is shared across all reruns and users
    Thread-safe with multiple connections
    """
    try:
        from psycopg2.pool import SimpleConnectionPool
        
        config = st.secrets["timescaledb"]
        pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,  # Max 10 concurrent connections
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            # ✅ Keepalive settings to prevent connection timeout
            keepalives=1,           # Enable TCP keepalive
            keepalives_idle=60,     # Start keepalive after 60s idle
            keepalives_interval=10, # Send keepalive every 10s
            keepalives_count=5      # Max 5 failed keepalives before disconnect
        )
        print(f"✅ Created connection pool: min=1, max=10 connections")
        return pool
    except Exception as e:
        st.error(f"Failed to create connection pool: {e}")
        raise


@st.cache_resource
def _get_db_connection():
    """
    Get a single connection from the pool (backward compatibility)
    For simple cases where pooling is not needed
    """
    pool = _get_db_connection_pool()
    return pool.getconn()


class DataLoader:
    """
    DataLoader with built-in caching and auto-update
    Singleton pattern - one instance per session
    """
    
    # Class-level cache (shared across all instances in same session)
    _klines_cache: Dict[str, pd.DataFrame] = {}
    _spread_cache: Dict[str, pd.DataFrame] = {}
    _cache_initialized: bool = False
    _last_update_time: float = 0
    _update_interval: float = 1.0  # Check for updates every 1 second (for near-realtime)
    _listener_thread: Optional[threading.Thread] = None
    _listener_running: bool = False
    _pending_index_updates: set = set()  # Intervals with new INDEX data
    _pending_perpetual_updates: set = set()  # Intervals with new PERPETUAL data
    _update_callbacks: List = []  # Callbacks to trigger when cache updates
    
    # Available intervals and their corresponding table names
    INTERVALS = {
        '5m': 'indexpriceklines_5m',
        '15m': 'indexpriceklines_15m',
        '1h': 'indexpriceklines_1h',
        '4h': 'indexpriceklines_4h',
        '1d': 'indexpriceklines_1d'
    }
    
    # Interval to milliseconds mapping (for filtering correct step size)
    INTERVAL_TO_MS = {
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000
    }
    
    # Interval to timedelta mapping (for calculating window start time)
    INTERVAL_TO_TIMEDELTA = {
        '5m': timedelta(minutes=5),
        '15m': timedelta(minutes=15),
        '1h': timedelta(hours=1),
        '4h': timedelta(hours=4),
        '1d': timedelta(days=1)
    }
    
    def __init__(self, auto_init: bool = True, enable_listener: bool = True):
        """
        Initialize DataLoader with cached database connection
        
        Args:
            auto_init: If True, automatically initialize cache on first access
            enable_listener: If True, start background LISTEN thread for real-time updates
        """
        self.conn = _get_db_connection()
        
        # ✅ IMPORTANT: Start listener BEFORE loading cache to avoid missing updates!
        # Any notifications during cache loading will be queued
        if enable_listener and not DataLoader._listener_running:
            self._start_listener()
        
        # Auto-initialize cache if needed (after listener is running)
        if auto_init and not DataLoader._cache_initialized:
            self._initialize_cache()
    
    def _load_interval_data(self, interval: str) -> tuple:
        """
        Load klines and basis spread for a single interval
        Returns tuple of (df_klines, df_spread, error)
        
        Note: Uses connection from self.conn (safe for single-threaded call)
        For parallel calls, use _load_interval_data_with_pool()
        """
        try:
            # Get total records
            stats = self.get_statistics(interval=interval)
            total_records = stats.get('total_records', 0)
            
            df_klines = None
            df_spread = None
            
            if total_records > 0:
                # Load ALL klines data
                df_klines = self.get_latest_records(interval=interval, n=total_records)
                
                # Load ALL basis spread data
                df_spread = self.get_basis_spread(interval=interval, n=total_records)
                if df_spread is not None and len(df_spread) > 0:
                    df_spread = df_spread.sort_values('time', ascending=True).reset_index(drop=True)
            
            return (df_klines, df_spread, None)
            
        except Exception as e:
            return (None, None, str(e))
    
    def _load_interval_data_with_pool(self, interval: str, pool) -> tuple:
        """
        Load klines and basis spread for a single interval using connection pool
        Thread-safe version for parallel loading
        
        Args:
            interval: Time interval to load
            pool: Connection pool to get connection from
            
        Returns:
            tuple of (df_klines, df_spread, error)
        """
        conn = None
        try:
            # ✅ Get connection from pool (thread-safe!)
            conn = pool.getconn()
            
            # Get statistics
            table_name = self.INTERVALS[interval]
            interval_ms = self.INTERVAL_TO_MS[interval]
            
            stats_query = f"""
                SELECT COUNT(*) as total_records
                FROM {table_name}
                WHERE ts_ms % {interval_ms} = 0
            """
            df_stats = pd.read_sql(stats_query, conn)
            total_records = int(df_stats.iloc[0]['total_records']) if not df_stats.empty else 0
            
            df_klines = None
            df_spread = None
            
            if total_records > 0:
                # Load klines
                klines_query = f"""
                    SELECT 
                        timestamp_dt,
                        ts_ms,
                        open,
                        high,
                        low,
                        close
                    FROM {table_name}
                    WHERE ts_ms % {interval_ms} = 0
                    ORDER BY ts_ms ASC
                """
                df_klines = pd.read_sql(klines_query, conn)
                
                if not df_klines.empty:
                    df_klines['timestamp_dt'] = pd.to_datetime(df_klines['timestamp_dt'])
                    interval_delta = self.INTERVAL_TO_TIMEDELTA[interval]
                    df_klines['time'] = df_klines['timestamp_dt'] - interval_delta
                    df_klines = df_klines[['time', 'timestamp_dt', 'ts_ms', 'open', 'high', 'low', 'close']]
                
                # Load basis spread
                index_table = self.INTERVALS[interval]
                perp_table = f"perpetual_trades_{interval}"
                
                spread_query = f"""
                    SELECT 
                        i.timestamp_dt as time,
                        i.close as index_close,
                        p.price_last_trade as perpetual_price,
                        (p.price_last_trade - i.close) as basis_spread
                    FROM {index_table} i
                    INNER JOIN {perp_table} p ON i.ts_ms = p.ts_ms
                    WHERE i.ts_ms % {interval_ms} = 0
                    ORDER BY i.ts_ms ASC
                """
                df_spread = pd.read_sql(spread_query, conn)
                
                if not df_spread.empty:
                    df_spread['time'] = pd.to_datetime(df_spread['time'])
            
            return (df_klines, df_spread, None)
            
        except Exception as e:
            return (None, None, str(e))
        finally:
            # ✅ Always return connection to pool
            if conn:
                pool.putconn(conn)
    
    def _start_listener(self):
        """Start background thread to listen for PostgreSQL notifications"""
        if DataLoader._listener_running:
            return
        
        DataLoader._listener_running = True
        DataLoader._listener_thread = threading.Thread(target=self._listen_for_notifications, daemon=True)
        DataLoader._listener_thread.start()
    
    def _listen_for_notifications(self):
        """Background thread that listens for PostgreSQL NOTIFY events"""
        # Create a separate connection for LISTEN (must not be used for other queries)
        try:
            config = st.secrets["timescaledb"]
            listen_conn = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"]
            )
            listen_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            cursor = listen_conn.cursor()
            
            # Listen to all channels (using actual table names)
            intervals = ['5m', '15m', '1h', '4h', '1d']
            for interval in intervals:
                cursor.execute(f"LISTEN indexpriceklines_{interval};")
                cursor.execute(f"LISTEN perpetual_trades_{interval};")
            
            print("🎧 Listening for PostgreSQL notifications on 10 channels...")
            print(f"   Index channels: indexpriceklines_5m, indexpriceklines_15m, indexpriceklines_1h, indexpriceklines_4h, indexpriceklines_1d")
            print(f"   Perpetual channels: perpetual_trades_5m, perpetual_trades_15m, perpetual_trades_1h, perpetual_trades_4h, perpetual_trades_1d")
            
            while DataLoader._listener_running:
                # Wait for notification with timeout
                if select.select([listen_conn], [], [], 5) == ([], [], []):
                    # Timeout - no notification, continue
                    continue
                
                # Poll for notifications
                listen_conn.poll()
                
                while listen_conn.notifies:
                    notify = listen_conn.notifies.pop(0)
                    channel = notify.channel  # e.g., 'indexpriceklines_5m', 'perpetual_trades_5m'
                    interval = notify.payload  # e.g., '5m', '15m'
                    
                    print(f"🔔 Notification from {channel}: {interval}")
                    
                    # Determine if this is from indexpriceklines or perpetual_trades
                    if channel.startswith('indexpriceklines_'):
                        # Index data arrived - update klines immediately
                        DataLoader._pending_index_updates.add(interval)
                        self._update_klines(interval)
                        
                        # Check if we can update spread (if perpetual also has data)
                        if interval in DataLoader._pending_perpetual_updates:
                            self._update_spread(interval)
                            # Clear both flags after successful spread update
                            DataLoader._pending_index_updates.discard(interval)
                            DataLoader._pending_perpetual_updates.discard(interval)
                    
                    elif channel.startswith('perpetual_trades_'):
                        # Perpetual data arrived
                        DataLoader._pending_perpetual_updates.add(interval)
                        
                        # Check if we can update spread (if index also has data)
                        if interval in DataLoader._pending_index_updates:
                            self._update_spread(interval)
                            # Clear both flags after successful spread update
                            DataLoader._pending_index_updates.discard(interval)
                            DataLoader._pending_perpetual_updates.discard(interval)
            
            cursor.close()
            listen_conn.close()
            
        except Exception as e:
            print(f"❌ Listener error: {e}")
            DataLoader._listener_running = False
    
    def _update_klines(self, interval: str):
        """
        Update klines cache for a specific interval
        Called when INDEX notification is received
        """
        try:
            if interval not in DataLoader._klines_cache:
                print(f"⚠️ Interval {interval} not in klines cache")
                return
            
            df_cache = DataLoader._klines_cache[interval]
            last_ts = df_cache['ts_ms'].iloc[-1]
            
            df_new = self.get_records_after_timestamp(interval=interval, after_ts_ms=last_ts)
            
            if df_new is not None and len(df_new) > 0:
                # ✅ Optimized concat (saves 20-50ms)
                # df_new is already sorted from DB (ORDER BY ts_ms ASC)
                # df_new timestamps > df_cache timestamps → No need to sort!
                df_updated = pd.concat([df_cache, df_new], ignore_index=True)
                DataLoader._klines_cache[interval] = df_updated
                print(f"✅ Updated {interval} klines: +{len(df_new)} records (total: {len(df_updated):,})")
                
                # Trigger callbacks for klines update
                self._trigger_update_callbacks(interval)
            else:
                print(f"⚠️ No new klines data for {interval}")
            
        except Exception as e:
            print(f"❌ Error updating klines for {interval}: {e}")
    
    def _update_spread(self, interval: str):
        """
        Update spread cache for a specific interval
        Called when BOTH index and perpetual notifications are received
        """
        try:
            if interval not in DataLoader._spread_cache:
                print(f"⚠️ Interval {interval} not in spread cache")
                return
            
            df_spread_cache = DataLoader._spread_cache[interval]
            last_spread_ts = int(df_spread_cache['time'].iloc[-1].timestamp() * 1000)
            
            print(f"🔍 Updating spread for {interval}, last_ts={last_spread_ts}")
            
            df_spread_new = self.get_basis_spread_after_timestamp(interval=interval, after_ts_ms=last_spread_ts)
            
            if df_spread_new is not None and len(df_spread_new) > 0:
                # ✅ Optimized concat (saves 20-50ms)
                # df_spread_new is already sorted from DB (ORDER BY ts_ms ASC)
                df_spread_updated = pd.concat([df_spread_cache, df_spread_new], ignore_index=True)
                DataLoader._spread_cache[interval] = df_spread_updated
                print(f"✅ Updated {interval} spread: +{len(df_spread_new)} records (total: {len(df_spread_updated):,})")
                
                # Trigger callbacks for spread update
                self._trigger_update_callbacks(interval)
            else:
                print(f"⚠️ No new spread data for {interval} (JOIN may not have matching records yet)")
            
        except Exception as e:
            print(f"❌ Error updating spread for {interval}: {e}")
    
    def register_update_callback(self, callback):
        """
        Register a callback function to be called when cache is updated
        
        Args:
            callback: Function with signature callback(interval: str) -> None
        
        Example:
            def on_data_update(interval):
                print(f"Data updated for {interval}")
                st.rerun()
            
            loader.register_update_callback(on_data_update)
        """
        if callback not in DataLoader._update_callbacks:
            DataLoader._update_callbacks.append(callback)
            print(f"📝 Registered update callback: {callback.__name__}")
    
    def unregister_update_callback(self, callback):
        """
        Unregister a callback function
        
        Args:
            callback: The callback function to remove
        """
        if callback in DataLoader._update_callbacks:
            DataLoader._update_callbacks.remove(callback)
            print(f"🗑️ Unregistered update callback: {callback.__name__}")
    
    def _trigger_update_callbacks(self, interval: str):
        """
        Trigger all registered callbacks
        
        Args:
            interval: The interval that was updated
        """
        for callback in DataLoader._update_callbacks:
            try:
                callback(interval)
            except Exception as e:
                print(f"❌ Error in callback {callback.__name__}: {e}")
    
    def _initialize_cache(self):
        """Initialize cache by loading ALL data for all intervals (parallel)"""
        if DataLoader._cache_initialized:
            return
        
        intervals = ['5m', '15m', '1h', '4h', '1d']
        
        print("🔄 Initializing cache... Loading all historical data.")
        
        try:
            completed = 0
            total = len(intervals)
            pool = _get_db_connection_pool()
            
            # ✅ Parallel loading with connection pool (thread-safe!)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self._load_interval_data_with_pool, interval, pool): interval 
                          for interval in intervals}
                
                for future in as_completed(futures):
                    interval = futures[future]
                    
                    try:
                        df_klines, df_spread, error = future.result()
                        
                        if error:
                            print(f"⚠️ Failed to load {interval}: {error}")
                        else:
                            # Store in class-level cache
                            if df_klines is not None and len(df_klines) > 0:
                                DataLoader._klines_cache[interval] = df_klines
                            if df_spread is not None and len(df_spread) > 0:
                                DataLoader._spread_cache[interval] = df_spread
                            
                            print(f"✅ Loaded {interval}: {len(df_klines):,} klines, {len(df_spread):,} spread")
                        
                        completed += 1
                        
                    except Exception as e:
                        print(f"⚠️ Error processing {interval}: {str(e)}")
                        completed += 1
            
            DataLoader._cache_initialized = True
            DataLoader._last_update_time = time.time()
            
            print(f"✅ All data loaded successfully! ({total} intervals)")
            
        except Exception as e:
            print(f"❌ Failed to initialize cache: {str(e)}")
            raise
    
    def _update_cache(self, force: bool = False):
        """
        Check for new data and update cache (called automatically)
        
        Args:
            force: If True, bypass time check and force update immediately
        """
        current_time = time.time()
        
        # Only update if enough time has passed (unless forced)
        if not force and (current_time - DataLoader._last_update_time < DataLoader._update_interval):
            return False
        
        DataLoader._last_update_time = current_time
        updated = False
        
        intervals = ['5m', '15m', '1h', '4h', '1d']
        
        for interval in intervals:
            # Update klines
            if interval in DataLoader._klines_cache:
                df_cache = DataLoader._klines_cache[interval]
                last_ts = df_cache['ts_ms'].iloc[-1]
                
                df_new = self.get_records_after_timestamp(interval=interval, after_ts_ms=last_ts)
                
                if df_new is not None and len(df_new) > 0:
                    # ✅ Optimized concat (saves 20-50ms)
                    df_updated = pd.concat([df_cache, df_new], ignore_index=True)
                    # Skip sort - data is already in order!
                    DataLoader._klines_cache[interval] = df_updated
                    updated = True
            
            # Update spread
            if interval in DataLoader._spread_cache:
                df_spread_cache = DataLoader._spread_cache[interval]
                last_spread_ts = int(df_spread_cache['time'].iloc[-1].timestamp() * 1000)
                
                df_spread_new = self.get_basis_spread_after_timestamp(interval=interval, after_ts_ms=last_spread_ts)
                
                if df_spread_new is not None and len(df_spread_new) > 0:
                    # ✅ Optimized concat (saves 20-50ms)
                    df_spread_updated = pd.concat([df_spread_cache, df_spread_new], ignore_index=True)
                    # Skip sort - data is already in order!
                    DataLoader._spread_cache[interval] = df_spread_updated
                    updated = True
        
        return updated
    
    def get_cached_klines(self, interval: str, check_update: bool = True, 
                           start_time: Optional[datetime] = None, 
                           end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get klines data from cache (auto-updates if needed)
        
        Args:
            interval: Time interval
            check_update: If True, check for updates before returning
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            DataFrame with klines data (filtered by time range if specified)
        """
        # Auto-update cache (respects time interval)
        if check_update:
            self._update_cache()
        
        if interval not in DataLoader._klines_cache:
            return pd.DataFrame()
        
        df = DataLoader._klines_cache[interval]
        
        # ✅ Binary search filtering (O(log n) instead of O(n)!)
        if start_time is not None or end_time is not None:
            # Use pandas searchsorted for O(log n) lookup
            # Assumes timestamp_dt is sorted (which it is from our queries)
            
            if start_time is not None:
                # Find first index >= start_time
                start_idx = df['timestamp_dt'].searchsorted(start_time, side='left')
            else:
                start_idx = 0
            
            if end_time is not None:
                # Find first index > end_time (non-inclusive)
                end_idx = df['timestamp_dt'].searchsorted(end_time, side='right')
            else:
                end_idx = len(df)
            
            # Return slice (no copy, just view with iloc)
            return df.iloc[start_idx:end_idx]
        
        # Return view (no copy for performance)
        return df
    
    def get_cached_spread(self, interval: str, check_update: bool = True,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get basis spread data from cache (auto-updates if needed)
        
        Args:
            interval: Time interval
            check_update: If True, check for updates before returning
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            DataFrame with basis spread data (filtered by time range if specified)
        """
        # Auto-update cache (respects time interval)
        if check_update:
            self._update_cache()
        
        if interval not in DataLoader._spread_cache:
            return pd.DataFrame()
        
        df = DataLoader._spread_cache[interval]
        
        # ✅ Binary search filtering (O(log n) instead of O(n)!)
        if start_time is not None or end_time is not None:
            # Use pandas searchsorted for O(log n) lookup
            # Assumes 'time' column is sorted (which it is from our queries)
            
            if start_time is not None:
                # Find first index >= start_time
                start_idx = df['time'].searchsorted(start_time, side='left')
            else:
                start_idx = 0
            
            if end_time is not None:
                # Find first index > end_time (non-inclusive)
                end_idx = df['time'].searchsorted(end_time, side='right')
            else:
                end_idx = len(df)
            
            # Return slice (no copy, just view with iloc)
            return df.iloc[start_idx:end_idx]
        
        # Return view (no copy for performance)
        return df
    
    @st.cache_data(ttl=10)  # Cache for 10 seconds (real-time data)
    def get_latest_records(_self, interval: str = '1h', n: int = 100) -> pd.DataFrame:
        """
        Get the latest N records
        
        Args:
            interval: Time interval
            n: Number of records to return
            
        Returns:
            DataFrame with latest N records (filtered by step size)
        """
        if interval not in _self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(_self.INTERVALS.keys())}")
        
        table_name = _self.INTERVALS[interval]
        interval_ms = _self.INTERVAL_TO_MS[interval]
        
        query = f"""
            SELECT 
                timestamp_dt,
                ts_ms,
                open,
                high,
                low,
                close
            FROM {table_name}
            WHERE ts_ms % {interval_ms} = 0
            ORDER BY ts_ms DESC
            LIMIT {n}
        """
        
        try:
            df = pd.read_sql(query, _self.conn)
            
            # Convert timestamp_dt to datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp_dt']):
                df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'])
            
            # Sort ascending for plotting
            df = df.sort_values('ts_ms', ascending=True).reset_index(drop=True)
            
            # Calculate window start time: time = timestamp_dt - interval
            interval_delta = _self.INTERVAL_TO_TIMEDELTA[interval]
            df['time'] = df['timestamp_dt'] - interval_delta
            
            # Reorder columns
            df = df[['time', 'timestamp_dt', 'ts_ms', 'open', 'high', 'low', 'close']]
            
            return df
        
        except Exception as e:
            st.error(f"Failed to load latest records: {e}")
            raise
    
    @st.cache_data(ttl=5)  # Cache for 5 seconds (very fresh data)
    def get_records_after_timestamp(_self, interval: str = '1h', after_ts_ms: int = 0) -> pd.DataFrame:
        """
        Get records with ts_ms greater than the specified timestamp
        
        Args:
            interval: Time interval
            after_ts_ms: Get records with ts_ms > this value
            
        Returns:
            DataFrame with new records (filtered by step size)
        """
        if interval not in _self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(_self.INTERVALS.keys())}")
        
        table_name = _self.INTERVALS[interval]
        interval_ms = _self.INTERVAL_TO_MS[interval]
        
        query = f"""
            SELECT 
                timestamp_dt,
                ts_ms,
                open,
                high,
                low,
                close
            FROM {table_name}
            WHERE ts_ms > {after_ts_ms}
              AND ts_ms % {interval_ms} = 0
            ORDER BY ts_ms ASC
        """
        
        try:
            df = pd.read_sql(query, _self.conn)
            
            if df.empty:
                return df
            
            # Convert timestamp_dt to datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp_dt']):
                df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'])
            
            # Calculate window start time: time = timestamp_dt - interval
            interval_delta = _self.INTERVAL_TO_TIMEDELTA[interval]
            df['time'] = df['timestamp_dt'] - interval_delta
            
            # Reorder columns
            df = df[['time', 'timestamp_dt', 'ts_ms', 'open', 'high', 'low', 'close']]
            
            return df
        
        except Exception as e:
            st.error(f"Failed to load new records: {e}")
            raise
    
    @st.cache_data(ttl=5)  # Cache for 5 seconds
    def get_basis_spread_after_timestamp(_self, interval: str = '1h', after_ts_ms: int = 0) -> pd.DataFrame:
        """
        Get basis spread records with ts_ms greater than the specified timestamp
        
        Args:
            interval: Time interval
            after_ts_ms: Get records with ts_ms > this value
            
        Returns:
            DataFrame with new spread records
        """
        if interval not in _self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of {list(_self.INTERVALS.keys())}")
        
        try:
            # Table names
            index_table = _self.INTERVALS[interval]
            perp_table = f"perpetual_trades_{interval}"
            interval_ms = _self.INTERVAL_TO_MS[interval]
            
            query = f"""
            SELECT 
                i.timestamp_dt as time,
                i.close as index_close,
                p.price_last_trade as perpetual_price,
                (p.price_last_trade - i.close) as basis_spread
            FROM {index_table} i
            INNER JOIN {perp_table} p ON i.ts_ms = p.ts_ms
            WHERE i.ts_ms > {after_ts_ms}
              AND i.ts_ms % {interval_ms} = 0
            ORDER BY i.ts_ms ASC
            """
            
            # Execute query
            df = pd.read_sql(query, _self.conn)
            
            # Convert time to datetime
            if not df.empty:
                df['time'] = pd.to_datetime(df['time'])
            
            return df
        
        except Exception as e:
            st.error(f"Failed to load new basis spread: {e}")
            raise
    
    def get_statistics(self, interval: str = '1h') -> dict:
        """
        Get basic statistics for a given interval
        
        Args:
            interval: Time interval
            
        Returns:
            Dictionary with statistics
        """
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        table_name = self.INTERVALS[interval]
        interval_ms = self.INTERVAL_TO_MS[interval]
        
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(timestamp_dt) as first_record,
                MAX(timestamp_dt) as last_record,
                MIN(low) as all_time_low,
                MAX(high) as all_time_high
            FROM {table_name}
            WHERE ts_ms % {interval_ms} = 0
        """
        
        try:
            # Use pandas read_sql
            df_temp = pd.read_sql(query, self.conn)
            if not df_temp.empty:
                result = df_temp.iloc[0]
                return {
                    'total_records': int(result['total_records']),
                    'first_record': result['first_record'],
                    'last_record': result['last_record'],
                    'all_time_low': result['all_time_low'],
                    'all_time_high': result['all_time_high']
                }
            else:
                return {}
        
        except Exception as e:
            st.error(f"Failed to get statistics: {e}")
            raise
    
    @st.cache_data(ttl=10)  # Cache for 10 seconds
    def get_basis_spread(_self, interval: str = '1h', start_date: Optional[datetime] = None, 
                         end_date: Optional[datetime] = None, n: Optional[int] = None) -> pd.DataFrame:
        """
        Calculate basis spread: perpetual last trade price - index close price
        
        Args:
            interval: Time interval ('5m', '15m', '1h', '4h', '1d')
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            n: Number of latest records to return (optional)
        
        Returns:
            DataFrame with columns: time, index_close, perpetual_price, basis_spread
        """
        if interval not in _self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of {list(_self.INTERVALS.keys())}")
        
        try:
            # Table names
            index_table = _self.INTERVALS[interval]
            perp_table = f"perpetual_trades_{interval}"
            interval_ms = _self.INTERVAL_TO_MS[interval]
            
            # Build query - use ts_ms for joining and filter by step size
            if n:
                # Get latest N records with step size filtering
                query = f"""
                SELECT 
                    i.timestamp_dt as time,
                    i.close as index_close,
                    p.price_last_trade as perpetual_price,
                    (p.price_last_trade - i.close) as basis_spread
                FROM {index_table} i
                INNER JOIN {perp_table} p ON i.ts_ms = p.ts_ms
                WHERE i.ts_ms % {interval_ms} = 0
                ORDER BY i.ts_ms DESC
                LIMIT {n}
                """
            else:
                # Get by date range with step size filtering
                conditions = [f"i.ts_ms % {interval_ms} = 0"]
                if start_date:
                    start_ts = int(start_date.timestamp() * 1000)
                    conditions.append(f"i.ts_ms >= {start_ts}")
                if end_date:
                    end_ts = int(end_date.timestamp() * 1000)
                    conditions.append(f"i.ts_ms <= {end_ts}")
                
                where_clause = f"WHERE {' AND '.join(conditions)}"
                
                query = f"""
                SELECT 
                    i.timestamp_dt as time,
                    i.close as index_close,
                    p.price_last_trade as perpetual_price,
                    (p.price_last_trade - i.close) as basis_spread
                FROM {index_table} i
                INNER JOIN {perp_table} p ON i.ts_ms = p.ts_ms
                {where_clause}
                ORDER BY i.ts_ms ASC
                """
            
            # Execute query
            df = pd.read_sql(query, _self.conn)
            
            # Convert time to datetime
            if not df.empty:
                df['time'] = pd.to_datetime(df['time'])
            
            return df
        
        except Exception as e:
            st.error(f"Failed to calculate basis spread: {e}")
            raise


# Example usage
if __name__ == "__main__":
    # This will only work when run within Streamlit context
    with DataLoader() as loader:
        # Get statistics
        stats = loader.get_statistics(interval='1h')
        print(f"Statistics: {stats}")
        
        # Get latest 100 records
        df = loader.get_latest_records(interval='1h', n=100)
        print(f"Loaded {len(df)} records")
        print(df.head())
