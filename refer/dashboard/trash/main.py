"""
Streamlit Dashboard for Index Price Klines
Display charts using ECharts
"""
import streamlit as st
from streamlit_echarts_zoom import st_echarts_zoom
from datetime import datetime, timedelta
import pandas as pd
import time
import sys
import os
import warnings

# Suppress threading warnings from Streamlit
warnings.filterwarnings('ignore', message='.*ScriptRunContext.*')

# Add src/dashboard to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from data_loader import DataLoader

# Page configuration
st.set_page_config(
    page_title="OKX BTC-USDT Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Styling (from main.py)
st.markdown("""
<style>
    /* Hide copy buttons */
    button[title="Copy to clipboard"],
    [data-testid="stHeaderActionElements"] {
        display: none !important;
    }
    
    /* Style for slider tooltip */
    .stSlider [data-testid="stThumbValue"] {
        background-color: #262730 !important;
        color: #26a69a !important;
        padding: 4px 8px !important;
        border-radius: 4px !important;
        font-size: 12px !important;
    }
    
    .stSlider [data-testid="stTickBar"] {
        display: none !important;
    }
    
    /* Icon-only buttons - no background box */
    .stButton > button {
        font-size: 32px !important;
        padding: 0 !important;
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        min-height: auto !important;
        width: auto !important;
    }
    
    .stButton > button:hover {
        background: transparent !important;
        border: none !important;
        transform: scale(1.2) !important;
    }
    
    /* Fix button container */
    .stButton {
        width: 100%;
        display: flex !important;
        justify-content: center !important;
    }
</style>
""", unsafe_allow_html=True)

# UTC Time display with JavaScript real-time clock (same as main.py)
import streamlit.components.v1 as components_v1
st.sidebar.markdown('<div style="margin-bottom: 10px;"></div>', unsafe_allow_html=True)
components_v1.html("""
<div style="
    background-color: #262730;
    padding: 10px;
    border-radius: 5px;
    text-align: center;
    border: 1px solid #464646;
">
    <div style="font-size: 11px; color: #9CA3AF; margin-bottom: 4px;">UTC TIME</div>
    <div id="utc-time" style="font-size: 16px; font-weight: 600; color: #26a69a; font-family: 'Courier New', monospace;">
        --:--:--
    </div>
    <div id="utc-date" style="font-size: 11px; color: #9CA3AF; margin-top: 4px;">
        ----------
    </div>
</div>
<script>
function updateUTCClock() {
    const now = new Date();
    const hours = String(now.getUTCHours()).padStart(2, '0');
    const minutes = String(now.getUTCMinutes()).padStart(2, '0');
    const seconds = String(now.getUTCSeconds()).padStart(2, '0');
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    const day = String(now.getUTCDate()).padStart(2, '0');
    
    const timeEl = document.getElementById('utc-time');
    const dateEl = document.getElementById('utc-date');
    
    if (timeEl) timeEl.textContent = hours + ':' + minutes + ':' + seconds;
    if (dateEl) dateEl.textContent = year + '-' + month + '-' + day;
}

updateUTCClock();
setInterval(updateUTCClock, 1000);
</script>
""", height=80)

# Initialize DataLoader (handles ALL cache + real-time updates automatically)
@st.cache_resource
def get_data_loader():
    """
    Get cached DataLoader instance
    - Automatically loads ALL data on first access (parallel)
    - Listens for PostgreSQL notifications for instant updates
    - NO manual cache management needed!
    """
    # ❌ REALTIME DISABLED: enable_listener=False
    return DataLoader(auto_init=True, enable_listener=False)

loader = get_data_loader()

# Initialize session state for playback controls
if 'is_playing' not in st.session_state:
    st.session_state.is_playing = False
if 'current_candle_index' not in st.session_state:
    st.session_state.current_candle_index = None

# ❌ REALTIME DISABLED: Auto-refresh disabled
# Initialize last refresh time for auto-refresh in realtime mode
# if 'last_refresh_time' not in st.session_state:
#     st.session_state.last_refresh_time = time.time()
if 'is_realtime_mode' not in st.session_state:
    st.session_state.is_realtime_mode = True
if 'last_interval' not in st.session_state:
    st.session_state.last_interval = None
if 'last_range_option' not in st.session_state:
    st.session_state.last_range_option = None

# Interval to seconds mapping
INTERVAL_TO_SECONDS = {
    '5m': 5 * 60,
    '15m': 15 * 60,
    '1h': 60 * 60,
    '4h': 4 * 60 * 60,
    '1d': 24 * 60 * 60
}

# Calculate max records based on time range
def calculate_max_records(interval: str, start_date: datetime, end_date: datetime) -> int:
    """Calculate max number of candles between start and end date for given interval"""
    total_seconds = (end_date - start_date).total_seconds()
    interval_seconds = INTERVAL_TO_SECONDS[interval]
    max_records = int(total_seconds / interval_seconds)
    return max(1, max_records)  # At least 1 record

# Generate timestamps for given interval
def generate_timestamps(interval: str, start_date: datetime, n_records: int):
    """Generate list of candle end times (timestamp_dt)"""
    interval_seconds = INTERVAL_TO_SECONDS[interval]
    timestamps = []
    
    # First candle end time = start_date + interval
    current_time = start_date + timedelta(seconds=interval_seconds)
    
    for i in range(n_records):
        timestamps.append(current_time)
        current_time += timedelta(seconds=interval_seconds)
    
    return timestamps

# Fixed start date and current end date
START_DATE = datetime(2025, 1, 1, 0, 0, 0)  # 2025-01-01 00:00:00 UTC
CURRENT_DATE = datetime.utcnow()

# Sidebar configuration
interval = st.sidebar.selectbox("Interval", options=['5m', '15m', '1h', '4h', '1d'], index=0)
data_range_option = st.sidebar.radio("Range", options=['Record', 'Date'], index=0)

# Reset playback when interval or range mode changes
if st.session_state.last_interval != interval or st.session_state.last_range_option != data_range_option:
    st.session_state.current_candle_index = None
    st.session_state.is_playing = False
    st.session_state.is_realtime_mode = True
    st.session_state.last_interval = interval
    st.session_state.last_range_option = data_range_option

# Calculate max records for current interval
max_records = calculate_max_records(interval, START_DATE, CURRENT_DATE)

# ✅ Cache timestamp generation (saves 50-200ms per rerun!)
if 'timestamps_cache' not in st.session_state:
    st.session_state.timestamps_cache = {}

if data_range_option == 'Record':
    st.sidebar.info(f"Max records: {max_records:,}")
    n_candles = st.sidebar.slider("Number of records", 50, max_records, 50, 50)
    
    # ✅ Track changes to reset to realtime mode
    if 'last_n_candles' not in st.session_state:
        st.session_state.last_n_candles = n_candles
    
    if st.session_state.last_n_candles != n_candles:
        # User changed slider → reset to realtime mode
        st.session_state.current_candle_index = None
        st.session_state.is_realtime_mode = True
        st.session_state.is_playing = False
        st.session_state.last_n_candles = n_candles
    
    # Check cache for all timestamps
    cache_key = f"all_{interval}_{START_DATE}_{max_records}"
    if cache_key not in st.session_state.timestamps_cache:
        # Generate only if not cached
        st.session_state.timestamps_cache[cache_key] = generate_timestamps(interval, START_DATE, max_records)
    
    all_timestamps = st.session_state.timestamps_cache[cache_key]
    timestamps = all_timestamps[-n_candles:]  # Take last N (most recent)
    
else:
    # Date range mode
    st.sidebar.info(f"{START_DATE.strftime('%Y/%m/%d')} - {CURRENT_DATE.strftime('%Y/%m/%d')}")
    
    # Default: Last 10 days
    default_start = max(START_DATE, CURRENT_DATE - timedelta(days=10))
    default_end = CURRENT_DATE
    
    start_date = st.sidebar.date_input("Start Date", default_start.date(), 
                                        min_value=START_DATE.date(), 
                                        max_value=CURRENT_DATE.date())
    end_date = st.sidebar.date_input("End Date", default_end.date(), 
                                      min_value=START_DATE.date(), 
                                      max_value=CURRENT_DATE.date())
    
    # ✅ Track changes to reset to realtime mode
    if 'last_start_date' not in st.session_state:
        st.session_state.last_start_date = start_date
    if 'last_end_date' not in st.session_state:
        st.session_state.last_end_date = end_date
    
    if st.session_state.last_start_date != start_date or st.session_state.last_end_date != end_date:
        # User changed date range → reset to realtime mode
        st.session_state.current_candle_index = None
        st.session_state.is_realtime_mode = True
        st.session_state.is_playing = False
        st.session_state.last_start_date = start_date
        st.session_state.last_end_date = end_date
    
    # Convert to datetime
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Calculate number of candles in date range
    n_candles = calculate_max_records(interval, start_datetime, end_datetime)
    
    # Check cache for date range timestamps
    date_cache_key = f"date_{interval}_{start_datetime}_{n_candles}"
    if date_cache_key not in st.session_state.timestamps_cache:
        # Generate only if not cached
        st.session_state.timestamps_cache[date_cache_key] = generate_timestamps(interval, start_datetime, n_candles)
    
    timestamps = st.session_state.timestamps_cache[date_cache_key]

# Initialize playback index
if st.session_state.current_candle_index is None or st.session_state.current_candle_index > len(timestamps):
    st.session_state.current_candle_index = len(timestamps)
    st.session_state.is_realtime_mode = True

# Check real-time mode
st.session_state.is_realtime_mode = (st.session_state.current_candle_index >= len(timestamps))

# Load data from DataLoader cache
try:
    # ✅ Load only the data range we need (not all cache!)
    # In realtime mode: get last N records directly (no timestamp filter)
    # In historical mode: filter by timestamps
    if st.session_state.is_realtime_mode:
        # Realtime: Get last N records without timestamp filter
        start_time = None
        end_time = None
    elif len(timestamps) > 0:
        # Historical: Filter by timestamp range
        start_time = timestamps[0]
        end_time = timestamps[-1]
    else:
        start_time = None
        end_time = None
    
    # ✅ Get data filtered by time range from cache (saves memory!)
    try:
        df_klines_full = loader.get_cached_klines(
            interval=interval, 
            check_update=False,
            start_time=start_time,
            end_time=None  # Don't filter end yet (for realtime updates)
        )
        
        df_spread_full = loader.get_cached_spread(
            interval=interval, 
            check_update=False,
            start_time=start_time,
            end_time=None  # Don't filter end yet (for realtime updates)
        )
        
        # In realtime mode, take only last N records
        if st.session_state.is_realtime_mode and len(timestamps) > 0:
            n_records = len(timestamps)
            df_klines_full = df_klines_full.tail(n_records)
            df_spread_full = df_spread_full.tail(n_records)
    except TypeError as e:
        # Fallback: Old API without start_time parameter
        st.warning("⚠️ Old DataLoader API detected. Please restart the app to use new filtering.")
        df_klines_full = loader.get_cached_klines(interval=interval, check_update=False)
        df_spread_full = loader.get_cached_spread(interval=interval, check_update=False)
        
        # Manual filter
        if start_time is not None:
            df_klines_full = df_klines_full[df_klines_full['timestamp_dt'] >= start_time]
            df_spread_full = df_spread_full[df_spread_full['time'] >= start_time]
    
    if df_klines_full.empty:
        st.warning(f"No klines data available for {interval}")
        st.stop()
    
    # ✅ Cache filtered DataFrames (saves 10-30ms per rerun when cache hit)
    if 'filter_cache' not in st.session_state:
        st.session_state.filter_cache = {}
    
    # Filter data based on timestamps (playback logic)
    # Get timestamps from current slice
    if st.session_state.current_candle_index > 0 and st.session_state.current_candle_index <= len(timestamps):
        cutoff_time = timestamps[st.session_state.current_candle_index - 1]
        
        # Create cache key for filtered data
        filter_key = f"filter_{interval}_{st.session_state.current_candle_index}_{len(df_klines_full)}"
        
        # Check cache first
        if filter_key in st.session_state.filter_cache:
            # ✅ Load from cache (instant!)
            cached_filter = st.session_state.filter_cache[filter_key]
            df_klines = cached_filter['klines']
            df_spread = cached_filter['spread']
        else:
            # Filter klines up to current playback position
            df_klines = df_klines_full[df_klines_full['timestamp_dt'] <= cutoff_time].copy()
            
            # Filter spread up to current playback position
            if not df_spread_full.empty:
                df_spread = df_spread_full[df_spread_full['time'] <= cutoff_time].copy()
            else:
                df_spread = pd.DataFrame()
            
            # Store in cache
            st.session_state.filter_cache[filter_key] = {
                'klines': df_klines,
                'spread': df_spread
            }
            
            # Limit cache size (keep last 50 states)
            if len(st.session_state.filter_cache) > 50:
                oldest_keys = list(st.session_state.filter_cache.keys())[:-50]
                for old_key in oldest_keys:
                    del st.session_state.filter_cache[old_key]
    else:
        # Show all data (realtime mode)
        # ✅ No copy needed - displaying full dataset (saves 10-30ms)
        df_klines = df_klines_full
        df_spread = df_spread_full
        
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

# # --- CHART 1: INDEX PRICE CANDLESTICK CHART ---
# # ❌ REALTIME AUTO-REFRESH DISABLED
# # Use fragment to refresh charts without reloading entire UI
# # @st.fragment(run_every="5s" if st.session_state.is_realtime_mode else None)  # ❌ COMMENTED - No auto-refresh
# def render_charts():
#     """Render charts - Historical mode only (realtime auto-refresh disabled)"""
#     # ❌ REALTIME DISABLED: Use static data from parent scope only
#     # # Re-load data from cache (updated by background listener)
#     # try:
#     #     # Get current display mode
#     #     is_realtime = st.session_state.get('is_realtime_mode', True)
#     #     current_index = st.session_state.get('current_candle_index', len(timestamps))
#     #     
#     #     # Load data based on mode
#     #     if is_realtime:
#     #         # Realtime: Get last N records
#     #         df_klines_display = loader.get_cached_klines(interval=interval, check_update=False)
#     #         df_spread_display = loader.get_cached_spread(interval=interval, check_update=False)
#     #         
#     #         # Take last N records
#     #         n_records = len(timestamps)
#     #         df_klines_display = df_klines_display.tail(n_records)
#     #         df_spread_display = df_spread_display.tail(n_records)
#     #     else:
#     #         # Historical: Use filtered data from parent scope
#     #         df_klines_display = df_klines
#     #         df_spread_display = df_spread
#     # except:
#     #     # Fallback to parent scope data
#     df_klines_display = df_klines
#     df_spread_display = df_spread
#     
#     # Use 2/3 width for charts
#     chart_col, _ = st.columns([2, 1])
#     
#     with chart_col:
#         st.markdown('<h3 style="font-size: 16px; font-weight: 600; margin-top: 20px; margin-bottom: 10px;">Index Price</h3>', unsafe_allow_html=True)
#     
#     if len(df_klines) > 0:
#         # Initialize cache if needed
#         if 'chart_data_cache' not in st.session_state:
#             st.session_state.chart_data_cache = {}
#         
#         # Create cache key based on current state
#         # Include last timestamp to detect new data
#         last_ts = df_klines_display['ts_ms'].iloc[-1] if len(df_klines_display) > 0 else 0
#         cache_key = f"candle_{interval}_{st.session_state.current_candle_index}_{len(df_klines_display)}_{last_ts}"
#         
#         # Check cache first
#         if cache_key in st.session_state.chart_data_cache:
#             # ✅ Load from cache (instant!)
#             cached = st.session_state.chart_data_cache[cache_key]
#             candle_data = cached['candle_data']
#             time_labels = cached['time_labels']
#         else:
#             # ⚠️ Cache miss: prepare data (first time only)
#             # ✅ Vectorized operations (10-40x faster than iterrows!)
#             candle_data = df_klines[['open', 'close', 'low', 'high']].values.tolist()
#             time_labels = df_klines['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
#             
#             # Store in cache for next rerun
#             st.session_state.chart_data_cache[cache_key] = {
#                 'candle_data': candle_data,
#                 'time_labels': time_labels
#             }
#             
#             # Limit cache size (keep last 100 states)
#             if len(st.session_state.chart_data_cache) > 100:
#                 # Remove oldest entries
#                 oldest_keys = list(st.session_state.chart_data_cache.keys())[:-100]
#                 for old_key in oldest_keys:
#                     del st.session_state.chart_data_cache[old_key]
#         
#         # ECharts candlestick configuration
#         candle_options = {
#             "animation": True,
#             "animationDuration": 300,  # 300ms animation (smooth but not slow)
#             "animationEasing": "cubicOut",  # Smooth easing
#             "animationDelay": 0,  # No delay
#             "backgroundColor": "#1E1E1E",
#             "grid": {
#                 "left": "3%",
#                 "right": "4%",
#                 "bottom": "15%",
#                 "top": "5%",
#                 "containLabel": True
#             },
#             "tooltip": {
#                 "trigger": "axis",
#                 "axisPointer": {"type": "cross"}
#             },
#             "dataZoom": [
#                 {
#                     "type": "inside",
#                     "start": 0,
#                     "end": 100,
#                     "zoomOnMouseWheel": True,
#                     "moveOnMouseMove": True
#                 },
#                 {
#                     "type": "slider",
#                     "start": 0,
#                     "end": 100,
#                     "height": 20,
#                     "bottom": 5,
#                     "borderColor": "#787B86",
#                     "fillerColor": "rgba(38, 166, 154, 0.2)",
#                     "handleStyle": {"color": "#787B86"},
#                     "textStyle": {"color": "#D1D4DC"}
#                 }
#             ],
#             "xAxis": {
#                 "type": "category",
#                 "data": time_labels,
#                 "axisLine": {"lineStyle": {"color": "#787B86"}},
#                 "axisLabel": {"color": "#D1D4DC", "fontSize": 10}
#             },
#             "yAxis": {
#                 "type": "value",
#                 "scale": True,
#                 "splitLine": {"lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}},
#                 "axisLine": {"lineStyle": {"color": "#787B86"}},
#                 "axisLabel": {"color": "#D1D4DC"}
#             },
#             "series": [
#                 {
#                     "name": "Index Price",
#                     "type": "candlestick",
#                     "data": candle_data,
#                     "itemStyle": {
#                         "color": "#26a69a",
#                         "color0": "#ef5350",
#                         "borderColor": "#26a69a",
#                         "borderColor0": "#ef5350"
#                     }
#                 }
#             ]
#         }
#         
#         # Render ECharts with zoom persistence
#         st_echarts_zoom(candle_options, height="350px", key="index_price_chart")
#     else:
#         st.warning("No candle data to display")
# 
# Call render_charts to display the index price chart
# render_charts()
# 
# --- CHART 2: BASIS SPREAD LINE CHART ---
# chart_col2, _ = st.columns([2, 1])
# 
# with chart_col2:
#     st.markdown('<h3 style="font-size: 16px; font-weight: 600; margin-top: 20px; margin-bottom: 10px;">Basis Spread</h3>', unsafe_allow_html=True)
#     
#     if len(df_spread) > 0:
#         # Initialize spread cache if needed
#         if 'spread_data_cache' not in st.session_state:
#             st.session_state.spread_data_cache = {}
#         
#         # Create cache key for spread data
#         # Include last timestamp to detect new data
#         last_spread_ts = int(df_spread['time'].iloc[-1].timestamp() * 1000) if len(df_spread) > 0 else 0
#         spread_cache_key = f"spread_{interval}_{st.session_state.current_candle_index}_{len(df_spread)}_{last_spread_ts}"
#         
#         # Check cache first
#         if spread_cache_key in st.session_state.spread_data_cache:
#             # ✅ Load from cache (instant!)
#             cached_spread = st.session_state.spread_data_cache[spread_cache_key]
#             spread_time_labels = cached_spread['time_labels']
#             spread_values = cached_spread['values']
#         else:
#             # ⚠️ Cache miss: prepare data (first time only)
#             # ✅ Already vectorized (dt.strftime() is vectorized)
#             spread_time_labels = df_spread['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
#             spread_values = df_spread['basis_spread'].tolist()
#             
#             # Store in cache
#             st.session_state.spread_data_cache[spread_cache_key] = {
#                 'time_labels': spread_time_labels,
#                 'values': spread_values
#             }
#             
#             # Limit cache size (keep last 100 states)
#             if len(st.session_state.spread_data_cache) > 100:
#                 oldest_keys = list(st.session_state.spread_data_cache.keys())[:-100]
#                 for old_key in oldest_keys:
#                     del st.session_state.spread_data_cache[old_key]
#         
#         # ECharts line chart configuration
#         spread_options = {
#             "animation": True,
#             "animationDuration": 300,  # 300ms animation (smooth but not slow)
#             "animationEasing": "cubicOut",  # Smooth easing
#             "animationDelay": 0,  # No delay
#             "backgroundColor": "#1E1E1E",
#             "grid": {
#                 "left": "3%",
#                 "right": "4%",
#                 "bottom": "15%",
#                 "top": "10%",
#                 "containLabel": True
#             },
#             "tooltip": {
#                 "trigger": "axis",
#                 "axisPointer": {"type": "cross"}
#             },
#             "dataZoom": [
#                 {
#                     "type": "inside",
#                     "start": 0,
#                     "end": 100,
#                     "zoomOnMouseWheel": True,
#                     "moveOnMouseMove": True
#                 },
#                 {
#                     "type": "slider",
#                     "start": 0,
#                     "end": 100,
#                     "height": 20,
#                     "bottom": 5,
#                     "borderColor": "#787B86",
#                     "fillerColor": "rgba(38, 166, 154, 0.2)",
#                     "handleStyle": {"color": "#787B86"},
#                     "textStyle": {"color": "#D1D4DC"}
#                 }
#             ],
#             "xAxis": {
#                 "type": "category",
#                 "data": spread_time_labels,
#                 "axisLine": {"lineStyle": {"color": "#787B86"}},
#                 "axisLabel": {"color": "#D1D4DC", "fontSize": 10}
#             },
#             "yAxis": {
#                 "type": "value",
#                 "splitLine": {"lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}},
#                 "axisLine": {"lineStyle": {"color": "#787B86"}},
#                 "axisLabel": {"color": "#D1D4DC"}
#             },
#             "series": [
#                 {
#                     "name": "Basis Spread",
#                     "type": "line",
#                     "data": spread_values,
#                     "lineStyle": {"color": "rgba(66, 133, 244, 1)", "width": 1},
#                     "areaStyle": {
#                         "color": {
#                             "type": "linear",
#                             "x": 0, "y": 0, "x2": 0, "y2": 1,
#                             "colorStops": [
#                                 {"offset": 0, "color": "rgba(66, 133, 244, 0.4)"},
#                                 {"offset": 1, "color": "rgba(66, 133, 244, 0.05)"}
#                             ]
#                         }
#                     },
#                     "smooth": False,
#                     "symbol": "none",
#                     "sampling": "lttb"
#                 }
#             ]
#         }
#         
#         # Render ECharts with zoom persistence
#         st_echarts_zoom(spread_options, height="250px", key="basis_spread_chart")
#     else:
#         st.warning("No basis spread data to display")
# 
# --- TIME PLAYBACK CONTROL BAR (At bottom of page) ---
st.markdown('<div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #262730;">', unsafe_allow_html=True)

# Buttons and slider row
btn_col1, btn_col2, btn_col3, btn_col4, btn_col5, slider_col = st.columns([0.5, 0.5, 0.5, 0.5, 0.5, 12], gap="small")

with btn_col1:
    if st.button("⏮️", key="start_btn", help="Start"):
        st.session_state.current_candle_index = 1
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = False
        st.rerun()

with btn_col2:
    if st.button("⏪", key="prev_btn", disabled=(st.session_state.current_candle_index <= 1), help="Previous"):
        st.session_state.current_candle_index = max(st.session_state.current_candle_index - 1, 1)
        st.session_state.is_playing = False
        st.rerun()

with btn_col3:
    if st.session_state.is_realtime_mode:
        st.button("▶️", key="play_disabled", disabled=True, help="Play")
    elif st.session_state.is_playing:
        if st.button("⏸️", key="pause_btn", help="Pause"):
            st.session_state.is_playing = False
            st.rerun()
    else:
        if st.button("▶️", key="play_btn", help="Play"):
            st.session_state.is_playing = True
            st.rerun()

with btn_col4:
    if st.button("⏭️", key="next_btn", disabled=(st.session_state.current_candle_index >= len(timestamps)), help="Next"):
        st.session_state.current_candle_index = min(st.session_state.current_candle_index + 1, len(timestamps))
        st.session_state.is_playing = False
        st.rerun()

with btn_col5:
    # ❌ REALTIME DISABLED: Refresh and Live buttons disabled
    # if st.session_state.is_realtime_mode:
    #     # In realtime mode: Refresh button
    #     if st.button("🔄", key="refresh_btn", help="Refresh"):
    #         st.rerun()
    # else:
    #     # In historical mode: Live button
    if st.button("⏩", key="live_btn", help="Live"):
        st.session_state.current_candle_index = len(timestamps)
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
        st.rerun()

with slider_col:
    # Time display above slider
    if st.session_state.is_realtime_mode:
        mode_text = '🔴 Real-time Mode'
    else:
        mode_text = '📼 Historical Mode'
    
    # Get current time (end time of current candle)
    if len(timestamps) > 0:
        if st.session_state.current_candle_index > 0 and st.session_state.current_candle_index <= len(timestamps):
            current_time = timestamps[st.session_state.current_candle_index - 1]
        else:
            current_time = timestamps[0]
        
        last_time = timestamps[-1]
        
        st.markdown(f'<div style="color: #26a69a; font-size: 16px; font-weight: 600; margin-bottom: -5px;">{mode_text} | {current_time.strftime("%Y-%m-%d %H:%M")} / {last_time.strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    
    # Slider for time navigation
    new_index = st.slider("Time Navigation", 1, len(timestamps), st.session_state.current_candle_index, label_visibility="collapsed")
    
    if new_index != st.session_state.current_candle_index:
        st.session_state.current_candle_index = new_index
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = (new_index >= len(timestamps))
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# ❌ REALTIME DISABLED: No auto-rerun or background updates
# ✅ Realtime mode: NO AUTO-RERUN
# User clicks "Refresh" button or manually triggers update
# DataLoader cache is automatically updated by LISTEN/NOTIFY background thread
# Charts will show updated data on next manual rerun (button click, slider change, etc.)

# Playback logic
if st.session_state.is_playing and not st.session_state.is_realtime_mode:
    st.session_state.current_candle_index = min(st.session_state.current_candle_index + 1, len(timestamps))
    if st.session_state.current_candle_index >= len(timestamps):
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
    time.sleep(0.2)
    st.rerun()
