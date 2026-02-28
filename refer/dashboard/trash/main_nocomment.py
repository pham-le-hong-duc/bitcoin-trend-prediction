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
warnings.filterwarnings('ignore', message='.*ScriptRunContext.*')
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from data_loader import DataLoader
st.set_page_config(
    page_title="OKX BTC-USDT Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
<style>
    /* Hide copy buttons */
    button[title="Copy to clipboard"],
    [data-testid="stHeaderActionElements"] {
        display: none !important;
    }
    /* Style for slider tooltip */
    .stSlider [data-testid="stThumbValue"] {
        background-color:
        color:
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
@st.cache_resource
def get_data_loader():
    """
    Get cached DataLoader instance
    - Automatically loads ALL data on first access (parallel)
    - Listens for PostgreSQL notifications for instant updates
    - NO manual cache management needed!
    """
    return DataLoader(auto_init=True, enable_listener=False)
loader = get_data_loader()
if 'is_playing' not in st.session_state:
    st.session_state.is_playing = False
if 'current_candle_index' not in st.session_state:
    st.session_state.current_candle_index = None
if 'is_realtime_mode' not in st.session_state:
    st.session_state.is_realtime_mode = True
if 'last_interval' not in st.session_state:
    st.session_state.last_interval = None
if 'last_range_option' not in st.session_state:
    st.session_state.last_range_option = None
INTERVAL_TO_SECONDS = {
    '5m': 5 * 60,
    '15m': 15 * 60,
    '1h': 60 * 60,
    '4h': 4 * 60 * 60,
    '1d': 24 * 60 * 60
}
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
    current_time = start_date + timedelta(seconds=interval_seconds)
    for i in range(n_records):
        timestamps.append(current_time)
        current_time += timedelta(seconds=interval_seconds)
    return timestamps
START_DATE = datetime(2025, 1, 1, 0, 0, 0)
CURRENT_DATE = datetime.utcnow()
interval = st.sidebar.selectbox("Interval", options=['5m', '15m', '1h', '4h', '1d'], index=0)
data_range_option = st.sidebar.radio("Range", options=['Record', 'Date'], index=0)
if st.session_state.last_interval != interval or st.session_state.last_range_option != data_range_option:
    st.session_state.current_candle_index = None
    st.session_state.is_playing = False
    st.session_state.is_realtime_mode = True
    st.session_state.last_interval = interval
    st.session_state.last_range_option = data_range_option
max_records = calculate_max_records(interval, START_DATE, CURRENT_DATE)
if 'timestamps_cache' not in st.session_state:
    st.session_state.timestamps_cache = {}
if data_range_option == 'Record':
    st.sidebar.info(f"Max records: {max_records:,}")
    n_candles = st.sidebar.slider("Number of records", 50, max_records, 50, 50)
    if 'last_n_candles' not in st.session_state:
        st.session_state.last_n_candles = n_candles
    if st.session_state.last_n_candles != n_candles:
        st.session_state.current_candle_index = None
        st.session_state.is_realtime_mode = True
        st.session_state.is_playing = False
        st.session_state.last_n_candles = n_candles
    cache_key = f"all_{interval}_{START_DATE}_{max_records}"
    if cache_key not in st.session_state.timestamps_cache:
        st.session_state.timestamps_cache[cache_key] = generate_timestamps(interval, START_DATE, max_records)
    all_timestamps = st.session_state.timestamps_cache[cache_key]
    timestamps = all_timestamps[-n_candles:]
else:
    st.sidebar.info(f"{START_DATE.strftime('%Y/%m/%d')} - {CURRENT_DATE.strftime('%Y/%m/%d')}")
    default_start = max(START_DATE, CURRENT_DATE - timedelta(days=10))
    default_end = CURRENT_DATE
    start_date = st.sidebar.date_input("Start Date", default_start.date(), 
                                        min_value=START_DATE.date(), 
                                        max_value=CURRENT_DATE.date())
    end_date = st.sidebar.date_input("End Date", default_end.date(), 
                                      min_value=START_DATE.date(), 
                                      max_value=CURRENT_DATE.date())
    if 'last_start_date' not in st.session_state:
        st.session_state.last_start_date = start_date
    if 'last_end_date' not in st.session_state:
        st.session_state.last_end_date = end_date
    if st.session_state.last_start_date != start_date or st.session_state.last_end_date != end_date:
        st.session_state.current_candle_index = None
        st.session_state.is_realtime_mode = True
        st.session_state.is_playing = False
        st.session_state.last_start_date = start_date
        st.session_state.last_end_date = end_date
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    n_candles = calculate_max_records(interval, start_datetime, end_datetime)
    date_cache_key = f"date_{interval}_{start_datetime}_{n_candles}"
    if date_cache_key not in st.session_state.timestamps_cache:
        st.session_state.timestamps_cache[date_cache_key] = generate_timestamps(interval, start_datetime, n_candles)
    timestamps = st.session_state.timestamps_cache[date_cache_key]
if st.session_state.current_candle_index is None or st.session_state.current_candle_index > len(timestamps):
    st.session_state.current_candle_index = len(timestamps)
    st.session_state.is_realtime_mode = True
st.session_state.is_realtime_mode = (st.session_state.current_candle_index >= len(timestamps))
try:
    if st.session_state.is_realtime_mode:
        start_time = None
        end_time = None
    elif len(timestamps) > 0:
        start_time = timestamps[0]
        end_time = timestamps[-1]
    else:
        start_time = None
        end_time = None
    try:
        df_klines_full = loader.get_cached_klines(
            interval=interval, 
            check_update=False,
            start_time=start_time,
            end_time=None
        )
        df_spread_full = loader.get_cached_spread(
            interval=interval, 
            check_update=False,
            start_time=start_time,
            end_time=None
        )
        if st.session_state.is_realtime_mode and len(timestamps) > 0:
            n_records = len(timestamps)
            df_klines_full = df_klines_full.tail(n_records)
            df_spread_full = df_spread_full.tail(n_records)
    except TypeError as e:
        st.warning("⚠️ Old DataLoader API detected. Please restart the app to use new filtering.")
        df_klines_full = loader.get_cached_klines(interval=interval, check_update=False)
        df_spread_full = loader.get_cached_spread(interval=interval, check_update=False)
        if start_time is not None:
            df_klines_full = df_klines_full[df_klines_full['timestamp_dt'] >= start_time]
            df_spread_full = df_spread_full[df_spread_full['time'] >= start_time]
    if df_klines_full.empty:
        st.warning(f"No klines data available for {interval}")
        st.stop()
    if 'filter_cache' not in st.session_state:
        st.session_state.filter_cache = {}
    if st.session_state.current_candle_index > 0 and st.session_state.current_candle_index <= len(timestamps):
        cutoff_time = timestamps[st.session_state.current_candle_index - 1]
        filter_key = f"filter_{interval}_{st.session_state.current_candle_index}_{len(df_klines_full)}"
        if filter_key in st.session_state.filter_cache:
            cached_filter = st.session_state.filter_cache[filter_key]
            df_klines = cached_filter['klines']
            df_spread = cached_filter['spread']
        else:
            df_klines = df_klines_full[df_klines_full['timestamp_dt'] <= cutoff_time].copy()
            if not df_spread_full.empty:
                df_spread = df_spread_full[df_spread_full['time'] <= cutoff_time].copy()
            else:
                df_spread = pd.DataFrame()
            st.session_state.filter_cache[filter_key] = {
                'klines': df_klines,
                'spread': df_spread
            }
            if len(st.session_state.filter_cache) > 50:
                oldest_keys = list(st.session_state.filter_cache.keys())[:-50]
                for old_key in oldest_keys:
                    del st.session_state.filter_cache[old_key]
    else:
        df_klines = df_klines_full
        df_spread = df_spread_full
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()
st.markdown('<div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #262730;">', unsafe_allow_html=True)
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
    if st.button("⏩", key="live_btn", help="Live"):
        st.session_state.current_candle_index = len(timestamps)
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
        st.rerun()
with slider_col:
    if st.session_state.is_realtime_mode:
        mode_text = '🔴 Real-time Mode'
    else:
        mode_text = '📼 Historical Mode'
    if len(timestamps) > 0:
        if st.session_state.current_candle_index > 0 and st.session_state.current_candle_index <= len(timestamps):
            current_time = timestamps[st.session_state.current_candle_index - 1]
        else:
            current_time = timestamps[0]
        last_time = timestamps[-1]
        st.markdown(f'<div style="color: #26a69a; font-size: 16px; font-weight: 600; margin-bottom: -5px;">{mode_text} | {current_time.strftime("%Y-%m-%d %H:%M")} / {last_time.strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    new_index = st.slider("Time Navigation", 1, len(timestamps), st.session_state.current_candle_index, label_visibility="collapsed")
    if new_index != st.session_state.current_candle_index:
        st.session_state.current_candle_index = new_index
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = (new_index >= len(timestamps))
        st.rerun()
st.markdown('</div>', unsafe_allow_html=True)
if st.session_state.is_playing and not st.session_state.is_realtime_mode:
    st.session_state.current_candle_index = min(st.session_state.current_candle_index + 1, len(timestamps))
    if st.session_state.current_candle_index >= len(timestamps):
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
    time.sleep(0.2)
    st.rerun()
