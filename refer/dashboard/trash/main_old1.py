"""
Streamlit Dashboard for Index Price Klines
Display charts using ECharts
"""
import streamlit as st
from streamlit_echarts_zoom import st_echarts_zoom
from data_loader import DataLoader
from datetime import datetime, timedelta
import pandas as pd
import time

# Page configuration
st.set_page_config(
    page_title="OKX BTC-USDT Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for playback controls
if 'is_playing' not in st.session_state:
    st.session_state.is_playing = False
if 'playback_speed' not in st.session_state:
    st.session_state.playback_speed = 1
if 'current_candle_index' not in st.session_state:
    st.session_state.current_candle_index = None
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = time.time()
if 'is_realtime_mode' not in st.session_state:
    st.session_state.is_realtime_mode = True
if 'last_interval' not in st.session_state:
    st.session_state.last_interval = None

# Initialize global cache for all intervals
if 'global_cache_initialized' not in st.session_state:
    st.session_state.global_cache_initialized = False
if 'global_klines_cache' not in st.session_state:
    st.session_state.global_klines_cache = {}
if 'global_spread_cache' not in st.session_state:
    st.session_state.global_spread_cache = {}
if 'cache_last_update' not in st.session_state:
    st.session_state.cache_last_update = {}

# Initialize data loader with error handling
@st.cache_resource
def get_data_loader():
    try:
        return DataLoader()
    except Exception as e:
        st.error(f"Failed to initialize database connection: {str(e)}")
        st.stop()

try:
    loader = get_data_loader()
except:
    st.error("Cannot connect to database. Please check if TimescaleDB is running.")
    st.code("docker-compose -f docker/docker-compose.infrastructure.yml up -d")
    st.stop()

# Function to initialize global cache with all intervals
def initialize_global_cache(loader, max_records=500):
    """
    Preload all intervals data into global cache
    Returns True if successful, False otherwise
    """
    intervals = ['5m', '15m', '1h', '4h', '1d']
    
    progress_text = st.empty()
    progress_bar = st.progress(0)
    
    try:
        for idx, interval in enumerate(intervals):
            progress_text.text(f"Loading {interval} data...")
            
            # Load klines data
            df_klines = loader.get_latest_records(interval=interval, n=max_records)
            if df_klines is not None and len(df_klines) > 0:
                st.session_state.global_klines_cache[interval] = df_klines
                st.session_state.cache_last_update[interval] = time.time()
            
            # Load basis spread data
            df_spread = loader.get_basis_spread(interval=interval, n=max_records)
            if df_spread is not None and len(df_spread) > 0:
                df_spread = df_spread.sort_values('time', ascending=True).reset_index(drop=True)
                st.session_state.global_spread_cache[interval] = df_spread
            
            # Update progress
            progress_bar.progress((idx + 1) / len(intervals))
        
        progress_text.text("✅ All data loaded successfully!")
        time.sleep(0.5)
        progress_text.empty()
        progress_bar.empty()
        
        return True
        
    except Exception as e:
        progress_text.empty()
        progress_bar.empty()
        st.error(f"Failed to initialize cache: {str(e)}")
        return False

# Initialize cache on first run
if not st.session_state.global_cache_initialized:
    st.info("🔄 Initializing cache... This will only happen once.")
    if initialize_global_cache(loader):
        st.session_state.global_cache_initialized = True
        st.rerun()
    else:
        st.stop()

# UTC Time display with JavaScript real-time clock
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

# Sidebar configuration
interval = st.sidebar.selectbox("Interval", options=['5m', '15m', '1h', '4h', '1d'], index=0)
data_range_option = st.sidebar.radio("Range", options=['Record', 'Date'], index=0)

# Reset playback when interval changes
if st.session_state.last_interval != interval:
    st.session_state.current_candle_index = None
    st.session_state.is_playing = False
    st.session_state.is_realtime_mode = True
    st.session_state.last_interval = interval

if data_range_option == 'Record':
    try:
        stats = loader.get_statistics(interval=interval)
    except (Exception) as e:
        st.sidebar.error(f"Database error: {str(e)}")
        st.sidebar.info("Try clicking '🔄 Reconnect Database' button above")
        st.stop()
    max_available = stats.get('total_records', 1000)
    n_candles = st.sidebar.slider("Number of records", 50, max_available, min(200, max_available), 50)
else:
    min_date, max_date = loader.get_available_date_range(interval=interval)
    if min_date and max_date:
        st.sidebar.info(f"{min_date.strftime('%Y/%m/%d')} - {max_date.strftime('%Y/%m/%d')}")
        default_start = max(min_date, max_date - timedelta(days=7))
        start_date = st.sidebar.date_input("Start Date", default_start.date(), min_value=min_date.date(), max_value=max_date.date())
        end_date = st.sidebar.date_input("End Date", max_date.date(), min_value=min_date.date(), max_value=max_date.date())
    else:
        st.sidebar.error("No data"); st.stop()

# Use global cache for data loading
try:
    if data_range_option == 'Record':
        # Get from global cache and slice to requested number
        if interval in st.session_state.global_klines_cache:
            df_full = st.session_state.global_klines_cache[interval].tail(n_candles).reset_index(drop=True)
        else:
            st.warning(f"No data available for {interval}"); st.stop()
    else:
        # For date range, filter from global cache
        if interval in st.session_state.global_klines_cache:
            df_cache = st.session_state.global_klines_cache[interval]
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            # Filter by date range
            df_full = df_cache[
                (df_cache['timestamp_dt'] >= start_datetime) & 
                (df_cache['timestamp_dt'] <= end_datetime)
            ].reset_index(drop=True)
            
            if len(df_full) == 0:
                st.warning("No data in selected date range"); st.stop()
        else:
            st.warning(f"No data available for {interval}"); st.stop()
        
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Please check database connection and try again.")
    st.stop()

# Initialize playback index
if st.session_state.current_candle_index is None or st.session_state.current_candle_index > len(df_full):
    st.session_state.current_candle_index = len(df_full)
    st.session_state.is_realtime_mode = True

# Check real-time mode
st.session_state.is_realtime_mode = (st.session_state.current_candle_index >= len(df_full))

# Slice data for playback
df = df_full.iloc[:st.session_state.current_candle_index].copy()

# Create a unique key for current chart state
chart_state_key = f"{interval}_{st.session_state.current_candle_index}_{len(df_full)}"

# Initialize cache dictionaries (one entry per unique state)
if 'chart_data_cache' not in st.session_state:
    st.session_state.chart_data_cache = {}

# Check if this specific state is already cached
if chart_state_key in st.session_state.chart_data_cache:
    # Use cached data for this state
    chart_data = st.session_state.chart_data_cache[chart_state_key]
else:
    # Prepare new data for this state
    chart_data = df[['time', 'open', 'high', 'low', 'close']].copy()
    chart_data['time'] = chart_data['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Cache it for future use
    st.session_state.chart_data_cache[chart_state_key] = chart_data
    
    # Limit cache size (keep only last 50 states)
    if len(st.session_state.chart_data_cache) > 50:
        # Remove oldest entry
        oldest_key = next(iter(st.session_state.chart_data_cache))
        del st.session_state.chart_data_cache[oldest_key]

# --- CSS STYLING --- TEMPORARILY DISABLED FOR TESTING
st.markdown("""
<style>
    /* MINIMAL CSS FOR TESTING */
    
    /* 2. Tùy chỉnh thanh Metrics bên dưới (Bo tròn 2 góc dưới) */
    .metrics-container {
        background-color: #1E1E1E;
        color: #D1D4DC;
        padding: 15px 20px; /* Padding vừa phải cho khung nhỏ */
        border-bottom-left-radius: 15px;
        border-bottom-right-radius: 15px;
        font-family: 'Arial', sans-serif;
        display: flex;
        flex-wrap: wrap; /* Cho phép xuống dòng nếu quá chật */
        justify-content: space-between;
        align-items: center;
        gap: 10px; /* Khoảng cách giữa các chỉ số */
    }

    /* Style cho từng chỉ số */
    .metric-item {
        text-align: left;
        min-width: 60px; /* Đảm bảo không bị co quá nhỏ */
    }
    
    .metric-label {
        font-size: 11px;
        color: #787B86;
        margin-bottom: 2px;
    }
    
    .metric-value {
        font-size: 16px; /* Giảm font size một chút cho vừa khung 1/3 */
        font-weight: 600;
        color: #E0E3EB;
    }

    .metric-change-pos { color: #26a69a; font-size: 12px; font-weight: normal; }
    .metric-change-neg { color: #ef5350; font-size: 12px; font-weight: normal; }
    
    /* Time display */
    .time-display {
        font-size: 16px;
        font-weight: 600;
        color: #26a69a;
    }
    
    /* Hide copy buttons */
    button[title="Copy to clipboard"],
    [data-testid="stHeaderActionElements"] {
        display: none !important;
    }
    
    /* Style for slider tooltip - will be updated by JS */
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

# Title (removed - cleaner look)
# st.markdown('<h1 style="font-size: 2rem; margin-top: 0.5rem; margin-bottom: 0.5rem;">📈 OKX BTC-USDT Dashboard</h1>', unsafe_allow_html=True)

# --- MAIN LAYOUT (1.05x Width) ---
# Chiều rộng: Cột 1 (1.05 phần), Cột 2 (0.95 phần - để trống)
col_main, col_space = st.columns([0.95, 0.95])

with col_main:
    st.markdown(f'<h3 style="font-size: 16px; font-weight: 600; margin-top: 0; margin-bottom: 0;">Index Price</h3>', unsafe_allow_html=True)

    # Tính toán các chỉ số
    close_price = df['close'].iloc[-1]
    open_price = df['open'].iloc[0]
    high_price = df['high'].max()
    low_price = df['low'].min()
    change_abs = close_price - open_price
    change_pct = (change_abs / open_price) * 100

    # Initialize processed data cache
    if 'candle_data_cache' not in st.session_state:
        st.session_state.candle_data_cache = {}
    if 'time_labels_cache' not in st.session_state:
        st.session_state.time_labels_cache = {}
    
    # Check if processed data for this state is cached
    if chart_state_key in st.session_state.candle_data_cache:
        # Use cached processed data
        candle_data = st.session_state.candle_data_cache[chart_state_key]
        time_labels = st.session_state.time_labels_cache[chart_state_key]
    else:
        # Prepare data for ECharts candlestick
        candle_data = []
        time_labels = []
        
        for _, row in chart_data.iterrows():
            candle_data.append([
                row['open'],
                row['close'],
                row['low'],
                row['high']
            ])
            
            # Format time directly
            if isinstance(row['time'], str):
                time_labels.append(row['time'][:16])  # Take YYYY-MM-DD HH:MM
            else:
                time_labels.append(pd.to_datetime(row['time']).strftime('%Y-%m-%d %H:%M'))
        
        # Cache processed data
        st.session_state.candle_data_cache[chart_state_key] = candle_data
        st.session_state.time_labels_cache[chart_state_key] = time_labels
        
        # Limit cache size
        if len(st.session_state.candle_data_cache) > 50:
            oldest_key = next(iter(st.session_state.candle_data_cache))
            del st.session_state.candle_data_cache[oldest_key]
            del st.session_state.time_labels_cache[oldest_key]
    
    # ECharts candlestick configuration
    candle_options = {
        "animation": False,
        "addDataAnimation": False,
        "backgroundColor": "#1E1E1E",
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "15%",
            "top": "5%",
            "containLabel": True
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            }
        },
        "dataZoom": [
            {
                "type": "inside",
                "start": 0,
                "end": 100,
                "zoomOnMouseWheel": True,
                "moveOnMouseMove": True
            },
            {
                "type": "slider",
                "start": 0,
                "end": 100,
                "height": 20,
                "bottom": 5,
                "borderColor": "#787B86",
                "fillerColor": "rgba(38, 166, 154, 0.2)",
                "handleStyle": {"color": "#787B86"},
                "textStyle": {"color": "#D1D4DC"}
            }
        ],
        "xAxis": {
            "type": "category",
            "data": time_labels,
            "axisLine": {"lineStyle": {"color": "#787B86"}},
            "axisLabel": {"color": "#D1D4DC", "fontSize": 10}
        },
        "yAxis": {
            "type": "value",
            "scale": True,
            "splitLine": {
                "lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}
            },
            "axisLine": {"lineStyle": {"color": "#787B86"}},
            "axisLabel": {"color": "#D1D4DC"}
        },
        "series": [
            {
                "name": "Index Price",
                "type": "candlestick",
                "data": candle_data,
                "itemStyle": {
                    "color": "#26a69a",
                    "color0": "#ef5350",
                    "borderColor": "#26a69a",
                    "borderColor0": "#ef5350"
                }
            }
        ]
    }
    
    # Debug: Log when chart is about to render
    # print(f"Rendering chart with {len(candle_data)} candles at index {st.session_state.current_candle_index}")
    
    # Render ECharts with zoom persistence (proper component)
    st_echarts_zoom(candle_options, height="270px", key="index_price_chart")

# --- BASIS SPREAD CHART (FINAL FIX - NO CRASH) ---
with col_main:
    st.markdown('<h3 style="font-size: 16px; font-weight: 600; margin-top: 20px; margin-bottom: 0;">Basis Spread</h3>', unsafe_allow_html=True)
    
    try:
        # Use global cache for basis spread
        if data_range_option == 'Record':
            # Get from global cache and slice to requested number
            if interval in st.session_state.global_spread_cache:
                df_spread_full = st.session_state.global_spread_cache[interval].tail(n_candles).reset_index(drop=True)
            else:
                df_spread_full = None
        else:
            # For date range, filter from global cache
            if interval in st.session_state.global_spread_cache:
                df_spread_cache = st.session_state.global_spread_cache[interval]
                start_datetime = datetime.combine(start_date, datetime.min.time())
                end_datetime = datetime.combine(end_date, datetime.max.time())
                
                # Filter by date range
                df_spread_full = df_spread_cache[
                    (df_spread_cache['time'] >= start_datetime) & 
                    (df_spread_cache['time'] <= end_datetime)
                ].reset_index(drop=True)
                
                if len(df_spread_full) == 0:
                    df_spread_full = None
            else:
                df_spread_full = None
        
        # Slice according to playback position
        if df_spread_full is not None and len(df_spread_full) > 0:
            if data_range_option == 'Record':
                df_spread = df_spread_full.iloc[:st.session_state.current_candle_index].copy()
            else:
                if st.session_state.current_candle_index <= len(df_full):
                    cutoff_time = df_full.iloc[st.session_state.current_candle_index - 1]['time']
                    df_spread = df_spread_full[df_spread_full['time'] <= cutoff_time].copy()
                else:
                    df_spread = df_spread_full.copy()
        else:
            df_spread = None
        
        if df_spread is not None and len(df_spread) > 0:
            # Create spread state key
            spread_state_key = f"{interval}_{st.session_state.current_candle_index}_{len(df_spread)}"
            
            # Initialize spread cache
            if 'spread_time_labels_cache' not in st.session_state:
                st.session_state.spread_time_labels_cache = {}
            if 'spread_values_cache' not in st.session_state:
                st.session_state.spread_values_cache = {}
            
            # Check if this spread state is cached
            if spread_state_key in st.session_state.spread_time_labels_cache:
                # Use cached data
                time_labels = st.session_state.spread_time_labels_cache[spread_state_key]
                values = st.session_state.spread_values_cache[spread_state_key]
            else:
                # Prepare data for ECharts
                spread_data = df_spread[['time', 'basis_spread']].copy()
                spread_data = spread_data.sort_values('time', ascending=True).reset_index(drop=True)
                
                # Format time for display
                time_labels = spread_data['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
                values = spread_data['basis_spread'].tolist()
                
                # Cache processed data
                st.session_state.spread_time_labels_cache[spread_state_key] = time_labels
                st.session_state.spread_values_cache[spread_state_key] = values
                
                # Limit cache size
                if len(st.session_state.spread_time_labels_cache) > 50:
                    oldest_key = next(iter(st.session_state.spread_time_labels_cache))
                    del st.session_state.spread_time_labels_cache[oldest_key]
                    del st.session_state.spread_values_cache[oldest_key]
            
            # ECharts configuration
            options = {
                "animation": False,
                "addDataAnimation": False,
                "backgroundColor": "#1E1E1E",
                "grid": {
                    "left": "3%",
                    "right": "4%",
                    "bottom": "15%",
                    "top": "10%",
                    "containLabel": True
                },
                "tooltip": {
                    "trigger": "axis",
                    "axisPointer": {
                        "type": "cross"
                    }
                },
                "dataZoom": [
                    {
                        "type": "inside",
                        "start": 0,
                        "end": 100,
                        "zoomOnMouseWheel": True,
                        "moveOnMouseMove": True
                    },
                    {
                        "type": "slider",
                        "start": 0,
                        "end": 100,
                        "height": 20,
                        "bottom": 5,
                        "borderColor": "#787B86",
                        "fillerColor": "rgba(38, 166, 154, 0.2)",
                        "handleStyle": {
                            "color": "#787B86"
                        },
                        "textStyle": {
                            "color": "#D1D4DC"
                        }
                    }
                ],
                "xAxis": {
                    "type": "category",
                    "data": time_labels,
                    "axisLine": {"lineStyle": {"color": "#787B86"}},
                    "axisLabel": {"color": "#D1D4DC", "fontSize": 10}
                },
                "yAxis": {
                    "type": "value",
                    "splitLine": {
                        "lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}
                    },
                    "axisLine": {"lineStyle": {"color": "#787B86"}},
                    "axisLabel": {"color": "#D1D4DC"}
                },
                "series": [
                    {
                        "name": "Basis Spread",
                        "type": "line",
                        "data": values,
                        "lineStyle": {"color": "rgba(66, 133, 244, 1)", "width": 1},
                        "areaStyle": {
                            "color": {
                                "type": "linear",
                                "x": 0, "y": 0, "x2": 0, "y2": 1,
                                "colorStops": [
                                    {"offset": 0, "color": "rgba(66, 133, 244, 0.4)"},
                                    {"offset": 1, "color": "rgba(66, 133, 244, 0.05)"}
                                ]
                            }
                        },
                        "smooth": False,
                        "symbol": "none",
                        "sampling": "lttb"
                    }
                ]
            }
            
            # Render ECharts with zoom persistence (proper component)
            st_echarts_zoom(options, height="200px", key="basis_spread_chart")

        else:
            st.warning("No basis spread data available")
    
    except Exception as e:
        st.error(f"Failed to load basis spread: {e}")

# --- TIME PLAYBACK CONTROL BAR (At bottom of page) ---
st.markdown('<div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #262730;">', unsafe_allow_html=True)

# Buttons and slider row (compact buttons, larger slider)
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
            st.session_state.playback_speed = 1
            st.rerun()

with btn_col4:
    if st.button("⏭️", key="next_btn", disabled=(st.session_state.current_candle_index >= len(df_full)), help="Next"):
        st.session_state.current_candle_index = min(st.session_state.current_candle_index + 1, len(df_full))
        st.session_state.is_playing = False
        st.rerun()

with btn_col5:
    if st.button("⏩", key="live_btn", help="Live"):
        st.session_state.current_candle_index = len(df_full)
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
        st.rerun()

with slider_col:
    # Time display above slider (reduced margin)
    if st.session_state.is_realtime_mode:
        mode_text = '🔴 Real-time Mode'
    else:
        mode_text = '📼 Historical Mode'
    
    # Get the time of the NEXT candle (the one being formed)
    # When current_candle_index = N, we show candles from 0 to N-1
    # The next candle's time = timestamp_dt of the last displayed candle (which is the end time of that candle)
    if st.session_state.current_candle_index > 0 and st.session_state.current_candle_index <= len(df_full):
        # The last candle in df is at index (current_candle_index - 1) in df_full
        # Its timestamp_dt is the START time of the next candle
        current_time = df_full.iloc[st.session_state.current_candle_index - 1]['timestamp_dt']
    else:
        current_time = df_full.iloc[0]['timestamp_dt']
    
    # Last time should also be timestamp_dt (time of the next candle after the last one)
    last_time = df_full.iloc[-1]['timestamp_dt']
    
    st.markdown(f'<div style="color: #26a69a; font-size: 16px; font-weight: 600; margin-bottom: -5px;">{mode_text} | {current_time.strftime("%Y-%m-%d %H:%M")} / {last_time.strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    
    # Slider for time navigation
    new_index = st.slider("Time Navigation", 1, len(df_full), st.session_state.current_candle_index, label_visibility="collapsed")
    
    if new_index != st.session_state.current_candle_index:
        st.session_state.current_candle_index = new_index
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = (new_index >= len(df_full))
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# Playback logic
if st.session_state.is_playing and not st.session_state.is_realtime_mode:
    st.session_state.current_candle_index = min(st.session_state.current_candle_index + st.session_state.playback_speed, len(df_full))
    if st.session_state.current_candle_index >= len(df_full):
        st.session_state.is_playing = False
        st.session_state.is_realtime_mode = True
    time.sleep(0.2)
    st.rerun()

# Background auto-update for ALL intervals (not just current one)
def update_all_intervals_cache(loader):
    """
    Check and update cache for all intervals with new records
    """
    intervals = ['5m', '15m', '1h', '4h', '1d']
    updated = False
    
    for interval in intervals:
        if interval in st.session_state.global_klines_cache:
            # Get last timestamp from cache
            df_cache = st.session_state.global_klines_cache[interval]
            last_ts = df_cache['ts_ms'].iloc[-1]
            
            # Query new records
            df_new = loader.get_records_after_timestamp(interval=interval, after_ts_ms=last_ts)
            
            if df_new is not None and len(df_new) > 0:
                # Append new records
                df_updated = pd.concat([df_cache, df_new], ignore_index=True)
                df_updated = df_updated.sort_values('ts_ms').reset_index(drop=True)
                
                # Keep last 500 records (configurable)
                if len(df_updated) > 500:
                    df_updated = df_updated.tail(500).reset_index(drop=True)
                
                # Update cache
                st.session_state.global_klines_cache[interval] = df_updated
                st.session_state.cache_last_update[interval] = time.time()
                updated = True
                
                # Also update spread cache
                if interval in st.session_state.global_spread_cache:
                    df_spread_cache = st.session_state.global_spread_cache[interval]
                    last_spread_ts = int(df_spread_cache['time'].iloc[-1].timestamp() * 1000)
                    
                    df_spread_new = loader.get_basis_spread_after_timestamp(interval=interval, after_ts_ms=last_spread_ts)
                    
                    if df_spread_new is not None and len(df_spread_new) > 0:
                        df_spread_updated = pd.concat([df_spread_cache, df_spread_new], ignore_index=True)
                        df_spread_updated = df_spread_updated.sort_values('time', ascending=True).reset_index(drop=True)
                        
                        if len(df_spread_updated) > 500:
                            df_spread_updated = df_spread_updated.tail(500).reset_index(drop=True)
                        
                        st.session_state.global_spread_cache[interval] = df_spread_updated
    
    return updated

# Real-time mode auto-refresh (ONLY if in realtime mode and Record mode)
if st.session_state.is_realtime_mode and data_range_option == 'Record':
    # Check every 2 seconds for new data (optimized for less rerun overhead)
    current_time_now = time.time()
    if current_time_now - st.session_state.last_update_time > 2.0:
        st.session_state.last_update_time = current_time_now
        
        # Update cache for all intervals in background
        cache_updated = update_all_intervals_cache(loader)
        
        # Check if current interval got new data
        current_cache_len = len(st.session_state.global_klines_cache[interval])
        if 'last_cache_len' not in st.session_state:
            st.session_state.last_cache_len = {}
        
        # Only rerun if current interval has new data
        if interval not in st.session_state.last_cache_len or st.session_state.last_cache_len[interval] != current_cache_len:
            # Current interval has new data - update display
            st.session_state.last_cache_len[interval] = current_cache_len
            st.session_state.current_candle_index = min(n_candles, current_cache_len)
            st.rerun()
    
    # Keep checking (rerun after 2s)
    time.sleep(2.0)
    st.rerun()