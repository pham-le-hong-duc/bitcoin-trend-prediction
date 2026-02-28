"""
Streamlit Dashboard - Real-time Mode Only
Simple real-time data visualization without playback controls
Auto-updates when new data arrives via PostgreSQL NOTIFY
"""
import streamlit as st
import streamlit.components.v1 as components
from streamlit_echarts_zoom import st_echarts_zoom
from data_loader import DataLoader
import pandas as pd
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="OKX BTC-USDT Real-time Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Styling
st.markdown("""
<style>
    /* Hide copy buttons */
    button[title="Copy to clipboard"],
    [data-testid="stHeaderActionElements"] {
        display: none !important;
    }
    /* Real-time indicator pulsing animation */
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    .realtime-indicator {
        animation: pulse 2s ease-in-out infinite;
    }
</style>
""", unsafe_allow_html=True)

# Real-time status indicator
st.sidebar.markdown("""
<div style="
    background-color: #1a472a;
    padding: 10px;
    border-radius: 5px;
    text-align: center;
    border: 1px solid #26a69a;
    margin-bottom: 10px;
">
    <div style="font-size: 11px; color: #9CA3AF; margin-bottom: 4px;">STATUS</div>
    <div class="realtime-indicator" style="font-size: 14px; font-weight: 600; color: #26a69a;">
        🟢 REAL-TIME ACTIVE
    </div>
    <div style="font-size: 10px; color: #9CA3AF; margin-top: 4px;">
        Auto-updating via NOTIFY
    </div>
</div>
""", unsafe_allow_html=True)

# ============================================================================
# LOCAL TIME CLOCK (Sidebar)
# ============================================================================
components.html("""
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

# ============================================================================
# SIDEBAR CONTROLS
# ============================================================================
# Interval selector
interval = st.sidebar.selectbox(
    "Interval",
    options=['5m', '15m', '1h', '4h', '1d'],
    index=0,
    help="Select time interval for candlestick data"
)

# Number of records to display
n_display = st.sidebar.number_input(
    "Number of records to display",
    min_value=50,
    max_value=10000,
    value=500,
    step=50,
    help="Number of latest records to display (set to max available or use slider)"
)

# Option to display all data
display_all = st.sidebar.checkbox(
    "Display all available data",
    value=False,
    help="Display all data instead of limiting to N records"
)

# ============================================================================
# INITIALIZE DATA LOADER
# ============================================================================
@st.cache_resource
def get_data_loader():
    """Initialize DataLoader with cache and real-time listener"""
    loader = DataLoader(auto_init=True, enable_listener=True)
    
    # Register callback for real-time updates
    def on_data_update(updated_interval: str):
        """Callback when data is updated - trigger Streamlit rerun"""
        print(f"🔄 Data updated for {updated_interval}, triggering rerun...")
        st.rerun()
    
    loader.register_update_callback(on_data_update)
    return loader

# Load DataLoader (cached across reruns)
loader = get_data_loader()

# ============================================================================
# MAIN CONTENT
# ============================================================================
# Display current interval and data info
st.write(f"**Selected Interval:** {interval}")

# ============================================================================
# LOAD DATA FROM CACHE
# ============================================================================
# Load ALL data from cache (cache is already loaded and updated by listener)
df_klines = loader.get_cached_klines(interval=interval, check_update=False)
df_spread = loader.get_cached_spread(interval=interval, check_update=False)

# Display data statistics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Klines Records", f"{len(df_klines):,}")
with col2:
    st.metric("Total Spread Records", f"{len(df_spread):,}")
with col3:
    if not df_klines.empty:
        last_update = df_klines['timestamp_dt'].iloc[-1]
        # Keep as UTC
        if last_update.tzinfo is None:
            # If timezone-naive, assume UTC
            last_update = pd.Timestamp(last_update, tz='UTC')
        st.metric("Last Update (UTC)", last_update.strftime('%Y-%m-%d %H:%M:%S'))

# Prepare display data based on user selection
if display_all:
    df_klines_display = df_klines.copy()
    df_spread_display = df_spread.copy()
    st.info(f"Displaying all {len(df_klines):,} records")
else:
    if len(df_klines) > n_display:
        df_klines_display = df_klines.tail(n_display).copy()
    else:
        df_klines_display = df_klines.copy()
    
    if len(df_spread) > n_display:
        df_spread_display = df_spread.tail(n_display).copy()
    else:
        df_spread_display = df_spread.copy()
    
    st.info(f"Displaying last {len(df_klines_display):,} of {len(df_klines):,} records")

# ============================================================================
# CHART 1: INDEX PRICE CANDLESTICK
# ============================================================================
st.markdown('<h3 style="font-size: 16px; font-weight: 600; margin-top: 20px; margin-bottom: 10px;">Index Price</h3>', unsafe_allow_html=True)

if not df_klines_display.empty:
    # Prepare data for ECharts
    candle_data = df_klines_display[['open', 'close', 'low', 'high']].values.tolist()
    
    # Keep time as UTC for display
    time_labels = df_klines_display['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    
    # ECharts candlestick configuration
    candle_options = {
        "animation": False,
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
            "axisPointer": {"type": "cross"}
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
            "splitLine": {"lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}},
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
    
    st_echarts_zoom(candle_options, height="350px", key="index_price_chart")
else:
    st.warning("No klines data available")

# ============================================================================
# CHART 2: BASIS SPREAD LINE CHART
# ============================================================================
st.markdown('<h3 style="font-size: 16px; font-weight: 600; margin-top: 20px; margin-bottom: 10px;">Basis Spread</h3>', unsafe_allow_html=True)

if not df_spread_display.empty:
    # Prepare data for ECharts
    
    # Keep time as UTC for display
    spread_time_labels = df_spread_display['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    spread_values = df_spread_display['basis_spread'].tolist()
    
    # ECharts line chart configuration
    spread_options = {
        "animation": False,
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
            "axisPointer": {"type": "cross"}
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
            "data": spread_time_labels,
            "axisLine": {"lineStyle": {"color": "#787B86"}},
            "axisLabel": {"color": "#D1D4DC", "fontSize": 10}
        },
        "yAxis": {
            "type": "value",
            "splitLine": {"lineStyle": {"color": "rgba(120, 123, 134, 0.2)"}},
            "axisLine": {"lineStyle": {"color": "#787B86"}},
            "axisLabel": {"color": "#D1D4DC"}
        },
        "series": [
            {
                "name": "Basis Spread",
                "type": "line",
                "data": spread_values,
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
    
    st_echarts_zoom(spread_options, height="250px", key="basis_spread_chart")
else:
    st.warning("No spread data available")

# ============================================================================
# FOOTER INFO
# ============================================================================
st.markdown("---")
current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
st.markdown(f"""
<div style="text-align: center; color: #9CA3AF; font-size: 11px; padding: 10px;">
    <strong>Real-time Dashboard</strong> | Data updates automatically via PostgreSQL NOTIFY | 
    Last refresh (UTC): {current_time}
</div>
""", unsafe_allow_html=True)
