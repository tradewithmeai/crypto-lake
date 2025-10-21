"""
Crypto Data Lake - Test GUI

A minimal Streamlit dashboard for visualizing collected cryptocurrency data.
Reads directly from Parquet files via DuckDB with efficient filtering.

Features:
- Multi-timeframe candlestick charts (1s, 1m, 5m, 15m, 1h)
- Volume visualization
- Spread analysis
- Data quality metrics (gaps, continuity)
- Auto-refresh for overnight monitoring

Installation:
    pip install streamlit plotly duckdb pandas pyyaml

Usage:
    streamlit run gui/app.py --server.headless true

Tips:
- Enable auto-refresh with 60s interval for overnight monitoring
- If charts freeze, check collector logs and WebSocket connectivity
- All timestamps are in UTC
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple

import duckdb
import pandas as pd
import streamlit as st
import yaml
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ========================================
# Configuration & Data Access
# ========================================

@st.cache_data(ttl=60)
def load_config(path: str = "config.yml") -> Dict[str, Any]:
    """Load config.yml with caching."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_exchanges(config: Dict[str, Any]) -> List[str]:
    """Extract exchange names from config."""
    return [ex["name"] for ex in config.get("exchanges", [])]


def get_symbols(config: Dict[str, Any], exchange: str) -> List[str]:
    """Get symbol list for a given exchange."""
    for ex in config.get("exchanges", []):
        if ex["name"].lower() == exchange.lower():
            return ex.get("symbols", [])
    return []


def build_parquet_glob(config: Dict[str, Any], exchange: str, symbol: str) -> str:
    """Build glob pattern for Parquet files."""
    base = config["general"]["base_path"]
    # Pattern matches both partitioned and compacted files
    return os.path.join(base, "parquet", exchange, symbol, "**", "*.parquet")


# ========================================
# DuckDB Queries
# ========================================

@st.cache_data(ttl=30, show_spinner="Loading data...")
def query_bars(
    config: Dict[str, Any],
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    timeframe: str
) -> pd.DataFrame:
    """
    Query OHLCV bars from Parquet using DuckDB.

    Args:
        config: Loaded configuration
        exchange: Exchange name
        symbol: Trading pair symbol
        start: Start datetime (UTC)
        end: End datetime (UTC)
        timeframe: '1s', '1m', '5m', '15m', or '1h'

    Returns:
        DataFrame with columns: window_start, open, high, low, close,
                                volume_base, volume_quote, spread
    """
    glob_path = build_parquet_glob(config, exchange, symbol)

    # Convert to strings for DuckDB
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")

    con = duckdb.connect()
    try:
        if timeframe == "1s":
            # Direct 1-second data
            query = f"""
            SELECT
                window_start,
                open,
                high,
                low,
                close,
                volume_base,
                volume_quote,
                trade_count,
                vwap,
                bid,
                ask,
                spread
            FROM read_parquet('{glob_path}')
            WHERE window_start >= TIMESTAMP '{start_str}'
              AND window_start <= TIMESTAMP '{end_str}'
            ORDER BY window_start
            """
        else:
            # Aggregate on the fly
            tf_map = {
                "1m": "minute",
                "5m": "5 minutes",
                "15m": "15 minutes",
                "1h": "hour"
            }
            trunc_spec = tf_map.get(timeframe, "minute")

            query = f"""
            SELECT
                date_trunc('{trunc_spec}', window_start) AS window_start,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close,
                sum(volume_base) AS volume_base,
                sum(volume_quote) AS volume_quote,
                sum(trade_count) AS trade_count,
                CASE
                    WHEN sum(volume_base) > 0 THEN sum(volume_quote) / sum(volume_base)
                    ELSE last(close)
                END AS vwap,
                last(bid) AS bid,
                last(ask) AS ask,
                avg(spread) AS spread
            FROM read_parquet('{glob_path}')
            WHERE window_start >= TIMESTAMP '{start_str}'
              AND window_start <= TIMESTAMP '{end_str}'
            GROUP BY date_trunc('{trunc_spec}', window_start)
            ORDER BY window_start
            """

        df = con.execute(query).fetch_df()

        # Ensure datetime type
        if not df.empty and 'window_start' in df.columns:
            df['window_start'] = pd.to_datetime(df['window_start'], utc=True)

        return df
    finally:
        con.close()


def compute_continuity_metrics(df: pd.DataFrame, timeframe: str) -> Dict[str, Any]:
    """
    Compute data quality metrics.

    Args:
        df: DataFrame with window_start column
        timeframe: Expected interval

    Returns:
        Dictionary with metrics: total_rows, missing_gaps, first_ts, last_ts
    """
    if df.empty:
        return {
            "total_rows": 0,
            "missing_gaps": 0,
            "first_ts": None,
            "last_ts": None,
            "expected_rows": 0
        }

    total_rows = len(df)
    first_ts = df['window_start'].min()
    last_ts = df['window_start'].max()

    # Expected interval in seconds
    interval_map = {
        "1s": 1,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600
    }
    interval_sec = interval_map.get(timeframe, 1)

    # Compute time span
    time_span_sec = (last_ts - first_ts).total_seconds()
    expected_rows = int(time_span_sec / interval_sec) + 1

    # Count gaps (where diff > expected interval)
    ts_series = df['window_start'].sort_values()
    diffs_sec = ts_series.diff().dt.total_seconds()
    missing_gaps = int((diffs_sec > interval_sec * 1.5).sum())  # Allow 50% tolerance

    return {
        "total_rows": total_rows,
        "missing_gaps": missing_gaps,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "expected_rows": expected_rows,
        "completeness": f"{100.0 * total_rows / expected_rows:.1f}%" if expected_rows > 0 else "N/A"
    }


# ========================================
# Visualization
# ========================================

def make_candlestick_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> go.Figure:
    """
    Create Plotly candlestick chart with volume and spread.

    Args:
        df: DataFrame with OHLCV data
        symbol: Symbol name for title
        timeframe: Timeframe string for title

    Returns:
        Plotly Figure object
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available for selected range",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20, color="gray")
        )
        return fig

    # Create subplots: candlestick + volume, and optionally spread
    has_spread = 'spread' in df.columns and df['spread'].notna().any()
    row_heights = [0.6, 0.25, 0.15] if has_spread else [0.7, 0.3]
    rows = 3 if has_spread else 2

    specs = [[{"secondary_y": False}]] * rows
    subplot_titles = [f"{symbol} - {timeframe}", "Volume"]
    if has_spread:
        subplot_titles.append("Spread")

    fig = make_subplots(
        rows=rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
        specs=specs
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df['window_start'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name="OHLC",
            increasing_line_color='green',
            decreasing_line_color='red'
        ),
        row=1, col=1
    )

    # Volume bars
    colors = ['green' if c >= o else 'red'
              for c, o in zip(df['close'], df['open'])]

    fig.add_trace(
        go.Bar(
            x=df['window_start'],
            y=df['volume_base'],
            name="Volume",
            marker_color=colors,
            opacity=0.7
        ),
        row=2, col=1
    )

    # Spread (if available)
    if has_spread:
        fig.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['spread'],
                name="Spread",
                line=dict(color='orange', width=2),
                mode='lines'
            ),
            row=3, col=1
        )

    # Update layout
    fig.update_layout(
        height=800,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
        template='plotly_dark'
    )

    # Format axes
    fig.update_xaxes(title_text="Time (UTC)", row=rows, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    if has_spread:
        fig.update_yaxes(title_text="Spread", row=3, col=1)

    return fig


# ========================================
# Streamlit App
# ========================================

def main():
    st.set_page_config(
        page_title="Crypto Data Lake - Test GUI",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("📊 Crypto Data Lake - Test GUI")
    st.markdown("*Real-time visualization of collected cryptocurrency data*")

    # Load configuration
    try:
        config = load_config("config.yml")
    except Exception as e:
        st.error(f"Failed to load config.yml: {e}")
        st.stop()

    # ========================================
    # Sidebar Controls
    # ========================================

    with st.sidebar:
        st.header("⚙️ Controls")

        # Exchange selection
        exchanges = get_exchanges(config)
        if not exchanges:
            st.error("No exchanges found in config.yml")
            st.stop()

        exchange = st.selectbox(
            "Exchange",
            exchanges,
            index=0
        )

        # Symbol selection
        symbols = get_symbols(config, exchange)
        if not symbols:
            st.error(f"No symbols found for {exchange}")
            st.stop()

        symbol = st.selectbox(
            "Symbol",
            symbols,
            index=0
        )

        # Timeframe selection
        timeframe = st.selectbox(
            "Timeframe",
            ["1s", "1m", "5m", "15m", "1h"],
            index=1  # Default to 1m
        )

        st.markdown("---")

        # Date range (default to last 24 hours)
        now_utc = datetime.now(timezone.utc)
        default_start = now_utc - timedelta(hours=24)

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=default_start.date(),
                max_value=now_utc.date()
            )
        with col2:
            start_time = st.time_input(
                "Start Time (UTC)",
                value=default_start.time()
            )

        col3, col4 = st.columns(2)
        with col3:
            end_date = st.date_input(
                "End Date",
                value=now_utc.date(),
                max_value=now_utc.date()
            )
        with col4:
            end_time = st.time_input(
                "End Time (UTC)",
                value=now_utc.time()
            )

        # Combine date and time
        start_dt = datetime.combine(start_date, start_time, tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, end_time, tzinfo=timezone.utc)

        st.markdown("---")

        # Auto-refresh
        st.subheader("🔄 Auto-Refresh")
        auto_refresh = st.checkbox("Enable Auto-Refresh", value=False)

        if auto_refresh:
            refresh_interval = st.selectbox(
                "Refresh Interval",
                [15, 30, 60, 120, 300],
                format_func=lambda x: f"{x}s",
                index=2  # Default to 60s
            )
        else:
            refresh_interval = 60

        # Manual refresh button
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ========================================
    # Main Content
    # ========================================

    # Query data
    try:
        df = query_bars(config, exchange, symbol, start_dt, end_dt, timeframe)
    except Exception as e:
        st.error(f"Failed to query data: {e}")
        st.stop()

    if df.empty:
        st.warning(f"No data found for {symbol} on {exchange} between {start_dt} and {end_dt}")
        st.info("💡 Try adjusting the date range or check if the collector is running.")
        st.stop()

    # ========================================
    # Health Metrics
    # ========================================

    st.header("📊 Data Health")

    metrics = compute_continuity_metrics(df, timeframe)

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Rows", f"{metrics['total_rows']:,}")

    with col2:
        st.metric("Expected Rows", f"{metrics['expected_rows']:,}")

    with col3:
        st.metric("Completeness", metrics['completeness'])

    with col4:
        gap_color = "🟢" if metrics['missing_gaps'] == 0 else "🔴"
        st.metric(f"{gap_color} Missing Gaps", metrics['missing_gaps'])

    with col5:
        if metrics['first_ts']:
            duration = metrics['last_ts'] - metrics['first_ts']
            hours = duration.total_seconds() / 3600
            st.metric("Time Span", f"{hours:.1f}h")
        else:
            st.metric("Time Span", "N/A")

    # ========================================
    # Chart
    # ========================================

    st.header("📈 Price Chart")

    fig = make_candlestick_chart(df, symbol, timeframe)
    st.plotly_chart(fig, use_container_width=True)

    # ========================================
    # Data Snapshot
    # ========================================

    st.header("📋 Data Snapshot")

    tab1, tab2, tab3 = st.tabs(["📊 Summary Stats", "🔝 First 10 Rows", "🔽 Last 10 Rows"])

    with tab1:
        if not df.empty:
            summary = pd.DataFrame({
                "Metric": ["Open", "High", "Low", "Close", "Volume (Base)", "Volume (Quote)"],
                "Min": [
                    df['open'].min(),
                    df['high'].min(),
                    df['low'].min(),
                    df['close'].min(),
                    df['volume_base'].min(),
                    df['volume_quote'].min()
                ],
                "Max": [
                    df['open'].max(),
                    df['high'].max(),
                    df['low'].max(),
                    df['close'].max(),
                    df['volume_base'].max(),
                    df['volume_quote'].max()
                ],
                "Mean": [
                    df['open'].mean(),
                    df['high'].mean(),
                    df['low'].mean(),
                    df['close'].mean(),
                    df['volume_base'].mean(),
                    df['volume_quote'].mean()
                ]
            })
            st.dataframe(summary, use_container_width=True, hide_index=True)

    with tab2:
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)

    with tab3:
        st.dataframe(df.tail(10), use_container_width=True, hide_index=True)

    # ========================================
    # Download
    # ========================================

    st.header("💾 Export")

    col1, col2 = st.columns(2)

    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{exchange}_{symbol}_{timeframe}_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col2:
        st.info(f"📦 Data size: {len(csv):,} bytes")

    # ========================================
    # Auto-refresh logic
    # ========================================

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

    # ========================================
    # Footer
    # ========================================

    st.markdown("---")
    st.markdown(
        f"*Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC* | "
        f"[GitHub](https://github.com/tradewithmeai/crypto-lake)"
    )


if __name__ == "__main__":
    main()
