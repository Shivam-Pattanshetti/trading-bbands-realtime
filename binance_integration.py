# ============================================================
# Binance Integration (Public REST API – Streamlit Compatible)
# ============================================================

import os
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Optional Streamlit import for reading secrets
try:
    import streamlit as st
except ImportError:
    st = None

# ------------------------------------------------------------
# Load environment variables and Streamlit secrets
# ------------------------------------------------------------
load_dotenv()


def get_secret(key):
    """Get value from Streamlit secrets or .env file."""
    if st and "general" in st.secrets and key in st.secrets["general"]:
        return st.secrets["general"][key]
    return os.getenv(key)


# Load keys (kept for compatibility, not required for public API)
BINANCE_API_KEY = get_secret("BINANCE_API_KEY")
BINANCE_API_SECRET = get_secret("BINANCE_API_SECRET")


# ------------------------------------------------------------
# Fetch Historical Klines (Public API with incremental support)
# ------------------------------------------------------------
def fetch_klines(symbol="BTCUSDT", interval="1h", days=30, start_time=None, end_time=None):
    """
    Fetch historical candlestick (kline) data using Binance's public REST API.
    Supports both full-day fetch and incremental fetch via start_time/end_time.

    Args:
        symbol (str): Trading pair, e.g., "BTCUSDT"
        interval (str): Candle interval, e.g., "1h", "4h", "1d"
        days (int): Number of days to fetch if no start/end provided
        start_time (datetime, optional): UTC start time for incremental fetch
        end_time (datetime, optional): UTC end time for incremental fetch

    Returns:
        pd.DataFrame: ['open_time', 'open', 'high', 'low', 'close', 'volume']
    """
    try:
        base_url = "https://data-api.binance.vision/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": 1000}

        # Handle incremental vs. full fetch
        if start_time:
            params["startTime"] = int(pd.Timestamp(start_time).timestamp() * 1000)
        if end_time:
            params["endTime"] = int(pd.Timestamp(end_time).timestamp() * 1000)
        elif not start_time:
            # Default: last N days of data
            params["startTime"] = int(
                (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).timestamp() * 1000
            )

        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or len(data) == 0:
            print(f"[WARN] No data returned for {symbol}. Params: {params}")
            return pd.DataFrame()

        # Convert API response into DataFrame
        df = pd.DataFrame(
            data,
            columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "num_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore",
            ],
        )

        # Convert datatypes
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Return key columns
        df = df[["open_time", "open", "high", "low", "close", "volume"]]

        print(f"[OK] Data fetched for {symbol}: {len(df)} rows")
        return df

    except Exception as e:
        print(f"[ERROR] Public Binance API error for {symbol}: {e}")
        time.sleep(2)
        return pd.DataFrame()


# ------------------------------------------------------------
# Optional: Local test (run this file alone to verify data)
# ------------------------------------------------------------
if __name__ == "__main__":
    symbol = "BTCUSDT"
    print(f"Testing incremental fetch for {symbol}...\n")

    df = fetch_klines(symbol=symbol, interval="1h", days=2)
    print(f"Initial fetch: {len(df)} rows")

    if not df.empty:
        last_time = df["open_time"].max()
        df_new = fetch_klines(symbol=symbol, interval="1h", start_time=last_time + pd.Timedelta(hours=1))
        print(f"Incremental fetch: {len(df_new)} new rows")
