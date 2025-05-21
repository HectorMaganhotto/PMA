import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
import pandas as pd
import streamlit as st

# API configuration
MARKETS_URL = "https://gamma-api.polymarket.com/markets"
MARKETS_PARAMS = {"limit": 500, "archived": "false"}
CACHE_TTL = 60  # seconds

_cache_data: List[Dict[str, Any]] | None = None
_cache_timestamp: float = 0.0


async def fetch_markets() -> List[Dict[str, Any]]:
    """Fetch market data from the Gamma API with simple in-memory caching."""
    global _cache_data, _cache_timestamp
    now = time.time()
    if _cache_data is not None and now - _cache_timestamp < CACHE_TTL:
        return _cache_data

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(MARKETS_URL, params=MARKETS_PARAMS)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict):
        markets = data.get("markets") or data.get("data") or []
    else:
        markets = data

    _cache_data = markets
    _cache_timestamp = now
    return markets


def hours_to_expiry(market: Dict[str, Any]) -> float:
    """Return the remaining hours until market resolution."""
    date_str = (
        market.get("endsAt")
        or market.get("endDate")
        or market.get("expiry"))
    if not isinstance(date_str, str) or not date_str:
        return 0.0
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def load_dataframe() -> pd.DataFrame:
    """Load markets into a DataFrame with computed columns."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    if not df.empty:
        df["hoursToExpiry"] = df.apply(hours_to_expiry, axis=1)
        yes = (
            pd.to_numeric(df["yesPrice"], errors="coerce")
            if "yesPrice" in df.columns
            else None
        )
        no = (
            pd.to_numeric(df["noPrice"], errors="coerce")
            if "noPrice" in df.columns
            else None
        )
        cols = [c for c in [yes, no] if c is not None]
        if cols:
            df["probability"] = pd.concat(cols, axis=1).max(axis=1, skipna=True)
    return df


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    st.sidebar.header("Filters")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input(
        "Min openInterest USDC", value=1000, step=100
    )

    df = load_dataframe()
    if df.empty:
        st.info("No market data available.")
        return
    st.write(f"Total current available markets: {len(df)}")

    df_filtered = df.copy()
    if "probability" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["probability"] >= min_prob]

    if "hoursToExpiry" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["hoursToExpiry"] >= min_hours]

    if "openInterest" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["openInterest"] >= min_open_interest]

    columns = [
        col
        for col in [
            "question",
            "yesPrice",
            "noPrice",
            "probability",
            "hoursToExpiry",
            "openInterest",
        ]
        if col in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
