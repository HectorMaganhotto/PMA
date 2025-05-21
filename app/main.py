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
        or market.get("expiry")
    )
    if not isinstance(date_str, str) or not date_str:
        return -1.0
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return -1.0
    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with computed helper columns."""
    df = df.copy()
    df["yesPrice"] = pd.to_numeric(df.get("yesPrice"), errors="coerce")
    df["noPrice"] = pd.to_numeric(df.get("noPrice"), errors="coerce")
    df["openInterest"] = pd.to_numeric(df.get("openInterest"), errors="coerce")
    df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    df["hoursLeft"] = df.apply(hours_to_expiry, axis=1)
    return df


def load_dataframe() -> pd.DataFrame:
    """Load markets into a DataFrame with computed columns."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    if df.empty:
        return df

    return normalize_dataframe(df)


def filter_dataframe(
    df: pd.DataFrame,
    *,
    min_prob: float,
    min_hours: int,
    min_open_interest: int,
    disable_prob: bool,
    disable_hours: bool,
    disable_open: bool,
) -> pd.DataFrame:
    """Apply filters to the DataFrame with debug output."""
    result = df.copy()
    st.write(f"Rows before filters: {len(result)}")
    if not disable_prob and "probability" in result.columns:
        before = len(result)
        result = result[result["probability"].fillna(-1) >= min_prob]
        st.write(f"After prob filter: {len(result)} (dropped {before - len(result)})")

    if not disable_hours and "hoursLeft" in result.columns:
        before = len(result)
        result = result[result["hoursLeft"].fillna(-1) >= min_hours]
        st.write(f"After hours filter: {len(result)} (dropped {before - len(result)})")

    if not disable_open and "openInterest" in result.columns:
        before = len(result)
        result = result[result["openInterest"].fillna(-1) >= min_open_interest]
        st.write(
            f"After open interest filter: {len(result)} (dropped {before - len(result)})"
        )

    return result


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    st.sidebar.header("Filters")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    disable_prob = st.sidebar.checkbox("Disable prob filter")
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    disable_hours = st.sidebar.checkbox("Disable hours filter")
    min_open_interest = st.sidebar.number_input(
        "Min openInterest USDC", value=1000, step=100
    )
    disable_open = st.sidebar.checkbox("Disable open interest filter")

    df = load_dataframe()
    st.subheader(f"Total current available markets: {len(df)}")
    st.write(
        f"prob>={min_prob}, hours>={min_hours}, openInt>={min_open_interest}"
    )
    if df.empty:
        st.info("No market data available.")
        return
    df_filtered = filter_dataframe(
        df,
        min_prob=min_prob,
        min_hours=min_hours,
        min_open_interest=min_open_interest,
        disable_prob=disable_prob,
        disable_hours=disable_hours,
        disable_open=disable_open,
    )

    columns = [
        col
        for col in [
            "question",
            "yesPrice",
            "noPrice",
            "probability",
            "hoursLeft",
            "openInterest",
        ]
        if col in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
