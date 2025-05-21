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


def load_dataframe() -> pd.DataFrame:
    """Load markets into a DataFrame with computed columns."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    if not df.empty:
        if {"yesPrice", "noPrice"}.issubset(df.columns):
            df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
        df["hoursLeft"] = df.apply(hours_to_expiry, axis=1)
    return df


def filter_dataframe(
    df: pd.DataFrame,
    hide_sports: bool,
    categories: List[str],
    search: str,
    min_prob: float,
    min_hours: int,
    min_open_interest: int,
) -> pd.DataFrame:
    """Apply sidebar filters to the markets DataFrame."""
    df_filtered = df.copy()

    if hide_sports and "category" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["category"].str.lower() != "sports"]

    if categories and "category" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["category"].isin(categories)]

    if search:
        pattern = search.lower()
        question_match = df_filtered.get("question", "").str.contains(pattern, case=False, na=False)
        slug_match = df_filtered.get("slug", "").str.contains(pattern, case=False, na=False)
        df_filtered = df_filtered[question_match | slug_match]

    if "probability" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["probability"] >= min_prob]

    if "hoursLeft" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["hoursLeft"] >= min_hours]

    if "openInterest" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["openInterest"] >= min_open_interest]

    return df_filtered


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    """Sort markets according to the selected option."""
    mapping = {
        "24h volume": ("volume24hr", False),
        "openInterest": ("openInterest", False),
        "endDate asc": ("hoursLeft", True),
        "endDate desc": ("hoursLeft", False),
        "probability asc": ("probability", True),
        "probability desc": ("probability", False),
    }
    col, ascending = mapping.get(option, (None, False))
    if col and col in df.columns:
        return df.sort_values(col, ascending=ascending, ignore_index=True)
    return df


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total current available markets: {len(df)}")

    st.sidebar.header("Filters")
    search_text = st.sidebar.text_input("Search")
    categories = sorted(df.get("category", pd.Series(dtype=str)).dropna().unique().tolist())
    selected_categories = st.sidebar.multiselect("Categories", categories, default=categories)
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input(
        "Min openInterest USDC", value=1000, step=100
    )
    sort_options = [
        "24h volume",
        "openInterest",
        "endDate asc",
        "endDate desc",
        "probability asc",
        "probability desc",
    ]
    sort_by = st.sidebar.selectbox("Sort by", sort_options)

    if df.empty:
        st.info("No market data available.")
        return

    df_filtered = filter_dataframe(
        df,
        hide_sports,
        selected_categories,
        search_text,
        min_prob,
        min_hours,
        min_open_interest,
    )
    df_filtered = sort_dataframe(df_filtered, sort_by)

    columns = [
        col
        for col in [
            "question",
            "yesPrice",
            "noPrice",
            "probability",
            "hoursLeft",
            "openInterest",
            "volume24hr",
            "category",
        ]
        if col in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
