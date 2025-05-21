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


def hours_left(market: Dict[str, Any]) -> float:
    """Return remaining hours until market resolution or -1 if unknown."""
    date_str = (
        market.get("endDate")
        or market.get("endsAt")
        or market.get("expiry")
    )
    if not isinstance(date_str, str) or not date_str:
        return -1.0
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return -1.0
    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add probability and hoursLeft columns to the DataFrame."""
    if df.empty:
        return df
    for col in ["yesPrice", "noPrice", "openInterest", "volume24h"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if {"yesPrice", "noPrice"}.issubset(df.columns):
        df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    else:
        df["probability"] = pd.NA
    df["hoursLeft"] = df.apply(hours_left, axis=1)
    return df


def load_dataframe() -> pd.DataFrame:
    """Load markets and normalize the DataFrame."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    return normalize_dataframe(df)


def filter_dataframe(
    df: pd.DataFrame,
    search: str,
    categories: List[str],
    hide_sports: bool,
    min_prob: float,
    min_hours: int,
    min_open_interest: int,
) -> pd.DataFrame:
    df_filtered = df.copy()

    if hide_sports and "category" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["category"].str.lower() != "sports"]

    if categories and "category" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["category"].isin(categories)]

    if search:
        mask = df_filtered["question"].str.contains(search, case=False, na=False)
        if "slug" in df_filtered.columns:
            mask |= df_filtered["slug"].str.contains(search, case=False, na=False)
        df_filtered = df_filtered[mask]

    if "probability" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["probability"] >= min_prob]
    if "hoursLeft" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["hoursLeft"] >= min_hours]
    if "openInterest" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["openInterest"] >= min_open_interest]

    return df_filtered


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    if option == "24h volume" and "volume24h" in df.columns:
        return df.sort_values("volume24h", ascending=False)
    if option == "openInterest" and "openInterest" in df.columns:
        return df.sort_values("openInterest", ascending=False)
    if option == "endDate asc":
        return df.sort_values("hoursLeft")
    if option == "endDate desc":
        return df.sort_values("hoursLeft", ascending=False)
    if option == "probability asc":
        return df.sort_values("probability")
    if option == "probability desc":
        return df.sort_values("probability", ascending=False)
    return df


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total current available markets: {len(df)}")
    if df.empty:
        st.info("No market data available.")
        return

    category_options: List[str] = []
    if "category" in df.columns:
        category_options = sorted(df["category"].dropna().unique().tolist())

    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search")
    selected_categories = st.sidebar.multiselect(
        "Categories", category_options, default=category_options
    )
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input(
        "Min openInterest USDC", value=1000, step=100
    )
    sort_option = st.sidebar.selectbox(
        "Sort by",
        [
            "24h volume",
            "openInterest",
            "endDate asc",
            "endDate desc",
            "probability asc",
            "probability desc",
        ],
    )

    df_filtered = filter_dataframe(
        df,
        search,
        selected_categories,
        hide_sports,
        min_prob,
        min_hours,
        min_open_interest,
    )
    df_filtered = sort_dataframe(df_filtered, sort_option)

    st.write(f"Markets after filtering: {len(df_filtered)}")
    columns = [
        c
        for c in [
            "question",
            "yesPrice",
            "noPrice",
            "probability",
            "hoursLeft",
            "openInterest",
            "volume24h",
            "category",
        ]
        if c in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
