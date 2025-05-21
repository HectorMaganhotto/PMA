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


def hours_left(date_val: Any) -> float:
    """Return hours remaining until the given end date."""
    if not isinstance(date_val, str) or not date_val:
        return -1.0
    date_val = date_val.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(date_val)
    except ValueError:
        return -1.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Compute helper columns for filtering and sorting."""
    if df.empty:
        return df
    df["endDate"] = df.apply(
        lambda r: r.get("endDate") or r.get("endsAt") or r.get("expiry"), axis=1
    )
    for col in ["yesPrice", "noPrice", "openInterest", "volume24hr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    df["hoursLeft"] = df["endDate"].apply(hours_left)
    return df


def load_dataframe() -> pd.DataFrame:
    """Fetch markets and prepare a DataFrame."""
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
    min_open: float,
) -> pd.DataFrame:
    """Apply sidebar filters in the required order."""
    result = df.copy()
    if hide_sports and "category" in result.columns:
        result = result[~result["category"].str.contains("sports", case=False, na=False)]
    if categories and "category" in result.columns:
        result = result[result["category"].isin(categories)]
    if search:
        s = search.lower()
        result = result[
            result.get("question", "").str.lower().str.contains(s, na=False)
            | result.get("slug", "").str.lower().str.contains(s, na=False)
        ]
    if "probability" in result.columns:
        result = result[result["probability"] >= min_prob]
    if "hoursLeft" in result.columns:
        result = result[result["hoursLeft"] >= float(min_hours)]
    if "openInterest" in result.columns:
        result = result[result["openInterest"] >= float(min_open)]
    return result


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    """Sort markets based on the dropdown selection."""
    mapping = {
        "24h volume": ("volume24hr", False),
        "openInterest": ("openInterest", False),
        "endDate asc": ("endDate", True),
        "endDate desc": ("endDate", False),
        "probability asc": ("probability", True),
        "probability desc": ("probability", False),
    }
    if option not in mapping:
        return df
    col, asc = mapping[option]
    if col not in df.columns:
        return df
    return df.sort_values(col, ascending=asc)


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total current available markets: {len(df)}")

    if df.empty:
        st.info("No market data available.")
        return

    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search")
    categories = sorted(df["category"].dropna().unique().tolist()) if "category" in df.columns else []
    selected_categories = st.sidebar.multiselect("Category", categories, default=categories)
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input("Min openInterest USDC", value=1000, step=100)
    sort_opt = st.sidebar.selectbox(
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

    filtered = filter_dataframe(
        df,
        search=search,
        categories=selected_categories,
        hide_sports=hide_sports,
        min_prob=min_prob,
        min_hours=min_hours,
        min_open=min_open_interest,
    )
    sorted_df = sort_dataframe(filtered, sort_opt)

    columns = [
        c
        for c in [
            "question",
            "yesPrice",
            "noPrice",
            "probability",
            "hoursLeft",
            "openInterest",
            "volume24hr",
            "category",
        ]
        if c in sorted_df.columns
    ]
    st.dataframe(sorted_df[columns])


if __name__ == "__main__":
    main()
