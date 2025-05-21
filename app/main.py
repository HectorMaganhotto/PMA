import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

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


def hours_to_expiry(date_str: str | None) -> float:
    """Return remaining hours until the given ISO date or -1 if invalid."""
    if not isinstance(date_str, str) or not date_str:
        return -1.0
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return -1.0
    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def hours_left(row: Dict[str, Any]) -> float:
    """Return remaining hours until market resolution or -1 if unknown."""
    date_str = row.get("endDate") or row.get("endsAt") or row.get("expiry")
    return hours_to_expiry(date_str)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add probability and hoursLeft columns."""
    if df.empty:
        return df
    df = df.copy()
    if {"yesPrice", "noPrice"}.issubset(df.columns):
        df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    else:
        df["probability"] = 0.0
    df["hoursLeft"] = df.apply(hours_left, axis=1)
    return df


def filter_dataframe(
    df: pd.DataFrame,
    search_text: str,
    categories: List[str],
    hide_sports: bool,
    min_prob: float,
    min_hours: int,
    min_open: int,
) -> pd.DataFrame:
    """Apply sidebar filters in order."""
    result = df.copy()

    if hide_sports and "category" in result.columns:
        result = result[result["category"].str.lower() != "sports"]

    if categories and "category" in result.columns:
        result = result[result["category"].isin(categories)]

    if search_text:
        mask_q = result["question"].str.contains(search_text, case=False, na=False)
        mask_s = result["slug"].str.contains(search_text, case=False, na=False)
        result = result[mask_q | mask_s]

    if "probability" in result.columns:
        result = result[result["probability"] >= min_prob]

    if "hoursLeft" in result.columns:
        result = result[result["hoursLeft"] >= min_hours]

    if "openInterest" in result.columns:
        result = result[result["openInterest"] >= min_open]

    return result


SORT_OPTIONS: Dict[str, Tuple[str, bool]] = {
    "24h volume": ("volume24hr", False),
    "openInterest": ("openInterest", False),
    "endDate asc": ("endDate", True),
    "endDate desc": ("endDate", False),
    "probability asc": ("probability", True),
    "probability desc": ("probability", False),
}


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    column, asc = SORT_OPTIONS.get(option, ("openInterest", False))
    if column not in df.columns:
        return df
    return df.sort_values(column, ascending=asc)


def load_dataframe() -> pd.DataFrame:
    """Load and normalize markets into a DataFrame."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    for col in ["yesPrice", "noPrice", "openInterest"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return normalize_dataframe(df)


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total current available markets: {len(df)}")
    if df.empty:
        st.info("No market data available.")
        return

    categories = sorted(df["category"].dropna().unique().tolist()) if "category" in df.columns else []

    st.sidebar.header("Filters")
    search_text = st.sidebar.text_input("Search question or slug")
    selected_categories = st.sidebar.multiselect("Categories", categories, default=categories)
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open = st.sidebar.number_input("Min openInterest USDC", value=1000, step=100)
    sort_option = st.sidebar.selectbox("Sort by", list(SORT_OPTIONS.keys()))

    filtered = filter_dataframe(
        df,
        search_text,
        selected_categories,
        hide_sports,
        min_prob,
        min_hours,
        min_open,
    )
    filtered = sort_dataframe(filtered, sort_option)

    st.write(f"Showing {len(filtered)} markets after filters.")
    columns = [
        "question",
        "yesPrice",
        "noPrice",
        "probability",
        "hoursLeft",
        "openInterest",
        "volume24hr",
        "category",
    ]
    cols = [c for c in columns if c in filtered.columns]
    st.dataframe(filtered[cols])


if __name__ == "__main__":
    main()
