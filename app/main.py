import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

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


def hours_left(row: Dict[str, Any]) -> float:
    """Return the remaining hours until market resolution.

    Accepts ISO strings with or without ``Z``. If parsing fails, returns ``-1``.
    """
    date_str = (
        row.get("endsAt")
        or row.get("endDate")
        or row.get("expiry"))
    if not isinstance(date_str, str) or not date_str:
        return -1.0
    try:
        if date_str.endswith("Z"):
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return -1.0

    delta = dt - datetime.now(timezone.utc)
    return round(delta.total_seconds() / 3600, 2)


def load_dataframe() -> pd.DataFrame:
    """Load markets into a DataFrame with computed columns."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    if not df.empty:
        add_computed_columns(df)
    return df


def apply_filters(
    df: pd.DataFrame,
    *,
    search: str = "",
    categories: Sequence[str] | None = None,
    hide_sports: bool = False,
    min_prob: float = 0.85,
    min_hours: float = 6.0,
    min_open_interest: float = 1000.0,
) -> pd.DataFrame:
    """Return ``df`` filtered according to the provided values."""

    out = df.copy()

    if hide_sports and "category" in out.columns:
        out = out[out["category"] != "Sports"]

    if categories and "category" in out.columns:
        out = out[out["category"].isin(categories)]

    if search:
        mask_q = (
            out["question"].astype(str).str.contains(search, case=False, na=False)
            if "question" in out.columns
            else pd.Series(False, index=out.index)
        )
        mask_s = (
            out["slug"].astype(str).str.contains(search, case=False, na=False)
            if "slug" in out.columns
            else pd.Series(False, index=out.index)
        )
        out = out[mask_q | mask_s]

    if "probability" in out.columns:
        out = out[out["probability"] >= min_prob]

    if "hoursLeft" in out.columns:
        out = out[out["hoursLeft"] >= min_hours]

    if "openInterest" in out.columns:
        out = out[out["openInterest"] >= min_open_interest]

    return out


def sort_df(df: pd.DataFrame, key: str, ascending: bool) -> pd.DataFrame:
    """Return ``df`` sorted by the given column if present."""
    if key in df.columns:
        return df.sort_values(key, ascending=ascending)
    return df


def add_computed_columns(df: pd.DataFrame) -> None:
    """Add ``probability`` and ``hoursLeft`` columns to ``df`` in-place."""
    if {"yesPrice", "noPrice"}.issubset(df.columns):
        df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    else:
        df["probability"] = pd.NA

    df["hoursLeft"] = df.apply(hours_left, axis=1)


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total current available markets: {len(df)}")
    if df.empty:
        st.info("No market data available.")
        return

    categories = []
    if "category" in df.columns:
        categories = sorted([c for c in df["category"].dropna().unique()])

    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search")
    selected_categories = st.sidebar.multiselect(
        "Category", categories, default=categories
    )
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input(
        "Min openInterest USDC", value=1000, step=100
    )

    sort_map = {
        "24h volume": ("volume24hr", False),
        "openInterest": ("openInterest", False),
        "endDate asc": ("endDate", True),
        "endDate desc": ("endDate", False),
        "probability asc": ("probability", True),
        "probability desc": ("probability", False),
    }
    sort_choice = st.sidebar.selectbox("Sort by", list(sort_map.keys()))

    df_filtered = apply_filters(
        df,
        search=search,
        categories=selected_categories,
        hide_sports=hide_sports,
        min_prob=min_prob,
        min_hours=min_hours,
        min_open_interest=min_open_interest,
    )

    sort_col, ascending = sort_map[sort_choice]
    df_filtered = sort_df(df_filtered, sort_col, ascending)

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
