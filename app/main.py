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
    if df.empty:
        return df

    df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    df["hoursLeft"] = df.apply(hours_to_expiry, axis=1)
    return df


def filter_dataframe(
    df: pd.DataFrame,
    search: str,
    categories: list[str],
    all_categories: list[str],
    hide_sports: bool,
    min_prob: float,
    min_hours: float,
    min_open_interest: float,
) -> pd.DataFrame:
    """Apply all filters to the markets DataFrame."""

    result = df.copy()

    if search:
        search_lower = search.lower()
        mask = False
        if "question" in result.columns:
            mask |= result["question"].astype(str).str.lower().str.contains(search_lower)
        if "slug" in result.columns:
            mask |= result["slug"].astype(str).str.lower().str.contains(search_lower)
        result = result[mask]

    if categories and len(categories) != len(all_categories) and "category" in result.columns:
        result = result[result["category"].isin(categories)]

    if hide_sports and "category" in result.columns:
        result = result[result["category"].str.lower() != "sports"]

    if "probability" in result.columns:
        result = result[result["probability"] >= min_prob]

    if "hoursLeft" in result.columns:
        result = result[result["hoursLeft"] >= min_hours]

    if "openInterest" in result.columns:
        result = result[result["openInterest"] >= min_open_interest]

    return result


SORT_OPTIONS = {
    "24h volume": ("volume24hr", False),
    "openInterest": ("openInterest", False),
    "endDate asc": ("hoursLeft", True),
    "endDate desc": ("hoursLeft", False),
    "probability asc": ("probability", True),
    "probability desc": ("probability", False),
}


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    """Sort the DataFrame according to the selected option."""
    col, ascending = SORT_OPTIONS.get(option, ("volume24hr", False))
    if col in df.columns:
        return df.sort_values(col, ascending=ascending)
    return df


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search")

    df = load_dataframe()
    total_markets = len(df)
    st.markdown(f"**Total markets loaded:** {total_markets}")

    if df.empty:
        st.info("No market data available.")
        return

    categories_all = sorted(df["category"].dropna().unique().tolist()) if "category" in df.columns else []
    selected_categories = st.sidebar.multiselect("Categories", categories_all, default=categories_all)
    hide_sports = st.sidebar.checkbox("Hide sports markets")
    min_prob = st.sidebar.slider("Min implied probability", 0.5, 1.0, 0.85, 0.01)
    min_hours = st.sidebar.slider("Min hours to expiry", 0, 48, 6)
    min_open_interest = st.sidebar.number_input("Min openInterest", value=1000, step=100)
    sort_option = st.sidebar.selectbox("Sort by", list(SORT_OPTIONS.keys()))

    df_filtered = filter_dataframe(
        df,
        search,
        selected_categories,
        categories_all,
        hide_sports,
        min_prob,
        min_hours,
        min_open_interest,
    )

    df_filtered = sort_dataframe(df_filtered, sort_option)

    columns = [
        col
        for col in ["question", "yesPrice", "noPrice", "probability", "hoursLeft", "openInterest", "volume24hr", "category"]
        if col in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
