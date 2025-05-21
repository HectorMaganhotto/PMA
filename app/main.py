import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Iterable

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


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with ``probability`` and ``hoursLeft`` columns."""
    result = df.copy()
    if {"yesPrice", "noPrice"}.issubset(result.columns):
        result["probability"] = result[["yesPrice", "noPrice"]].max(axis=1, skipna=True)
    else:
        result["probability"] = pd.NA
    result["hoursLeft"] = result.apply(hours_to_expiry, axis=1)
    return result


def filter_dataframe(
    df: pd.DataFrame,
    search: str,
    categories: Iterable[str] | None,
    hide_sports: bool,
    min_prob: float,
    min_hours: int,
    min_open: int,
) -> pd.DataFrame:
    """Filter ``df`` according to sidebar controls."""
    result = df.copy()

    if search:
        text = search.lower()
        q_match = result["question"].str.contains(text, case=False, na=False) if "question" in result.columns else False
        slug_match = result.get("slug", pd.Series(""))
        if isinstance(slug_match, pd.Series):
            slug_match = slug_match.str.contains(text, case=False, na=False)
        else:
            slug_match = False
        result = result[q_match | slug_match]

    if hide_sports and "category" in result.columns:
        result = result[~result["category"].str.contains("sports", case=False, na=False)]

    if categories and "category" in result.columns and "All" not in categories:
        result = result[result["category"].isin(categories)]

    if "probability" in result.columns:
        result = result[result["probability"] >= min_prob]

    if "hoursLeft" in result.columns:
        result = result[result["hoursLeft"] >= min_hours]

    if "openInterest" in result.columns:
        result = result[result["openInterest"] >= min_open]

    return result


def sort_dataframe(df: pd.DataFrame, option: str) -> pd.DataFrame:
    """Sort ``df`` according to the selected option."""
    if option == "24h volume" and "volume24hr" in df.columns:
        return df.sort_values("volume24hr", ascending=False)
    if option == "openInterest" and "openInterest" in df.columns:
        return df.sort_values("openInterest", ascending=False)

    date_col = None
    for col in ("endDate", "endsAt", "expiry"):
        if col in df.columns:
            date_col = col
            break

    if option == "endDate asc" and date_col:
        return df.sort_values(date_col, ascending=True)
    if option == "endDate desc" and date_col:
        return df.sort_values(date_col, ascending=False)

    if option == "probability asc" and "probability" in df.columns:
        return df.sort_values("probability", ascending=True)
    if option == "probability desc" and "probability" in df.columns:
        return df.sort_values("probability", ascending=False)

    return df


def load_dataframe() -> pd.DataFrame:
    """Load markets into a DataFrame with computed columns."""
    markets = asyncio.run(fetch_markets())
    df = pd.DataFrame(markets)
    if df.empty:
        return df
    df = add_computed_columns(df)
    return df


def main() -> None:
    st.set_page_config(page_title="Polymarket Browser", layout="wide")
    st.title("Polymarket Markets (Read-Only)")

    df = load_dataframe()
    st.write(f"Total markets loaded: {len(df)}")

    st.sidebar.header("Filters")
    search_text = st.sidebar.text_input("Search question or slug")
    categories = ["All"]
    if "category" in df.columns:
        categories += sorted(c for c in df["category"].dropna().unique())
    selected_categories = st.sidebar.multiselect("Category", categories, default=["All"])
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
    if df.empty:
        st.info("No market data available.")
        return

    df_filtered = filter_dataframe(
        df,
        search_text,
        selected_categories,
        hide_sports,
        min_prob,
        min_hours,
        int(min_open_interest),
    )
    df_filtered = sort_dataframe(df_filtered, sort_option)

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
        if c in df_filtered.columns
    ]
    st.dataframe(df_filtered[columns])


if __name__ == "__main__":
    main()
