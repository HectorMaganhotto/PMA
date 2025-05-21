import pandas as pd
from app.main import filter_dataframe, sort_dataframe


def sample_df():
    return pd.DataFrame([
        {
            "question": "Foo?",
            "slug": "foo",
            "category": "Politics",
            "probability": 0.9,
            "hoursLeft": 10,
            "openInterest": 2000,
            "volume24hr": 500,
        },
        {
            "question": "Bar?",
            "slug": "bar",
            "category": "Sports",
            "probability": 0.6,
            "hoursLeft": 2,
            "openInterest": 800,
            "volume24hr": 100,
        },
    ])


def test_probability_filter():
    df = sample_df()
    out = filter_dataframe(df, "", [], [], False, 0.7, 0, 0)
    assert len(out) == 1


def test_hide_sports():
    df = sample_df()
    out = filter_dataframe(df, "", [], ["Politics", "Sports"], True, 0, 0, 0)
    assert "Sports" not in out["category"].values


def test_search():
    df = sample_df()
    out = filter_dataframe(df, "foo", [], ["Politics", "Sports"], False, 0, 0, 0)
    assert len(out) == 1 and out.iloc[0]["slug"] == "foo"


def test_category_filter():
    df = sample_df()
    out = filter_dataframe(df, "", ["Politics"], ["Politics", "Sports"], False, 0, 0, 0)
    assert out["category"].unique().tolist() == ["Politics"]


def test_sorting():
    df = sample_df()
    out = sort_dataframe(df, "probability desc")
    assert out.iloc[0]["probability"] >= out.iloc[1]["probability"]
