import pandas as pd
from app.main import add_computed_columns, filter_dataframe, sort_dataframe


def example_df():
    data = [
        {
            "question": "Will team A win?",
            "slug": "team-a-win",
            "category": "Sports",
            "yesPrice": 0.9,
            "noPrice": 0.1,
            "openInterest": 2000,
            "volume24hr": 50,
            "endDate": "2030-01-01T00:00:00Z",
        },
        {
            "question": "Will candidate X be elected?",
            "slug": "candidate-x-election",
            "category": "Politics",
            "yesPrice": 0.6,
            "noPrice": 0.4,
            "openInterest": 500,
            "volume24hr": 100,
            "endDate": "2030-01-02T00:00:00Z",
        },
    ]
    df = pd.DataFrame(data)
    return add_computed_columns(df)


def test_search_filter():
    df = example_df()
    result = filter_dataframe(df, "candidate", None, False, 0.0, 0, 0)
    assert len(result) == 1
    assert result.iloc[0]["slug"] == "candidate-x-election"


def test_hide_sports():
    df = example_df()
    result = filter_dataframe(df, "", None, True, 0.0, 0, 0)
    assert all(result["category"] != "Sports")


def test_probability_filter():
    df = example_df()
    result = filter_dataframe(df, "", None, False, 0.8, 0, 0)
    assert len(result) == 1
    assert result.iloc[0]["slug"] == "team-a-win"


def test_sort_open_interest():
    df = example_df()
    sorted_df = sort_dataframe(df, "openInterest")
    assert sorted_df.iloc[0]["openInterest"] >= sorted_df.iloc[1]["openInterest"]

