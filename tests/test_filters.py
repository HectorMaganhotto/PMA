import pandas as pd
from app.main import add_computed_columns, apply_filters

def test_missing_end_date_filter():
    df = pd.DataFrame([
        {
            "question": "Q1",
            "yesPrice": 0.6,
            "noPrice": 0.4,
            "openInterest": 2000,
            "endDate": "2030-01-01T00:00:00Z",
            "category": "Politics",
            "volume24hr": 10,
        },
        {
            "question": "Q2",
            "yesPrice": 0.7,
            "noPrice": 0.3,
            "openInterest": 2000,
            "category": "Sports",
            "volume24hr": 5,
        },
    ])
    add_computed_columns(df)
    assert df.loc[1, "hoursLeft"] == -1

    out = apply_filters(
        df,
        min_hours=1,
        categories=["Politics", "Sports"],
        hide_sports=False,
        min_prob=0.5,
        min_open_interest=0,
    )
    assert len(out) == 1
    assert out.iloc[0]["question"] == "Q1"

