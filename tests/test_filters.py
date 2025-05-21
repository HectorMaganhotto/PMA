import unittest
from datetime import datetime, timezone, timedelta

import pandas as pd

from app.main import hours_to_expiry, filter_dataframe


class FilterTests(unittest.TestCase):
    def test_hours_to_expiry_missing(self) -> None:
        market = {"endDate": None}
        self.assertEqual(hours_to_expiry(market), -1.0)

    def test_filter_sequence(self) -> None:
        now = datetime.now(timezone.utc)
        df = pd.DataFrame([
            {
                "question": "Will X happen?",
                "slug": "x-happen",
                "category": "Politics",
                "yesPrice": 0.9,
                "noPrice": 0.1,
                "openInterest": 2000,
                "volume24hr": 100,
                "endDate": (now + timedelta(hours=10)).isoformat(),
            },
            {
                "question": "Will Y happen?",
                "slug": "y-happen",
                "category": "Sports",
                "yesPrice": 0.6,
                "noPrice": 0.4,
                "openInterest": 500,
                "volume24hr": 50,
                "endDate": (now + timedelta(hours=2)).isoformat(),
            },
            {
                "question": "Will Z happen?",
                "slug": "z-happen",
                "category": "Politics",
                "yesPrice": 0.7,
                "noPrice": 0.3,
                "openInterest": 1500,
                "volume24hr": 10,
                "endDate": None,
            },
        ])
        df["probability"] = df[["yesPrice", "noPrice"]].max(axis=1)
        df["hoursLeft"] = df.apply(hours_to_expiry, axis=1)

        result = filter_dataframe(
            df,
            hide_sports=True,
            categories=["Politics"],
            search="will x",
            min_prob=0.8,
            min_hours=3,
            min_open_interest=1000,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["slug"], "x-happen")


if __name__ == "__main__":
    unittest.main()
