import unittest
import pandas as pd

from app.main import normalize_dataframe, filter_dataframe

class FilterTests(unittest.TestCase):
    def setUp(self):
        data = [
            {
                "question": "Will A happen?",
                "slug": "a",
                "category": "Politics",
                "yesPrice": "0.8",
                "noPrice": "0.2",
                "openInterest": "1500",
                "volume24hr": 200,
                "endDate": "2030-01-01T00:00:00Z",
            },
            {
                "question": "Will B happen?",
                "slug": "b",
                "category": "Sports",
                "yesPrice": "0.4",
                "noPrice": "0.6",
                "openInterest": "500",
                "volume24hr": 50,
            },
        ]
        self.df = normalize_dataframe(pd.DataFrame(data))

    def test_hours_left_missing(self):
        self.assertEqual(self.df.loc[1, "hoursLeft"], -1)

    def test_filtering(self):
        filtered = filter_dataframe(
            self.df,
            search="a",
            categories=["Politics"],
            hide_sports=True,
            min_prob=0.5,
            min_hours=0,
            min_open=1000,
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["slug"], "a")

if __name__ == "__main__":
    unittest.main()
