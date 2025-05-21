import unittest
import pandas as pd
from app.main import filter_dataframe, hours_to_expiry

class FilterTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame([
            {
                "question": "A",
                "yesPrice": 0.6,
                "noPrice": 0.4,
                "openInterest": 500,
                "endDate": "2100-01-01T00:00:00Z",
            },
            {
                "question": "B",
                "yesPrice": 0.4,
                "noPrice": 0.6,
                "openInterest": 1500,
                "endDate": None,
            },
        ])
        self.df["hoursToExpiry"] = self.df.apply(hours_to_expiry, axis=1)
        self.df["probability"] = self.df[["yesPrice", "noPrice"]].max(axis=1)

    def test_min_prob_filter(self):
        result = filter_dataframe(self.df, min_prob=0.7, min_hours=0, min_open_interest=0)
        self.assertEqual(len(result), 0)

    def test_min_hours_filter(self):
        result = filter_dataframe(self.df, min_prob=0, min_hours=1, min_open_interest=0)
        self.assertEqual(result["question"].tolist(), ["A"])

    def test_min_open_interest_filter(self):
        result = filter_dataframe(self.df, min_prob=0, min_hours=0, min_open_interest=1000)
        self.assertEqual(result["question"].tolist(), ["B"])

if __name__ == "__main__":
    unittest.main()
