import unittest
import pandas as pd

from app.main import normalize_dataframe, filter_dataframe


class FilterTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame([
            {
                "question": "Will A happen",
                "slug": "a-happen",
                "yesPrice": 0.9,
                "noPrice": 0.1,
                "openInterest": 2000,
                "category": "Politics",
                "volume24hr": 100,
                "endDate": "2030-01-01T00:00:00Z",
            },
            {
                "question": "Will B happen",
                "slug": "b-happen",
                "yesPrice": 0.6,
                "noPrice": 0.4,
                "openInterest": 500,
                "category": "Sports",
                "volume24hr": 50,
                "endDate": "2030-01-01T00:00:00Z",
            },
            {
                "question": "Will C happen",
                "slug": "c-happen",
                "yesPrice": 0.4,
                "noPrice": 0.6,
                "openInterest": 1500,
                "category": "Politics",
                "volume24hr": 20,
                "endDate": None,
            },
        ])
        self.df = normalize_dataframe(self.df)

    def test_min_prob(self):
        filtered = filter_dataframe(self.df, "", ["Politics", "Sports"], False, 0.7, 0, 0)
        self.assertEqual(len(filtered), 1)

    def test_hide_sports(self):
        filtered = filter_dataframe(self.df, "", ["Politics", "Sports"], True, 0.0, 0, 0)
        self.assertNotIn("Sports", filtered["category"].tolist())

    def test_min_hours(self):
        filtered = filter_dataframe(self.df, "", ["Politics", "Sports"], False, 0.0, 1, 0)
        self.assertTrue(filtered["hoursLeft"].min() >= 1)

    def test_min_open_interest(self):
        filtered = filter_dataframe(self.df, "", ["Politics", "Sports"], False, 0.0, 0, 1000)
        self.assertTrue((filtered["openInterest"] >= 1000).all())


if __name__ == "__main__":
    unittest.main()
