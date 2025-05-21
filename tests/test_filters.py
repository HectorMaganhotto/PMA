import unittest

import pandas as pd

from app.main import normalize_dataframe, filter_dataframe


class FilterTests(unittest.TestCase):
    def test_filters(self):
        df = pd.DataFrame([
            {
                "question": "Foo",
                "slug": "foo",
                "category": "Sports",
                "yesPrice": 0.6,
                "noPrice": 0.4,
                "openInterest": 2000,
                "volume24h": 100,
                "endDate": "2100-01-01T00:00:00Z",
            },
            {
                "question": "Bar",
                "slug": "bar",
                "category": "Politics",
                "yesPrice": 0.7,
                "noPrice": 0.3,
                "openInterest": 500,
                "volume24h": 50,
                "endDate": None,
            },
        ])
        df = normalize_dataframe(df)
        self.assertEqual(df.loc[1, "hoursLeft"], -1.0)

        filtered = filter_dataframe(
            df,
            search="foo",
            categories=["Sports"],
            hide_sports=False,
            min_prob=0.5,
            min_hours=0,
            min_open_interest=0,
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["slug"], "foo")

        filtered = filter_dataframe(
            df,
            search="",
            categories=["Politics"],
            hide_sports=False,
            min_prob=0.6,
            min_hours=1,
            min_open_interest=1000,
        )
        self.assertTrue(filtered.empty)


if __name__ == "__main__":
    unittest.main()
