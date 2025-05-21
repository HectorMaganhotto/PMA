import pandas as pd
from datetime import datetime, timedelta, timezone
import unittest

from app.main import normalize_dataframe, filter_dataframe


class FilterTests(unittest.TestCase):
    def test_filter_pipeline_allows_rows(self):
        now = datetime.now(timezone.utc)
        df = pd.DataFrame([
            {
                "question": "Q1",
                "yesPrice": 0.6,
                "noPrice": 0.4,
                "openInterest": 1500,
                "endDate": (now + timedelta(hours=10)).isoformat(),
            },
            {
                "question": "Q2",
                "yesPrice": 0.4,
                "noPrice": 0.6,
                "openInterest": 500,
                "endDate": (now + timedelta(hours=1)).isoformat(),
            },
        ])
        df_norm = normalize_dataframe(df)
        result = filter_dataframe(
            df_norm,
            min_prob=0.5,
            min_hours=0,
            min_open_interest=1000,
            disable_prob=False,
            disable_hours=False,
            disable_open=False,
        )
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()

