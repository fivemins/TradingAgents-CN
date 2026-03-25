from __future__ import annotations

import unittest

import pandas as pd

from tradingagents.overnight.scoring import calc_trend_score


class OvernightScoringTests(unittest.TestCase):
    def test_calc_trend_score_handles_duplicate_close_columns(self) -> None:
        close_values = [float(value) for value in range(1, 26)]
        adjusted_values = [value - 0.2 for value in close_values]
        history = pd.DataFrame(
            zip(close_values, adjusted_values, strict=False),
            columns=["Close", "Close"],
        )

        score = calc_trend_score(history)

        self.assertGreater(score, 0.0)


if __name__ == "__main__":
    unittest.main()
