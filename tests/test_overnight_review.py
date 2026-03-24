from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.market_regime import IndexSnapshotResult
from tradingagents.overnight.models import MarketRegime, TailMetrics
from tradingagents.overnight.review import run_overnight_review


def make_history_frame() -> pd.DataFrame:
    dates = pd.bdate_range(end="2025-03-19", periods=25)
    frame = pd.DataFrame(
        {
            "Date": dates.strftime("%Y-%m-%d"),
            "Open": [9.0 + index * 0.03 for index in range(len(dates))],
            "High": [9.1 + index * 0.03 for index in range(len(dates))],
            "Low": [8.9 + index * 0.03 for index in range(len(dates))],
            "Close": [9.05 + index * 0.03 for index in range(len(dates))],
            "Volume": [1000 + index * 10 for index in range(len(dates))],
            "Turnover": [1.0e9 + index * 5e7 for index in range(len(dates))],
            "TurnoverRate": [2.0 + index * 0.03 for index in range(len(dates))],
        }
    )
    frame.loc[frame["Date"] == "2025-03-17", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [10.0, 10.3, 9.9, 10.2, 1.4e9, 3.8]
    frame.loc[frame["Date"] == "2025-03-18", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [10.5, 10.8, 10.4, 10.6, 1.8e9, 4.1]
    frame.loc[frame["Date"] == "2025-03-19", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [10.4, 10.6, 10.1, 10.3, 1.6e9, 3.4]
    return frame


class OvernightReviewTests(unittest.TestCase):
    def _run_review(self, tempdir: str) -> dict:
        profile = build_security_profile("600519", "cn_a")
        universe = [{"code": profile.normalized_ticker, "profile": profile, "name": "贵州茅台", "pool": "main"}]

        with (
            patch("tradingagents.overnight.review._load_trade_dates", return_value=["2025-03-17", "2025-03-18"]),
            patch("tradingagents.overnight.review._build_benchmark_map", return_value={"2025-03-17": 0.3, "2025-03-18": -0.2}),
            patch("tradingagents.overnight.review._build_review_universe", return_value=universe),
            patch("tradingagents.overnight.review.load_history_frame", return_value=make_history_frame()),
            patch(
                "tradingagents.overnight.review.load_index_snapshot",
                return_value=IndexSnapshotResult(
                    values={"上证指数": 0.6},
                    provider_route="akshare_index_daily",
                ),
            ),
            patch(
                "tradingagents.overnight.review.evaluate_market_regime",
                return_value=MarketRegime(market_ok=True, market_message="市场正常", benchmark_pct=0.4),
            ),
            patch("tradingagents.overnight.review.load_risk_stocks", return_value=(set(), {"matched_events": 0, "risk_codes": 0, "scanned_days": 0})),
            patch("tradingagents.overnight.review.check_buy_filters", return_value=(True, "通过")),
            patch("tradingagents.overnight.review.calc_quick_score", return_value=70.0),
            patch(
                "tradingagents.overnight.review.load_tail_metrics",
                return_value=TailMetrics(
                    has_real_tail_data=True,
                    source="unit-test",
                    quality="real",
                    tail_return_pct=0.82,
                    tail_amount_ratio=0.18,
                ),
            ),
            patch("tradingagents.overnight.review.calculate_total_score", return_value=(80.0, {"trend_strength": 22.0, "tail_strength": 18.0})),
        ):
            return run_overnight_review(end_trade_date="2025-03-18", data_dir=Path(tempdir), window_days=2)

    def test_run_overnight_review_aggregates_next_open_returns(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = self._run_review(tempdir)

        summary = result["summary"]
        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["days_evaluated"], 2)
        self.assertEqual(summary["days_with_formal_picks"], 2)
        self.assertTrue(summary["has_valid_samples"])
        self.assertAlmostEqual(summary["avg_next_open_return"], 0.5272, places=4)
        self.assertAlmostEqual(summary["avg_excess_return"], 0.4772, places=4)
        self.assertTrue(summary["survivorship_bias"])
        self.assertEqual(summary["data_quality"]["status"], "incomplete")
        self.assertIn("survivorship_bias", summary["bias_flags"])
        self.assertTrue(summary["evaluation_config_hash"])
        self.assertEqual(summary["evaluation_config_version"], "overnight_phase2_v1")
        self.assertEqual(len(summary["regime_breakdown"]), 1)
        self.assertEqual(len(summary["pool_breakdown"]), 1)
        self.assertEqual(len(summary["tail_quality_breakdown"]), 1)
        self.assertEqual(result["candidate_results"][0]["category"], "formal")
        self.assertIsNotNone(result["candidate_results"][0]["benchmark_next_open_return"])
        self.assertIsNotNone(result["candidate_results"][0]["excess_return"])

    def test_same_frozen_inputs_produce_identical_review_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            first = self._run_review(tempdir)
            second = self._run_review(tempdir)

        self.assertEqual(first["summary"], second["summary"])
        self.assertEqual(first["daily_results"], second["daily_results"])
        self.assertEqual(first["candidate_results"], second["candidate_results"])


if __name__ == "__main__":
    unittest.main()
