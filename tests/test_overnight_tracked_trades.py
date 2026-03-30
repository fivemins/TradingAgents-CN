from datetime import datetime, timedelta, timezone
import tempfile
import unittest
from unittest.mock import patch

from tradingagents.overnight.tracked_trades import (
    build_tracked_trade_stats,
    refresh_tracked_trade,
)


CHINA_TZ = timezone(timedelta(hours=8))


def make_trade() -> dict:
    return {
        "trade_id": "trade-001",
        "trade_date": "2025-03-20",
        "market_region": "cn_a",
        "scan_id": "scan-001",
        "scan_mode": "strict",
        "source_bucket": "formal",
        "ticker": "600519.SS",
        "name": "贵州茅台",
        "pool": "主板",
        "quality": "real",
        "quick_score": 78.5,
        "total_score": 83.2,
        "factor_breakdown": {"trend_strength": 22.0},
        "tail_metrics": {"quality": "real"},
        "confirmed_at": "2025-03-20T06:40:00+00:00",
        "entry_target_time": "14:55",
        "entry_price": None,
        "entry_time_used": None,
        "exit_target_time": "10:00",
        "exit_trade_date": None,
        "exit_price": None,
        "exit_time_used": None,
        "strategy_return": None,
        "status": "pending_entry",
        "last_error": None,
        "last_checked_at": None,
        "created_at": "2025-03-20T06:40:00+00:00",
        "updated_at": "2025-03-20T06:40:00+00:00",
    }


class OvernightTrackedTradeTests(unittest.TestCase):
    def test_refresh_before_entry_keeps_pending_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.tracked_trades.load_trade_calendar_dates",
                return_value=["2025-03-20", "2025-03-21"],
            ):
                updates = refresh_tracked_trade(
                    make_trade(),
                    data_dir=tempdir,
                    now=datetime(2025, 3, 20, 14, 50, tzinfo=CHINA_TZ),
                )

        self.assertEqual(updates["status"], "pending_entry")
        self.assertIsNone(updates["last_error"])

    def test_refresh_validates_trade_when_entry_and_exit_prices_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.tracked_trades.load_trade_calendar_dates",
                return_value=["2025-03-20", "2025-03-21"],
            ), patch(
                "tradingagents.overnight.tracked_trades.lookup_trade_price",
                side_effect=[(100.0, "14:55"), (103.0, "10:00")],
            ) as lookup:
                updates = refresh_tracked_trade(
                    make_trade(),
                    data_dir=tempdir,
                    now=datetime(2025, 3, 21, 10, 5, tzinfo=CHINA_TZ),
                )

        self.assertEqual(lookup.call_args_list[0].kwargs["prefer_on_tie"], "before")
        self.assertEqual(lookup.call_args_list[1].kwargs["prefer_on_tie"], "after")
        self.assertEqual(updates["status"], "validated")
        self.assertEqual(updates["entry_price"], 100.0)
        self.assertEqual(updates["exit_price"], 103.0)
        self.assertEqual(updates["strategy_return"], 3.0)
        self.assertIsNone(updates["last_error"])

    def test_refresh_marks_unavailable_when_entry_missing_and_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.tracked_trades.load_trade_calendar_dates",
                return_value=["2025-03-20", "2025-03-21"],
            ), patch(
                "tradingagents.overnight.tracked_trades.lookup_trade_price",
                return_value=(None, None),
            ):
                first = refresh_tracked_trade(
                    make_trade(),
                    data_dir=tempdir,
                    now=datetime(2025, 3, 20, 15, 0, tzinfo=CHINA_TZ),
                )

            retry_trade = {**make_trade(), **first}
            with patch(
                "tradingagents.overnight.tracked_trades.load_trade_calendar_dates",
                return_value=["2025-03-20", "2025-03-21"],
            ), patch(
                "tradingagents.overnight.tracked_trades.lookup_trade_price",
                side_effect=[(100.0, "14:54"), (101.0, "10:01")],
            ):
                second = refresh_tracked_trade(
                    retry_trade,
                    data_dir=tempdir,
                    now=datetime(2025, 3, 21, 10, 5, tzinfo=CHINA_TZ),
                )

        self.assertEqual(first["status"], "unavailable")
        self.assertEqual(first["last_error"], "missing_entry_price")
        self.assertEqual(second["status"], "validated")
        self.assertEqual(second["entry_time_used"], "14:54")
        self.assertEqual(second["exit_time_used"], "10:01")
        self.assertEqual(second["strategy_return"], 1.0)

    def test_build_tracked_trade_stats_counts_only_validated_returns(self) -> None:
        stats = build_tracked_trade_stats(
            [
                {**make_trade(), "status": "validated", "strategy_return": 5.0},
                {**make_trade(), "trade_id": "trade-002", "status": "pending_entry"},
                {**make_trade(), "trade_id": "trade-003", "status": "unavailable"},
                {**make_trade(), "trade_id": "trade-004", "status": "validated", "strategy_return": -2.0},
            ]
        )

        self.assertEqual(stats["total_days"], 4)
        self.assertEqual(stats["validated_days"], 2)
        self.assertEqual(stats["pending_count"], 1)
        self.assertEqual(stats["unavailable_count"], 1)
        self.assertEqual(stats["avg_return"], 1.5)
        self.assertEqual(stats["win_rate"], 0.5)
        self.assertEqual(stats["cumulative_return"], 2.9)


if __name__ == "__main__":
    unittest.main()
