from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.models import OvernightSnapshot
from tradingagents.overnight.tail_data import (
    calc_tail_metrics_from_minute_df,
    load_tail_metrics,
    pick_intraday_price_near_time,
)


def make_snapshot() -> OvernightSnapshot:
    profile = build_security_profile("600519", "cn_a")
    return OvernightSnapshot(
        profile=profile,
        name="贵州茅台",
        latest=100.0,
        pre_close=98.0,
        open_price=99.0,
        high=101.0,
        low=97.5,
        amount=1_500_000_000.0,
        turnover=4.5,
        pct=2.0,
        intraday_return_from_open=1.2,
        position=82.0,
        dist_to_high=0.5,
        amplitude=3.6,
        pool="main",
    )


def make_minute_frame() -> pd.DataFrame:
    rows = []
    for minute, price in enumerate([100.1, 100.2, 100.35, 100.45, 100.5], start=30):
        rows.append(
            {
                "dt": f"2025-03-20 14:{minute:02d}:00",
                "open": price - 0.05,
                "price": price,
                "high_p": price + 0.03,
                "low_p": price - 0.04,
                "volume": 1000 + minute,
                "amount": 1_000_000 + minute * 1000,
            }
        )
    return pd.DataFrame(rows)


class OvernightTailDataTests(unittest.TestCase):
    def test_pick_intraday_price_near_time_prefers_exact_then_nearest(self) -> None:
        frame = pd.DataFrame(
            [
                {"dt": "2025-03-20 14:54:00", "price": 99.8},
                {"dt": "2025-03-20 14:55:00", "price": 100.0},
                {"dt": "2025-03-20 14:57:00", "price": 100.4},
            ]
        )

        exact_price, exact_time = pick_intraday_price_near_time(frame, "2025-03-20", "14:55")
        nearest_price, nearest_time = pick_intraday_price_near_time(frame, "2025-03-20", "14:56")

        self.assertEqual(exact_price, 100.0)
        self.assertEqual(exact_time, "14:55")
        self.assertEqual(nearest_price, 100.0)
        self.assertEqual(nearest_time, "14:55")

    def test_pick_intraday_price_near_time_obeys_tie_direction(self) -> None:
        frame = pd.DataFrame(
            [
                {"dt": "2025-03-20 14:54:00", "price": 99.8},
                {"dt": "2025-03-20 14:56:00", "price": 100.4},
            ]
        )

        entry_price, entry_time = pick_intraday_price_near_time(
            frame,
            "2025-03-20",
            "14:55",
            prefer_on_tie="before",
        )
        exit_price, exit_time = pick_intraday_price_near_time(
            frame,
            "2025-03-20",
            "14:55",
            prefer_on_tie="after",
        )

        self.assertEqual(entry_price, 99.8)
        self.assertEqual(entry_time, "14:54")
        self.assertEqual(exit_price, 100.4)
        self.assertEqual(exit_time, "14:56")

    def test_intraday_preview_marks_partial_when_market_not_closed(self) -> None:
        with patch("tradingagents.overnight.tail_data._market_close_reached", return_value=False):
            tail = calc_tail_metrics_from_minute_df(
                make_minute_frame(),
                make_snapshot(),
                "2025-03-20",
                "14:30",
                10,
                "intraday_preview",
            )

        self.assertTrue(tail.has_real_tail_data)
        self.assertEqual(tail.quality, "partial")
        self.assertGreater(tail.tail_return_pct, 0.0)

    def test_strict_keeps_invalid_before_close(self) -> None:
        with patch("tradingagents.overnight.tail_data._market_close_reached", return_value=False):
            tail = calc_tail_metrics_from_minute_df(
                make_minute_frame(),
                make_snapshot(),
                "2025-03-20",
                "14:30",
                10,
                "strict",
            )

        self.assertFalse(tail.has_real_tail_data)
        self.assertEqual(tail.quality, "invalid")
        self.assertEqual(tail.note, "market_not_closed")

    def test_intraday_preview_uses_proxy_when_minute_data_missing(self) -> None:
        snapshot = make_snapshot()
        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.tail_data._load_intraday_minute_df",
                return_value=(pd.DataFrame(), "no_tail_source"),
            ):
                tail = load_tail_metrics(
                    snapshot.profile,
                    snapshot,
                    "2025-03-20",
                    "intraday_preview",
                    Path(tempdir),
                    "14:30",
                    10,
                )

        self.assertFalse(tail.has_real_tail_data)
        self.assertEqual(tail.quality, "proxy")
        self.assertEqual(tail.note, "proxy_after:no_tail_source")


if __name__ == "__main__":
    unittest.main()
