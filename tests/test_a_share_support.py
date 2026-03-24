from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from tradingagents.dataflows.a_share_support import get_a_share_history
from tradingagents.market_utils import build_security_profile


def _akshare_history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": ["2025-03-19", "2025-03-20"],
            "开盘": ["10.1", "10.3"],
            "最高": ["10.5", "10.6"],
            "最低": ["10.0", "10.2"],
            "收盘": ["10.4", "10.5"],
            "成交量": ["100000", "120000"],
            "成交额": ["1000000", "1250000"],
            "换手率": ["1.2", "1.3"],
        }
    )


def _yfinance_history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(["2025-03-19", "2025-03-20"]),
            "Open": [10.1, 10.3],
            "High": [10.5, 10.6],
            "Low": [10.0, 10.2],
            "Close": [10.4, 10.5],
            "Volume": [100000, 120000],
        }
    )


class AShareSupportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = build_security_profile("600519", "cn_a")

    def test_prefers_akshare_history_and_standardizes_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch(
                    "tradingagents.dataflows.a_share_support.get_config",
                    return_value={"data_cache_dir": temp_dir},
                ),
                patch(
                    "tradingagents.dataflows.a_share_support._safe_akshare_call",
                    return_value=_akshare_history_frame(),
                ),
            ):
                history = get_a_share_history(self.profile, "2025-03-01", "2025-03-21")

        self.assertFalse(history.empty)
        self.assertIn("Date", history.columns)
        self.assertIn("Close", history.columns)
        self.assertEqual(history.iloc[-1]["Date"], "2025-03-20")

    def test_fallback_handles_yfinance_none_like_failure_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ticker = MagicMock()
            ticker.history.side_effect = TypeError("'NoneType' object is not subscriptable")
            with (
                patch(
                    "tradingagents.dataflows.a_share_support.get_config",
                    return_value={"data_cache_dir": temp_dir},
                ),
                patch(
                    "tradingagents.dataflows.a_share_support._safe_akshare_call",
                    return_value=None,
                ),
                patch(
                    "tradingagents.dataflows.a_share_support.call_with_proxy_fallback",
                    side_effect=[pd.DataFrame(), pd.DataFrame(), TypeError("boom"), TypeError("boom")],
                ),
                patch(
                    "tradingagents.dataflows.a_share_support.yf.Ticker",
                    return_value=ticker,
                ),
            ):
                history = get_a_share_history(self.profile, "2025-03-01", "2025-03-21")

        self.assertIsInstance(history, pd.DataFrame)
        self.assertTrue(history.empty)

    def test_fallback_uses_cached_history_when_live_sources_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "a_share_history"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / f"{self.profile.normalized_ticker}-2025-03-01-2025-03-21.csv"
            _yfinance_history_frame().to_csv(cache_path, index=False, encoding="utf-8-sig")

            with (
                patch(
                    "tradingagents.dataflows.a_share_support.get_config",
                    return_value={"data_cache_dir": temp_dir},
                ),
                patch(
                    "tradingagents.dataflows.a_share_support._safe_akshare_call",
                    return_value=None,
                ),
                patch(
                    "tradingagents.dataflows.a_share_support.call_with_proxy_fallback",
                    side_effect=RuntimeError("network down"),
                ),
            ):
                history = get_a_share_history(self.profile, "2025-03-01", "2025-03-21")

        self.assertFalse(history.empty)
        self.assertEqual(history.iloc[-1]["Date"], "2025-03-20")

    def test_fallback_handles_duplicate_yfinance_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            frame = _yfinance_history_frame()
            duplicate = frame.copy()
            duplicate.columns = pd.MultiIndex.from_tuples(
                [
                    ("Date", ""),
                    ("Open", "600519.SS"),
                    ("High", "600519.SS"),
                    ("Low", "600519.SS"),
                    ("Close", "600519.SS"),
                    ("Volume", "600519.SS"),
                ]
            )

            with (
                patch(
                    "tradingagents.dataflows.a_share_support.get_config",
                    return_value={"data_cache_dir": temp_dir},
                ),
                patch(
                    "tradingagents.dataflows.a_share_support._safe_akshare_call",
                    return_value=None,
                ),
                patch(
                    "tradingagents.dataflows.a_share_support.call_with_proxy_fallback",
                    side_effect=[duplicate, duplicate],
                ),
            ):
                history = get_a_share_history(self.profile, "2025-03-01", "2025-03-21")

        self.assertFalse(history.empty)
        self.assertIn("Close", history.columns)
        self.assertEqual(history.iloc[-1]["Date"], "2025-03-20")


if __name__ == "__main__":
    unittest.main()
