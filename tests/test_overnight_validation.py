from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.overnight.validation import validate_scan_candidates


class OvernightValidationTests(unittest.TestCase):
    def test_validate_scan_candidates_computes_next_open_returns(self) -> None:
        history = pd.DataFrame(
            {
                "Date": ["2025-03-19", "2025-03-20", "2025-03-21"],
                "Open": [100.0, 101.0, 104.0],
                "Close": [101.0, 102.0, 105.0],
            }
        )
        candidates = [
            {
                "bucket": "formal",
                "ticker": "600519.SS",
                "name": "贵州茅台",
                "latest": 102.0,
            },
            {
                "bucket": "watchlist",
                "ticker": "300750.SZ",
                "name": "宁德时代",
                "latest": 210.0,
            },
        ]

        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.validation.load_history_frame",
                return_value=history,
            ):
                result = validate_scan_candidates(
                    trade_date="2025-03-20",
                    market_region="cn_a",
                    candidates=candidates,
                    data_dir=Path(tempdir),
                )

        self.assertEqual(result["summary"]["validation_status"], "validated")
        self.assertEqual(result["summary"]["validated_formal_count"], 1)
        self.assertAlmostEqual(result["summary"]["avg_next_open_return"], 1.9608, places=4)
        self.assertEqual(result["candidates"][0]["validation_status"], "validated")
        self.assertEqual(result["candidates"][1]["validation_status"], "watchlist_only")

    def test_validate_scan_candidates_marks_pending_without_next_open(self) -> None:
        history = pd.DataFrame(
            {
                "Date": ["2025-03-19", "2025-03-20"],
                "Open": [100.0, 101.0],
                "Close": [101.0, 102.0],
            }
        )
        candidates = [{"bucket": "formal", "ticker": "600519.SS", "name": "贵州茅台", "latest": 102.0}]

        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "tradingagents.overnight.validation.load_history_frame",
                return_value=history,
            ):
                result = validate_scan_candidates(
                    trade_date="2025-03-20",
                    market_region="cn_a",
                    candidates=candidates,
                    data_dir=Path(tempdir),
                )

        self.assertEqual(result["summary"]["validation_status"], "pending")
        self.assertEqual(result["summary"]["validated_formal_count"], 0)
        self.assertIsNone(result["candidates"][0]["next_open_return"])
