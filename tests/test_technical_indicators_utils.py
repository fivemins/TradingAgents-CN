from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.dataflows.interface import get_technical_indicators_window
from tradingagents.dataflows.technical_indicators_utils import TechnicalIndicatorsUtils


def _sample_price_frame() -> pd.DataFrame:
    dates = pd.bdate_range(end="2025-03-21", periods=260)
    frame = pd.DataFrame(
        {
            "Date": dates,
            "Open": [100 + index * 0.4 for index in range(len(dates))],
            "High": [101 + index * 0.4 for index in range(len(dates))],
            "Low": [99 + index * 0.4 for index in range(len(dates))],
            "Close": [100.5 + index * 0.4 for index in range(len(dates))],
            "Volume": [1_000_000 + index * 1_000 for index in range(len(dates))],
        }
    )
    return frame.set_index("Date")


class TechnicalIndicatorsUtilsTests(unittest.TestCase):
    def test_online_cache_recovers_from_corrupted_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            symbol = "600519.SS"
            fixed_today = pd.Timestamp("2026-03-23")
            cache_file = cache_dir / f"{symbol}-YFin-data-2011-03-23-2026-03-23.csv"
            cache_file.write_text("bad,data\n1,2,3\n", encoding="utf-8")

            with (
                patch(
                    "tradingagents.dataflows.technical_indicators_utils.get_config",
                    return_value={"data_cache_dir": str(cache_dir)},
                ),
                patch(
                    "tradingagents.dataflows.technical_indicators_utils.pd.Timestamp.today",
                    return_value=fixed_today,
                ),
                patch(
                    "tradingagents.dataflows.technical_indicators_utils.yf.download",
                    return_value=_sample_price_frame(),
                ),
            ):
                value = TechnicalIndicatorsUtils.get_indicator_value(
                    symbol=symbol,
                    indicator="close_50_sma",
                    curr_date="2025-03-20",
                    data_dir=str(cache_dir),
                    online=True,
                )

            self.assertIsInstance(value, float)
            repaired = pd.read_csv(cache_file)
            self.assertEqual(
                repaired.columns.tolist(),
                ["Date", "Open", "High", "Low", "Close", "Volume"],
            )

    def test_pandas_indicator_engine_keeps_core_relationships(self) -> None:
        frame = _sample_price_frame().reset_index()
        with patch.object(
            TechnicalIndicatorsUtils, "_load_price_frame", return_value=frame
        ):
            sma = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "close_50_sma", "2025-03-20", "unused", True
            )
            ema = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "close_10_ema", "2025-03-20", "unused", True
            )
            macd = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "macd", "2025-03-20", "unused", True
            )
            macds = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "macds", "2025-03-20", "unused", True
            )
            macdh = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "macdh", "2025-03-20", "unused", True
            )
            boll = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "boll", "2025-03-20", "unused", True
            )
            boll_ub = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "boll_ub", "2025-03-20", "unused", True
            )
            boll_lb = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "boll_lb", "2025-03-20", "unused", True
            )
            rsi = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "rsi", "2025-03-20", "unused", True
            )
            atr = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "atr", "2025-03-20", "unused", True
            )
            vwma = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "vwma", "2025-03-20", "unused", True
            )
            mfi = TechnicalIndicatorsUtils.get_indicator_series(
                "600519.SS", "mfi", "2025-03-20", "unused", True
            )

        self.assertAlmostEqual(
            float(sma.iloc[-1]["close_50_sma"]),
            float(frame["Close"].tail(50).mean()),
            places=6,
        )
        self.assertGreater(float(ema.iloc[-1]["close_10_ema"]), 0.0)
        self.assertAlmostEqual(
            float(macdh.iloc[-1]["macdh"]),
            float(macd.iloc[-1]["macd"]) - float(macds.iloc[-1]["macds"]),
            places=6,
        )
        self.assertGreaterEqual(
            float(boll_ub.iloc[-1]["boll_ub"]), float(boll.iloc[-1]["boll"])
        )
        self.assertGreaterEqual(
            float(boll.iloc[-1]["boll"]), float(boll_lb.iloc[-1]["boll_lb"])
        )
        self.assertTrue(0.0 <= float(rsi.iloc[-1]["rsi"]) <= 100.0)
        self.assertGreater(float(atr.iloc[-1]["atr"]), 0.0)
        self.assertTrue(
            frame["Close"].tail(20).min()
            <= float(vwma.iloc[-1]["vwma"])
            <= frame["Close"].tail(20).max()
        )
        self.assertTrue(0.0 <= float(mfi.iloc[-1]["mfi"]) <= 100.0)

    def test_indicator_window_reuses_single_series_load(self) -> None:
        series = pd.DataFrame(
            {
                "Date": ["2025-03-18", "2025-03-19", "2025-03-20"],
                "close_50_sma": [100.0, 101.0, 102.0],
            }
        )
        with patch(
            "tradingagents.dataflows.interface.TechnicalIndicatorsUtils.get_indicator_series",
            return_value=series,
        ) as mocked_series:
            output = get_technical_indicators_window(
                "600519",
                "close_50_sma",
                "2025-03-20",
                5,
                True,
            )

        mocked_series.assert_called_once()
        self.assertIn("2025-03-18: 100.0", output)
        self.assertIn("2025-03-20: 102.0", output)


if __name__ == "__main__":
    unittest.main()
