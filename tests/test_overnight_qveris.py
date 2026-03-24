from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from tradingagents.overnight.market_regime import IndexSnapshotResult, load_index_snapshot
from tradingagents.overnight.universe import _load_market_spot_table_from_qveris


class OvernightQVerisFallbackTests(unittest.TestCase):
    def test_index_snapshot_falls_back_to_qveris(self) -> None:
        with (
            patch("tradingagents.overnight.market_regime.datetime") as mocked_datetime,
            patch(
                "tradingagents.overnight.market_regime.ak.stock_zh_index_spot_sina",
                side_effect=RuntimeError("akshare down"),
            ),
            patch(
                "tradingagents.overnight.market_regime._load_index_snapshot_from_qveris",
                return_value=IndexSnapshotResult(
                    values={"上证指数": 0.12, "沪深300": 0.21, "创业板指": -0.05},
                    provider_route="qveris:tool-1",
                    qveris_tool_ids=["tool-1"],
                ),
            ),
        ):
            mocked_datetime.now.return_value.strftime.return_value = "2025-03-20"
            result = load_index_snapshot("2025-03-20")

        self.assertEqual(result.provider_route, "qveris:tool-1")
        self.assertEqual(result.values["上证指数"], 0.12)
        self.assertEqual(result.qveris_tool_ids, ["tool-1"])

    def test_qveris_spot_rows_are_normalized_for_dynamic_pool(self) -> None:
        seed_frame = pd.DataFrame(
            [
                {"code": "600519", "name": "贵州茅台", "score": 80.0, "amount": 2.45e9},
                {"code": "300750", "name": "宁德时代", "score": 70.0, "amount": 8.6e8},
            ]
        )
        tool = {
            "tool_id": "ths_ifind.real_time_quotation.v1",
            "discovery_id": "search-1",
            "supports_batch": True,
            "batch_parameter_name": "codes",
        }
        response = {
            "result": {
                "data": [
                    [
                        {
                            "thscode": "600519.SH",
                            "latest": 1620.5,
                            "preClose": 1600.0,
                            "open": 1608.0,
                            "high": 1625.0,
                            "low": 1605.0,
                            "amount": 2.45e9,
                            "turnoverRatio": 1.25,
                            "changeRatio": 1.2813,
                        },
                        {
                            "thscode": "300750.SZ",
                            "latest": 205.0,
                            "preClose": 202.0,
                            "open": 203.5,
                            "high": 206.0,
                            "low": 201.5,
                            "amount": 8.6e8,
                            "turnoverRatio": 2.8,
                            "changeRatio": 1.4851,
                        },
                    ]
                ]
            }
        }

        with (
            patch(
                "tradingagents.overnight.universe._load_recent_universe_seed_frame",
                return_value=seed_frame,
            ),
            patch("tradingagents.overnight.universe.QVerisToolRegistry.ensure_tool", return_value=tool),
            patch("tradingagents.overnight.universe.QVerisClient.call_tool", return_value=response),
        ):
            frame = _load_market_spot_table_from_qveris()

        self.assertEqual(frame.attrs["provider_route"], "qveris:ths_ifind.real_time_quotation.v1")
        self.assertEqual(
            list(frame.columns),
            ["代码", "名称", "最新价", "涨跌幅", "成交额", "换手率", "昨收", "今开", "最高", "最低", "_provider_route"],
        )
        self.assertEqual(frame.iloc[0]["代码"], "600519")
        self.assertEqual(frame.iloc[1]["名称"], "宁德时代")


if __name__ == "__main__":
    unittest.main()
