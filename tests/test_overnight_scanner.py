from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.market_regime import IndexSnapshotResult
from tradingagents.overnight.models import MarketRegime, OvernightSnapshot, TailMetrics
from tradingagents.overnight.scanner import run_overnight_scan


def make_snapshot(code: str, name: str, pool: str) -> OvernightSnapshot:
    profile = build_security_profile(code, "cn_a")
    return OvernightSnapshot(
        profile=profile,
        name=name,
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
        pool=pool,  # type: ignore[arg-type]
    )


class OvernightScannerTests(unittest.TestCase):
    def test_proxy_tail_can_only_enter_watchlist(self) -> None:
        real_snapshot = make_snapshot("600519", "贵州茅台", "main")
        proxy_snapshot = make_snapshot("300750", "宁德时代", "gem")
        raw_spot = pd.DataFrame({"代码": ["600519", "300750"]})
        raw_spot.attrs["provider_route"] = "akshare:spot"

        def fake_tail_batch(snapshots, _trade_date, *_args, **_kwargs):
            metrics: dict[str, TailMetrics] = {}
            for snapshot in snapshots:
                if snapshot.code == real_snapshot.code:
                    metrics[snapshot.code] = TailMetrics(
                        has_real_tail_data=True,
                        source="unit-test",
                        quality="real",
                        tail_return_pct=0.88,
                        tail_amount_ratio=0.18,
                    )
                else:
                    metrics[snapshot.code] = TailMetrics(
                        has_real_tail_data=False,
                        source="unit-test",
                        quality="proxy",
                        tail_return_pct=0.36,
                        tail_amount_ratio=0.11,
                    )
            return metrics

        def fake_score(snapshot, *_args, **_kwargs):
            if snapshot.code == real_snapshot.code:
                return 82.0, {"trend_strength": 22.0}
            return 80.0, {"trend_strength": 20.0}

        with tempfile.TemporaryDirectory() as tempdir:
            with (
                patch(
                    "tradingagents.overnight.scanner.load_index_snapshot",
                    return_value=IndexSnapshotResult(
                        values={"上证指数": 0.8},
                        provider_route="akshare_index_spot_sina",
                    ),
                ),
                patch(
                    "tradingagents.overnight.scanner.evaluate_market_regime",
                    return_value=MarketRegime(
                        market_ok=True,
                        market_message="市场正常，可执行标准阈值。",
                        benchmark_pct=0.8,
                    ),
                ),
                patch(
                    "tradingagents.overnight.scanner.load_market_spot_table",
                    return_value=raw_spot,
                ),
                patch(
                    "tradingagents.overnight.scanner.build_dynamic_pool_from_frame",
                    return_value=pd.DataFrame({"代码": ["600519", "300750"]}),
                ),
                patch(
                    "tradingagents.overnight.scanner.build_snapshots_from_pool_frame",
                    return_value=[real_snapshot, proxy_snapshot],
                ),
                patch(
                    "tradingagents.overnight.scanner.load_risk_stocks",
                    return_value=(set(), {"matched_events": 0, "risk_codes": 0, "scanned_days": 0}),
                ),
                patch(
                    "tradingagents.overnight.scanner.check_buy_filters",
                    return_value=(True, ""),
                ),
                patch(
                    "tradingagents.overnight.scanner.pick_history_enrichment_list",
                    return_value=[real_snapshot.code, proxy_snapshot.code],
                ),
                patch(
                    "tradingagents.overnight.scanner.load_history_frame",
                    return_value=pd.DataFrame({"close": [1, 2, 3]}),
                ),
                patch(
                    "tradingagents.overnight.scanner.pick_tail_enrichment_list",
                    return_value=[real_snapshot.code, proxy_snapshot.code],
                ),
                patch(
                    "tradingagents.overnight.scanner.load_tail_metrics_batch",
                    side_effect=fake_tail_batch,
                ),
                patch(
                    "tradingagents.overnight.scanner.calculate_total_score",
                    side_effect=fake_score,
                ),
            ):
                result = run_overnight_scan(
                    trade_date="2025-03-20",
                    mode="research_fallback",
                    data_dir=Path(tempdir),
                )

        formal = result["formal_recommendations"]
        preliminary = result["preliminary_candidates"]
        scored = result["total_score_candidates"]
        watchlist = result["watchlist"]
        rejected = result["rejected_candidates"]
        self.assertEqual(result["summary"]["data_quality"]["status"], "research_fallback")
        self.assertEqual(result["summary"]["provider_route"]["spot"], "akshare:spot")
        self.assertEqual(result["summary"]["universe_snapshot_date"], "2025-03-20")
        self.assertEqual(result["summary"]["scored_count"], 2)
        self.assertEqual(result["summary"]["rejected_count"], 0)
        self.assertEqual(len(preliminary), 2)
        self.assertEqual(preliminary[0]["ticker"], "600519.SS")
        self.assertEqual(preliminary[1]["ticker"], "300750.SZ")
        self.assertEqual(len(scored), 2)
        self.assertEqual(scored[0]["selection_stage"], "formal")
        self.assertEqual(scored[1]["selection_stage"], "watchlist")
        self.assertEqual(len(formal), 1)
        self.assertEqual(formal[0]["ticker"], "600519.SS")
        self.assertEqual(formal[0]["quality"], "real")
        self.assertEqual(len(watchlist), 1)
        self.assertEqual(watchlist[0]["ticker"], "300750.SZ")
        self.assertEqual(watchlist[0]["quality"], "proxy")
        self.assertEqual(rejected, [])


if __name__ == "__main__":
    unittest.main()
