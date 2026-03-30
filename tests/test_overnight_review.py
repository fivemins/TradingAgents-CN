from __future__ import annotations

from contextlib import ExitStack
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.market_regime import IndexSnapshotResult
from tradingagents.overnight.models import MarketRegime, ScanParams, TailMetrics
from tradingagents.overnight.review import _load_review_universe_for_date, run_overnight_review


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
    frame.loc[frame["Date"] == "2025-03-17", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [
        10.0,
        10.3,
        9.9,
        10.2,
        1.4e9,
        3.8,
    ]
    frame.loc[frame["Date"] == "2025-03-18", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [
        10.5,
        10.8,
        10.4,
        10.6,
        1.8e9,
        4.1,
    ]
    frame.loc[frame["Date"] == "2025-03-19", ["Open", "High", "Low", "Close", "Turnover", "TurnoverRate"]] = [
        10.4,
        10.6,
        10.1,
        10.3,
        1.6e9,
        3.4,
    ]
    return frame


def make_minute_frame(trade_date: str, prices_by_time: dict[str, float]) -> pd.DataFrame:
    rows = []
    for time_text, price in prices_by_time.items():
        rows.append(
            {
                "datetime": f"{trade_date} {time_text}:00",
                "open": price,
                "close": price,
                "high": price,
                "low": price,
                "volume": 1000,
                "amount": price * 1000,
            }
        )
    return pd.DataFrame(rows)


class OvernightReviewTests(unittest.TestCase):
    def _build_universe(self, *codes: str) -> list[dict[str, object]]:
        universe: list[dict[str, object]] = []
        for code in codes:
            profile = build_security_profile(code, "cn_a")
            pool = "gem" if code.startswith("300") else "main"
            universe.append(
                {
                    "code": profile.normalized_ticker,
                    "profile": profile,
                    "name": profile.normalized_ticker,
                    "pool": pool,
                }
            )
        return universe

    def _tail_metrics(self, quality: str) -> TailMetrics:
        return TailMetrics(
            has_real_tail_data=quality == "real",
            source="unit-test",
            quality=quality,  # type: ignore[arg-type]
            tail_return_pct=0.82,
            tail_amount_ratio=0.18,
        )

    def _run_review(
        self,
        tempdir: str,
        *,
        return_basis: str = "buy_1455_sell_next_day_1000",
        trade_dates: list[str] | None = None,
        universe: list[dict[str, object]] | None = None,
        total_score_by_code: dict[str, float] | None = None,
        quick_score_by_code: dict[str, float] | None = None,
        tail_quality_by_code: dict[str, str] | None = None,
        intraday_frames: dict[tuple[str, str], pd.DataFrame] | None = None,
        intraday_benchmark_map: dict[str, dict[str, object]] | None = None,
        next_open_benchmark_map: dict[str, float | None] | None = None,
    ) -> dict:
        trade_dates = trade_dates or ["2025-03-17", "2025-03-18"]
        universe = universe or self._build_universe("600519")
        total_score_by_code = total_score_by_code or {
            str(item["code"]): 80.0 for item in universe
        }
        quick_score_by_code = quick_score_by_code or {
            str(item["code"]): 70.0 for item in universe
        }
        tail_quality_by_code = tail_quality_by_code or {
            str(item["code"]): "real" for item in universe
        }
        intraday_frames = intraday_frames or {}
        if intraday_benchmark_map is None:
            intraday_benchmark_map = {
                trade_date: {
                    "next_trade_date": "2025-03-18" if trade_date == "2025-03-17" else "2025-03-19",
                    "entry_price": 1.0,
                    "entry_time_used": "14:55",
                    "exit_price": 1.0,
                    "exit_time_used": "10:00",
                    "benchmark_return": 0.0,
                }
                for trade_date in trade_dates
            }
        next_open_benchmark_map = next_open_benchmark_map or {
            trade_date: 0.0 for trade_date in trade_dates
        }

        def fake_calc_quick_score(snapshot, _benchmark_pct: float) -> float:
            return quick_score_by_code[snapshot.code]

        def fake_load_tail_metrics(_profile, snapshot, *_args):
            quality = tail_quality_by_code.get(snapshot.code, "missing")
            return self._tail_metrics(quality)

        def fake_calculate_total_score(snapshot, *_args):
            score = total_score_by_code[snapshot.code]
            return score, {"trend_strength": round(score * 0.3, 2), "tail_strength": 18.0}

        def fake_load_intraday_minute_frame(profile, trade_date: str, _cache_root: Path):
            frame = intraday_frames.get((profile.normalized_ticker, trade_date), pd.DataFrame())
            return frame.copy(), "unit-test-minute"

        with ExitStack() as stack:
            stack.enter_context(
                patch("tradingagents.overnight.review._load_trade_dates", return_value=trade_dates)
            )
            if return_basis == "next_open":
                stack.enter_context(
                    patch(
                        "tradingagents.overnight.review._build_benchmark_map",
                        return_value=next_open_benchmark_map,
                    )
                )
            else:
                stack.enter_context(
                    patch(
                        "tradingagents.overnight.review._build_intraday_benchmark_map",
                        return_value=intraday_benchmark_map,
                    )
                )
            stack.enter_context(
                patch("tradingagents.overnight.review._build_review_universe", return_value=universe)
            )
            stack.enter_context(
                patch("tradingagents.overnight.review.load_history_frame", return_value=make_history_frame())
            )
            stack.enter_context(
                patch(
                    "tradingagents.overnight.review.load_index_snapshot",
                    return_value=IndexSnapshotResult(
                        values={"上证指数": 0.6},
                        provider_route="akshare_index_daily",
                    ),
                )
            )
            stack.enter_context(
                patch(
                    "tradingagents.overnight.review.evaluate_market_regime",
                    return_value=MarketRegime(
                        market_ok=True,
                        market_message="市场正常",
                        benchmark_pct=0.4,
                    ),
                )
            )
            stack.enter_context(
                patch(
                    "tradingagents.overnight.review.load_risk_stocks",
                    return_value=(set(), {"matched_events": 0, "risk_codes": 0, "scanned_days": 0}),
                )
            )
            stack.enter_context(
                patch("tradingagents.overnight.review.check_buy_filters", return_value=(True, "通过"))
            )
            stack.enter_context(
                patch("tradingagents.overnight.review.calc_quick_score", side_effect=fake_calc_quick_score)
            )
            stack.enter_context(
                patch("tradingagents.overnight.review.load_tail_metrics", side_effect=fake_load_tail_metrics)
            )
            stack.enter_context(
                patch(
                    "tradingagents.overnight.review.calculate_total_score",
                    side_effect=fake_calculate_total_score,
                )
            )
            stack.enter_context(
                patch(
                    "tradingagents.overnight.review.load_intraday_minute_frame",
                    side_effect=fake_load_intraday_minute_frame,
                )
            )
            return run_overnight_review(
                end_trade_date=trade_dates[-1],
                data_dir=Path(tempdir),
                window_days=len(trade_dates),
                return_basis=return_basis,  # type: ignore[arg-type]
            )

    def test_run_overnight_review_preserves_legacy_next_open_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = self._run_review(
                tempdir,
                return_basis="next_open",
                next_open_benchmark_map={"2025-03-17": 0.3, "2025-03-18": -0.2},
            )

        summary = result["summary"]
        self.assertEqual(summary["return_basis"], "next_open")
        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["days_evaluated"], 2)
        self.assertEqual(summary["days_with_trade"], 2)
        self.assertEqual(summary["days_with_formal_picks"], 2)
        self.assertTrue(summary["has_valid_samples"])
        self.assertAlmostEqual(summary["avg_strategy_return"], 0.5272, places=4)
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

    def test_run_overnight_review_defaults_to_intraday_top1_basis(self) -> None:
        code = build_security_profile("600519", "cn_a").normalized_ticker
        intraday_frames = {
            (code, "2025-03-17"): make_minute_frame("2025-03-17", {"14:55": 10.0}),
            (code, "2025-03-18"): make_minute_frame("2025-03-18", {"10:00": 10.2, "14:55": 10.0}),
            (code, "2025-03-19"): make_minute_frame("2025-03-19", {"10:00": 9.8}),
        }
        intraday_benchmark_map = {
            "2025-03-17": {
                "next_trade_date": "2025-03-18",
                "entry_price": 4000.0,
                "entry_time_used": "14:55",
                "exit_price": 4020.0,
                "exit_time_used": "10:00",
                "benchmark_return": 0.5,
            },
            "2025-03-18": {
                "next_trade_date": "2025-03-19",
                "entry_price": 4025.0,
                "entry_time_used": "14:55",
                "exit_price": 4004.875,
                "exit_time_used": "10:00",
                "benchmark_return": -0.5,
            },
        }

        with tempfile.TemporaryDirectory() as tempdir:
            result = self._run_review(
                tempdir,
                intraday_frames=intraday_frames,
                intraday_benchmark_map=intraday_benchmark_map,
            )

        summary = result["summary"]
        self.assertEqual(summary["return_basis"], "buy_1455_sell_next_day_1000")
        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["days_with_trade"], 2)
        self.assertAlmostEqual(summary["avg_strategy_return"], 0.0, places=4)
        self.assertAlmostEqual(summary["median_strategy_return"], 0.0, places=4)
        self.assertAlmostEqual(summary["avg_benchmark_return"], 0.0, places=4)
        self.assertAlmostEqual(summary["avg_excess_return"], 0.0, places=4)
        self.assertAlmostEqual(summary["positive_pick_rate"], 0.5, places=4)
        self.assertEqual(result["candidate_results"][0]["category"], "selected")
        self.assertEqual(result["candidate_results"][0]["entry_time_used"], "14:55")
        self.assertEqual(result["candidate_results"][0]["exit_time_used"], "10:00")
        self.assertEqual(result["daily_results"][0]["selected_ticker"], code)
        self.assertEqual(result["daily_results"][0]["trade_count"], 1)
        self.assertEqual(result["daily_results"][0]["entry_time_used"], "14:55")
        self.assertEqual(result["daily_results"][0]["exit_time_used"], "10:00")

    def test_intraday_review_uses_nearest_minute_prices_with_tie_breakers(self) -> None:
        code = build_security_profile("600519", "cn_a").normalized_ticker
        intraday_frames = {
            (code, "2025-03-18"): make_minute_frame("2025-03-18", {"14:54": 10.0, "14:56": 10.4}),
            (code, "2025-03-19"): make_minute_frame("2025-03-19", {"09:59": 10.2, "10:01": 10.6}),
        }
        intraday_benchmark_map = {
            "2025-03-18": {
                "next_trade_date": "2025-03-19",
                "entry_price": 4000.0,
                "entry_time_used": "14:55",
                "exit_price": 4000.0,
                "exit_time_used": "10:00",
                "benchmark_return": 0.0,
            }
        }

        with tempfile.TemporaryDirectory() as tempdir:
            result = self._run_review(
                tempdir,
                trade_dates=["2025-03-18"],
                intraday_frames=intraday_frames,
                intraday_benchmark_map=intraday_benchmark_map,
            )

        candidate = result["candidate_results"][0]
        self.assertEqual(candidate["entry_time_used"], "14:54")
        self.assertEqual(candidate["exit_time_used"], "10:01")
        self.assertAlmostEqual(candidate["entry_price"], 10.0, places=4)
        self.assertAlmostEqual(candidate["exit_price"], 10.6, places=4)
        self.assertAlmostEqual(candidate["strategy_return"], 6.0, places=4)
        self.assertEqual(result["daily_results"][0]["entry_time_used"], "14:54")
        self.assertEqual(result["daily_results"][0]["exit_time_used"], "10:01")

    def test_intraday_review_selects_top_score_even_without_real_tail_and_does_not_fallback(self) -> None:
        universe = self._build_universe("300750", "600519")
        top_code = str(universe[0]["code"])
        backup_code = str(universe[1]["code"])
        intraday_frames = {
            (backup_code, "2025-03-18"): make_minute_frame("2025-03-18", {"14:55": 20.0}),
            (backup_code, "2025-03-19"): make_minute_frame("2025-03-19", {"10:00": 21.0}),
        }
        intraday_benchmark_map = {
            "2025-03-18": {
                "next_trade_date": "2025-03-19",
                "entry_price": 4000.0,
                "entry_time_used": "14:55",
                "exit_price": 4012.0,
                "exit_time_used": "10:00",
                "benchmark_return": 0.3,
            }
        }

        with tempfile.TemporaryDirectory() as tempdir:
            result = self._run_review(
                tempdir,
                trade_dates=["2025-03-18"],
                universe=universe,
                total_score_by_code={top_code: 90.0, backup_code: 80.0},
                quick_score_by_code={top_code: 82.0, backup_code: 79.0},
                tail_quality_by_code={top_code: "proxy", backup_code: "real"},
                intraday_frames=intraday_frames,
                intraday_benchmark_map=intraday_benchmark_map,
            )

        summary = result["summary"]
        candidate = result["candidate_results"][0]
        daily = result["daily_results"][0]
        self.assertEqual(candidate["ticker"], top_code)
        self.assertEqual(candidate["quality"], "proxy")
        self.assertFalse(candidate["counted_in_performance"])
        self.assertEqual(candidate["skipped_reason"], "missing_entry_price")
        self.assertEqual(daily["selected_ticker"], top_code)
        self.assertIsNone(daily["strategy_return"])
        self.assertEqual(summary["trade_count"], 0)
        self.assertEqual(summary["days_with_trade"], 0)
        self.assertFalse(summary["has_valid_samples"])
        self.assertEqual(result["audit"]["missing_trade_price_count"], 1)
        self.assertNotIn(backup_code, [item["ticker"] for item in result["candidate_results"]])

    def test_same_frozen_inputs_produce_identical_review_outputs(self) -> None:
        code = build_security_profile("600519", "cn_a").normalized_ticker
        intraday_frames = {
            (code, "2025-03-17"): make_minute_frame("2025-03-17", {"14:55": 10.0}),
            (code, "2025-03-18"): make_minute_frame("2025-03-18", {"10:00": 10.2, "14:55": 10.0}),
            (code, "2025-03-19"): make_minute_frame("2025-03-19", {"10:00": 9.8}),
        }

        with tempfile.TemporaryDirectory() as tempdir:
            first = self._run_review(tempdir, intraday_frames=intraday_frames)
            second = self._run_review(tempdir, intraday_frames=intraday_frames)

        self.assertEqual(first["summary"], second["summary"])
        self.assertEqual(first["daily_results"], second["daily_results"])
        self.assertEqual(first["candidate_results"], second["candidate_results"])

    def test_missing_snapshot_date_falls_back_to_latest_saved_snapshot_before_trade_date(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            cache_root = Path(tempdir) / "overnight_cache"
            snapshot_dir = cache_root / "universe"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            for snapshot_date in ("2025-03-16", "2025-03-18"):
                (snapshot_dir / f"{snapshot_date}.parquet").write_bytes(b"placeholder")

            snapshot_frame = pd.DataFrame(
                [
                    {"code": "600519", "name": "贵州茅台", "pool": "main"},
                ]
            )

            def fake_snapshot_loader(_: Path, requested_date: str) -> pd.DataFrame:
                if requested_date in {"2025-03-16", "2025-03-18"}:
                    return snapshot_frame
                return pd.DataFrame()

            with (
                patch("tradingagents.overnight.review.load_universe_snapshot", side_effect=fake_snapshot_loader),
                patch(
                    "tradingagents.overnight.review._build_review_universe",
                    side_effect=AssertionError("live universe fallback should not be used when saved snapshots exist"),
                ),
            ):
                universe, snapshot_date, bias_flags = _load_review_universe_for_date(
                    "2025-03-17",
                    cache_root,
                    ScanParams(),
                )

        self.assertEqual(snapshot_date, "2025-03-16")
        self.assertIn("survivorship_bias", bias_flags)
        self.assertIn("saved_snapshot_fallback", bias_flags)
        self.assertEqual(universe[0]["code"], "600519.SS")


if __name__ == "__main__":
    unittest.main()
