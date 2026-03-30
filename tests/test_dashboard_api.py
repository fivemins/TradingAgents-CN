from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from dashboard_api.app import create_app
from dashboard_api.repair import repair_dashboard_data
from dashboard_api.store import OvernightCandidateStore, OvernightScanStore, OvernightTrackedTradeStore


class DummyLauncher:
    def __init__(self):
        self.launched_tasks: list[str] = []
        self.launched_scans: list[str] = []
        self.launched_reviews: list[str] = []

    def launch(self, task_id: str) -> int:
        self.launched_tasks.append(task_id)
        return 4242

    def launch_task(self, task_id: str) -> int:
        self.launched_tasks.append(task_id)
        return 4242

    def launch_overnight_scan(self, scan_id: str) -> int:
        self.launched_scans.append(scan_id)
        return 4343

    def launch_overnight_review(self, review_id: str) -> int:
        self.launched_reviews.append(review_id)
        return 4444


class DashboardApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.launcher = DummyLauncher()
        self.app = create_app(data_dir=self.tempdir.name, launcher=self.launcher)
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _create_task(self, ticker: str = "600519", source_query: str = "") -> dict:
        response = self.client.post(
            f"/api/tasks{source_query}",
            json={
                "ticker": ticker,
                "analysis_date": "2025-03-20",
                "market_region": "cn_a",
                "analysts": ["market", "social", "news", "fundamentals"],
                "research_depth": 1,
                "llm_provider": "ark",
                "quick_think_llm": "doubao-seed-2.0-lite",
                "deep_think_llm": "doubao-seed-2.0-pro",
                "online_tools": True,
            },
        )
        self.assertEqual(response.status_code, 201)
        return response.json()

    def _create_scan(self, mode: str = "strict") -> dict:
        response = self.client.post(
            "/api/overnight/scans",
            json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "mode": mode,
            },
        )
        self.assertEqual(response.status_code, 201)
        return response.json()

    def _persist_scan_candidates(self, scan_id: str) -> OvernightCandidateStore:
        store = OvernightCandidateStore(self.app.state.settings.db_path)
        store.initialize()
        store.replace_scan_candidates(
            scan_id=scan_id,
            trade_date="2025-03-20",
            market_region="cn_a",
            formal_candidates=[
                {
                    "ticker": "600519.SS",
                    "name": "贵州茅台",
                    "pool": "主板",
                    "quality": "real",
                    "latest": 1620.0,
                    "pct": 1.8,
                    "amount": 2450000000.0,
                    "turnover": 1.2,
                    "quick_score": 78.5,
                    "total_score": 83.2,
                    "factor_breakdown": {"trend_strength": 22.0},
                    "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                    "filter_reason": None,
                    "excluded_from_final": None,
                }
            ],
            watchlist_candidates=[
                {
                    "ticker": "300750.SZ",
                    "name": "宁德时代",
                    "pool": "创业板",
                    "quality": "proxy",
                    "latest": 210.0,
                    "pct": 0.9,
                    "amount": 860000000.0,
                    "turnover": 2.8,
                    "quick_score": 68.1,
                    "total_score": 64.3,
                    "factor_breakdown": {"trend_strength": 18.0},
                    "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                    "filter_reason": None,
                    "excluded_from_final": None,
                }
            ],
        )
        return store

    def _create_review(self, end_trade_date: str = "2025-03-20") -> dict:
        response = self.client.post(
            "/api/overnight/reviews",
            json={
                "end_trade_date": end_trade_date,
                "market_region": "cn_a",
            },
        )
        self.assertEqual(response.status_code, 201)
        return response.json()

    def _mark_scan_succeeded(self, scan_id: str) -> None:
        scan_store = OvernightScanStore(self.app.state.settings.db_path)
        scan_store.initialize()
        scan_store.update_scan(
            scan_id,
            status="succeeded",
            progress_message="Overnight scan completed successfully.",
            market_message="市场偏强，允许扩大推荐池。",
            formal_count=1,
            watchlist_count=1,
            summary_json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "mode": "strict",
                "market_message": "市场偏强，允许扩大推荐池。",
                "formal_count": 1,
                "watchlist_count": 1,
                "provider_route": {"tail": "frozen-minute"},
            },
        )

    def _create_tracked_trade(self, source_bucket: str = "formal") -> dict:
        scan = self._create_scan("strict")
        self._mark_scan_succeeded(scan["scan_id"])
        response = self.client.post(
            "/api/overnight/trades",
            json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "scan_id": scan["scan_id"],
                "scan_mode": "strict",
                "source_bucket": source_bucket,
                "candidate": {
                    "ticker": "600519.SS",
                    "name": "贵州茅台",
                    "pool": "主板",
                    "quality": "real",
                    "quick_score": 78.5,
                    "total_score": 83.2,
                    "factor_breakdown": {"trend_strength": 22.0},
                    "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                },
            },
        )
        self.assertEqual(response.status_code, 201)
        return response.json()

    def test_options_endpoint_returns_defaults(self) -> None:
        response = self.client.get("/api/options")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("providers", payload)
        self.assertIn("defaults", payload)
        self.assertIn("market_regions", payload)
        self.assertEqual(payload["defaults"]["market_region"], "cn_a")

    def test_system_readiness_endpoint_returns_component_payload(self) -> None:
        with patch(
            "dashboard_api.app.collect_readiness",
            return_value={
                "checked_at": "2026-03-23T00:00:00+00:00",
                "ready": False,
                "components": {
                    "ark": {
                        "name": "ark",
                        "ok": True,
                        "status": "ok",
                        "message": "Ark gateway reachable.",
                    },
                    "frontend": {
                        "name": "frontend",
                        "ok": False,
                        "status": "error",
                        "message": "dist missing",
                    },
                    "qveris": {
                        "name": "qveris",
                        "ok": False,
                        "status": "error",
                        "message": "not configured",
                        "configured": False,
                        "active_keys": 0,
                        "rotation_enabled": False,
                    },
                },
            },
        ):
            response = self.client.get("/api/system/readiness")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["ready"])
        self.assertIn("ark", payload["components"])
        self.assertIn("frontend", payload["components"])
        self.assertIn("qveris", payload["components"])
        self.assertEqual(payload["components"]["qveris"]["active_keys"], 0)

    def test_create_task_persists_and_launches_runner(self) -> None:
        payload = self._create_task()
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["market_region"], "cn_a")
        self.assertEqual(payload["ticker"], "600519.SS")
        self.assertIsNone(payload["structured_summary"])
        self.assertEqual(len(self.launcher.launched_tasks), 1)

        task_response = self.client.get(f"/api/tasks/{payload['task_id']}")
        self.assertEqual(task_response.status_code, 200)
        self.assertEqual(task_response.json()["ticker"], "600519.SS")

    def test_terminate_task_marks_running_task_as_failed(self) -> None:
        payload = self._create_task()
        store = self.app.state.task_store
        store.update_task(
            payload["task_id"],
            status="running",
            stage="risk",
            progress_message="Running.",
            pid=999999,
        )

        with patch("dashboard_api.app.terminate_task_process") as terminate_process:
            response = self.client.post(f"/api/tasks/{payload['task_id']}/terminate")

        self.assertEqual(response.status_code, 200)
        terminate_process.assert_called_once_with(999999)
        detail = response.json()
        self.assertEqual(detail["status"], "failed")
        self.assertEqual(detail["stage"], "completed")
        self.assertEqual(detail["error_message"], "Task terminated by user.")
        self.assertEqual(detail["progress_message"], "Task terminated by user.")

    def test_delete_task_removes_row_and_artifacts(self) -> None:
        payload = self._create_task()
        artifact_dir = Path(payload["artifact_dir"])
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "final_decision.md").write_text("HOLD", encoding="utf-8")

        with patch("dashboard_api.app.terminate_task_process") as terminate_process:
            response = self.client.delete(f"/api/tasks/{payload['task_id']}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        terminate_process.assert_called_once_with(4242)
        self.assertFalse(artifact_dir.exists())
        missing = self.client.get(f"/api/tasks/{payload['task_id']}")
        self.assertEqual(missing.status_code, 404)

    def test_delete_task_unlinks_overnight_candidate(self) -> None:
        scan_payload = self._create_scan("strict")
        scan_id = scan_payload["scan_id"]
        self._persist_scan_candidates(scan_id)
        payload = self._create_task(
            "600519",
            source_query=(
                f"?source_type=overnight_scan&source_scan_id={scan_id}"
                "&source_trade_date=2025-03-20&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            ),
        )

        with patch("dashboard_api.app.terminate_task_process") as terminate_process:
            response = self.client.delete(f"/api/tasks/{payload['task_id']}")

        self.assertEqual(response.status_code, 200)
        terminate_process.assert_called_once_with(4242)
        candidate_store = OvernightCandidateStore(self.app.state.settings.db_path)
        candidate_store.initialize()
        candidate = candidate_store.get_candidate(scan_id, "600519.SS")
        self.assertIsNotNone(candidate)
        self.assertIsNone(candidate["linked_task_id"])

    def test_delete_overnight_scan_removes_row_candidates_and_artifacts(self) -> None:
        payload = self._create_scan("strict")
        scan_id = payload["scan_id"]
        artifact_dir = Path(payload["artifact_dir"])
        self._persist_scan_candidates(scan_id)
        (artifact_dir / "events.log").write_text("scan log", encoding="utf-8")

        with patch("dashboard_api.app.terminate_task_process") as terminate_process:
            response = self.client.delete(f"/api/overnight/scans/{scan_id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        terminate_process.assert_called_once_with(4343)
        self.assertFalse(artifact_dir.exists())
        missing = self.client.get(f"/api/overnight/scans/{scan_id}")
        self.assertEqual(missing.status_code, 404)

        candidate_store = OvernightCandidateStore(self.app.state.settings.db_path)
        candidate_store.initialize()
        self.assertEqual(candidate_store.list_candidates(scan_id), [])

    def test_delete_overnight_review_removes_row_and_artifacts(self) -> None:
        payload = self._create_review("2025-03-31")
        review_id = payload["review_id"]
        artifact_dir = Path(payload["artifact_dir"])
        (artifact_dir / "events.log").write_text("review log", encoding="utf-8")

        with patch("dashboard_api.app.terminate_task_process") as terminate_process:
            response = self.client.delete(f"/api/overnight/reviews/{review_id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        terminate_process.assert_called_once_with(4444)
        self.assertFalse(artifact_dir.exists())
        missing = self.client.get(f"/api/overnight/reviews/{review_id}")
        self.assertEqual(missing.status_code, 404)

    def test_create_tracked_trade_conflicts_on_duplicate_and_can_recreate_after_delete(self) -> None:
        payload = self._create_tracked_trade("total_score")
        self.assertEqual(payload["source_bucket"], "total_score")
        self.assertEqual(payload["status"], "pending_entry")
        self.assertEqual(payload["scan_mode"], "strict")

        duplicate_scan = self._create_scan("strict")
        self._mark_scan_succeeded(duplicate_scan["scan_id"])
        duplicate = self.client.post(
            "/api/overnight/trades",
            json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "scan_id": duplicate_scan["scan_id"],
                "scan_mode": "strict",
                "source_bucket": "watchlist",
                "candidate": {
                    "ticker": "300750.SZ",
                    "name": "宁德时代",
                    "pool": "创业板",
                    "quality": "proxy",
                    "quick_score": 68.1,
                    "total_score": 64.3,
                    "factor_breakdown": {"trend_strength": 18.0},
                    "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                },
            },
        )
        self.assertEqual(duplicate.status_code, 409)

        delete_response = self.client.delete(f"/api/overnight/trades/{payload['trade_id']}")
        self.assertEqual(delete_response.status_code, 200)

        recreated = self.client.post(
            "/api/overnight/trades",
            json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "scan_id": duplicate_scan["scan_id"],
                "scan_mode": "strict",
                "source_bucket": "watchlist",
                "candidate": {
                    "ticker": "300750.SZ",
                    "name": "宁德时代",
                    "pool": "创业板",
                    "quality": "proxy",
                    "quick_score": 68.1,
                    "total_score": 64.3,
                    "factor_breakdown": {"trend_strength": 18.0},
                    "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                },
            },
        )
        self.assertEqual(recreated.status_code, 201)
        self.assertEqual(recreated.json()["source_bucket"], "watchlist")

    def test_refresh_pending_tracked_trades_updates_status_and_stats(self) -> None:
        payload = self._create_tracked_trade("formal")

        with patch(
            "dashboard_api.app.refresh_tracked_trade",
            return_value={
                "entry_target_time": "14:55",
                "entry_price": 100.0,
                "entry_time_used": "14:55",
                "exit_target_time": "10:00",
                "exit_trade_date": "2025-03-21",
                "exit_price": 103.0,
                "exit_time_used": "10:00",
                "strategy_return": 3.0,
                "status": "validated",
                "last_error": None,
                "last_checked_at": "2025-03-21T02:01:00+00:00",
                "updated_at": "2025-03-21T02:01:00+00:00",
            },
        ):
            response = self.client.post("/api/overnight/trades/refresh-pending")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["stats"]["total_days"], 1)
        self.assertEqual(body["stats"]["validated_days"], 1)
        self.assertEqual(body["stats"]["avg_return"], 3.0)
        self.assertEqual(body["stats"]["win_rate"], 1.0)
        self.assertEqual(body["stats"]["cumulative_return"], 3.0)
        self.assertEqual(body["items"][0]["status"], "validated")
        self.assertEqual(body["items"][0]["strategy_return"], 3.0)

        detail = self.client.get(f"/api/overnight/trades/{payload['trade_id']}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["exit_trade_date"], "2025-03-21")

    def test_delete_scan_does_not_delete_tracked_trade(self) -> None:
        payload = self._create_tracked_trade("formal")
        scan_id = payload["scan_id"]

        response = self.client.delete(f"/api/overnight/scans/{scan_id}")
        self.assertEqual(response.status_code, 200)

        trades = self.client.get("/api/overnight/trades")
        self.assertEqual(trades.status_code, 200)
        self.assertEqual(len(trades.json()["items"]), 1)
        self.assertEqual(trades.json()["items"][0]["trade_id"], payload["trade_id"])

        store = OvernightTrackedTradeStore(self.app.state.settings.db_path)
        store.initialize()
        persisted = store.get_trade(payload["trade_id"])
        self.assertIsNotNone(persisted)

    def test_task_source_context_can_be_attached_without_changing_request_body(self) -> None:
        payload = self._create_task(
            source_query=(
                "?source_type=overnight_scan"
                "&source_scan_id=scan-001"
                "&source_trade_date=2025-03-20"
                "&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            )
        )
        self.assertEqual(payload["source_context"]["type"], "overnight_scan")
        self.assertEqual(payload["source_context"]["scan_id"], "scan-001")
        self.assertEqual(payload["source_context"]["trade_date"], "2025-03-20")

    def test_task_list_can_filter_by_source_type(self) -> None:
        self._create_task(
            source_query=(
                "?source_type=overnight_scan"
                "&source_scan_id=scan-001"
                "&source_trade_date=2025-03-20"
                "&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            )
        )
        self._create_task("000001")

        response = self.client.get("/api/tasks?source_type=overnight_scan")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["source_context"]["type"], "overnight_scan")

    def test_artifacts_endpoint_reads_saved_files(self) -> None:
        payload = self._create_task("000001")
        artifact_dir = Path(payload["artifact_dir"])
        reports_dir = artifact_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "market.md").write_text("# Market", encoding="utf-8")
        (artifact_dir / "final_decision.md").write_text("BUY", encoding="utf-8")
        (artifact_dir / "factor_snapshot.json").write_text(
            json.dumps(
                {
                    "recommended_action": "BUY",
                    "composite_score": 66.5,
                    "confidence": 0.72,
                    "scores": {
                        "technical": {
                            "score": 68,
                            "confidence": 0.7,
                            "summary": "ok",
                            "subscores": {
                                "trend_strength": {
                                    "score": 70,
                                    "value": {"last_close": 12.3},
                                    "summary": "trend ok",
                                    "weight": 0.35,
                                }
                            },
                            "top_positive_signals": [
                                {
                                    "signal": "趋势强弱",
                                    "value": "站上均线",
                                    "impact": "positive",
                                    "source": "price_history",
                                    "weight": 0.14,
                                }
                            ],
                            "top_negative_signals": [],
                            "confidence_drivers": ["行情完整"],
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "evidence_snapshot.json").write_text(
            json.dumps(
                {
                    "technical": {
                        "strengths": [
                            {
                                "signal": "趋势强弱",
                                "value": "站上均线",
                                "impact": "positive",
                                "source": "price_history",
                                "weight": 0.14,
                            }
                        ],
                        "risks": [],
                        "raw_metrics": [
                            {
                                "signal": "最新收盘价",
                                "value": 12.3,
                                "impact": "neutral",
                                "source": "price_history",
                                "weight": 0.0,
                            }
                        ],
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "structured_decision.json").write_text(
            json.dumps(
                {
                    "decision": "BUY",
                    "score": 66.5,
                    "confidence": 0.72,
                    "summary": "positive bias",
                    "source": "unit-test",
                    "threshold_policy": {
                        "style": "balanced",
                        "buy_at_or_above": 65,
                        "sell_at_or_below": 45,
                        "min_confidence_for_directional_call": 0.5,
                    },
                    "primary_drivers": ["technical: 趋势强弱 - 站上均线"],
                    "risk_flags": ["news: 新闻语义 - 需要观察监管口径"],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        response = self.client.get(f"/api/tasks/{payload['task_id']}/artifacts")
        self.assertEqual(response.status_code, 200)
        artifact_payload = response.json()
        self.assertEqual(artifact_payload["reports"]["market_report"], "# Market")
        self.assertEqual(artifact_payload["reports"]["final_trade_decision"], "BUY")
        self.assertEqual(
            artifact_payload["structured"]["structured_decision"]["decision"],
            "BUY",
        )
        self.assertEqual(
            artifact_payload["structured"]["factor_snapshot"]["recommended_action"],
            "BUY",
        )
        self.assertIn(
            "threshold_policy",
            artifact_payload["structured"]["structured_decision"],
        )

    def test_artifacts_endpoint_returns_null_for_missing_structured_payloads(self) -> None:
        payload = self._create_task("002595")
        artifact_dir = Path(payload["artifact_dir"])
        reports_dir = artifact_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "market.md").write_text("# Market", encoding="utf-8")

        response = self.client.get(f"/api/tasks/{payload['task_id']}/artifacts")
        self.assertEqual(response.status_code, 200)
        artifact_payload = response.json()
        self.assertIsNone(artifact_payload["structured"]["factor_snapshot"])
        self.assertIsNone(artifact_payload["structured"]["evidence_snapshot"])
        self.assertIsNone(artifact_payload["structured"]["structured_decision"])

    def test_task_detail_lazy_backfills_structured_summary(self) -> None:
        payload = self._create_task("300750")
        artifact_dir = Path(payload["artifact_dir"])
        (artifact_dir / "factor_snapshot.json").write_text(
            json.dumps(
                {
                    "recommended_action": "HOLD",
                    "composite_score": 58.4,
                    "confidence": 0.69,
                    "scores": {
                        "technical": {
                            "top_positive_signals": [
                                {
                                    "signal": "趋势强弱",
                                    "value": "站上SMA20",
                                    "impact": "positive",
                                    "source": "price_history",
                                    "weight": 0.12,
                                }
                            ],
                            "top_negative_signals": [
                                {
                                    "signal": "近期动量",
                                    "value": "20日收益率 -4.10%",
                                    "impact": "negative",
                                    "source": "price_history",
                                    "weight": 0.15,
                                }
                            ],
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "structured_decision.json").write_text(
            json.dumps(
                {
                    "decision": "HOLD",
                    "score": 58.4,
                    "confidence": 0.69,
                    "summary": "signals mixed",
                    "source": "unit-test",
                    "primary_drivers": ["technical: 趋势强弱 - 站上SMA20"],
                    "risk_flags": ["technical: 近期动量 - 20日收益率 -4.10%"],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        detail_response = self.client.get(f"/api/tasks/{payload['task_id']}")
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.json()
        self.assertEqual(
            detail_payload["structured_summary"]["recommended_action"],
            "HOLD",
        )
        self.assertIn("站上SMA20", detail_payload["structured_summary"]["primary_driver"])

        list_response = self.client.get("/api/tasks")
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.json()
        self.assertEqual(
            list_payload["items"][0]["structured_summary"]["recommended_action"],
            "HOLD",
        )

    def test_task_detail_normalizes_legacy_source_context_and_missing_structured_payloads(self) -> None:
        payload = self._create_task("600519")
        task_store = self.app.state.task_store
        task = task_store.get_task(payload["task_id"])
        self.assertIsNotNone(task)
        config_snapshot = dict(task["config_snapshot"])
        config_snapshot["source_context"] = {
            "type": "manual",
            "ticker": "600519.SS",
            "name": "????",
        }
        task_store.update_task(payload["task_id"], config_snapshot=config_snapshot)

        detail_response = self.client.get(f"/api/tasks/{payload['task_id']}")
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.json()
        self.assertEqual(detail_payload["source_context"]["name"], "600519.SS")

        artifact_response = self.client.get(f"/api/tasks/{payload['task_id']}/artifacts")
        self.assertEqual(artifact_response.status_code, 200)
        artifact_payload = artifact_response.json()
        self.assertIsNone(artifact_payload["structured"]["factor_snapshot"])
        self.assertIsNone(artifact_payload["structured"]["evidence_snapshot"])
        self.assertIsNone(artifact_payload["structured"]["structured_decision"])

    def test_legacy_scan_detail_backfills_missing_defaults(self) -> None:
        payload = self._create_scan("strict")
        scan_id = payload["scan_id"]
        scan_store = self.app.state.scan_store
        updated = scan_store.update_scan(
            scan_id,
            status="succeeded",
            summary_json={"provider_route": {"spot": "legacy-feed"}},
        )
        self.assertIsNotNone(updated)

        response = self.client.get(f"/api/overnight/scans/{scan_id}")
        self.assertEqual(response.status_code, 200)
        detail = response.json()
        self.assertEqual(detail["top_formal_tickers"], [])
        self.assertEqual(detail["bias_flags"], [])
        self.assertEqual(detail["preliminary_candidates"], [])
        self.assertEqual(detail["data_quality"]["status"], "unknown")

        persisted = scan_store.get_scan(scan_id)
        self.assertIsNotNone(persisted)
        persisted_summary = persisted["summary_json"]
        self.assertIn("top_formal_tickers", persisted_summary)
        self.assertIn("data_quality", persisted_summary)

    def test_legacy_review_detail_backfills_missing_breakdowns(self) -> None:
        payload = self._create_review("2025-03-20")
        review_store = self.app.state.review_store
        updated = review_store.update_review(
            payload["review_id"],
            status="succeeded",
            summary_json={"candidate_count": 3},
        )
        self.assertIsNotNone(updated)

        response = self.client.get(f"/api/overnight/reviews/{payload['review_id']}")
        self.assertEqual(response.status_code, 200)
        detail = response.json()
        self.assertEqual(detail["regime_breakdown"], [])
        self.assertEqual(detail["pool_breakdown"], [])
        self.assertEqual(detail["tail_quality_breakdown"], [])
        self.assertEqual(detail["data_quality"]["status"], "unknown")

        persisted = review_store.get_review(payload["review_id"])
        self.assertIsNotNone(persisted)
        persisted_summary = persisted["summary_json"]
        self.assertIn("regime_breakdown", persisted_summary)
        self.assertIn("pool_breakdown", persisted_summary)
        self.assertIn("tail_quality_breakdown", persisted_summary)

    def test_repair_dashboard_data_is_idempotent_for_legacy_records(self) -> None:
        task_payload = self._create_task("000001")
        task_store = self.app.state.task_store
        task = task_store.get_task(task_payload["task_id"])
        self.assertIsNotNone(task)
        config_snapshot = dict(task["config_snapshot"])
        config_snapshot["source_context"] = {
            "type": "manual",
            "ticker": "000001.SZ",
            "name": "????",
        }
        task_store.update_task(
            task_payload["task_id"],
            config_snapshot=config_snapshot,
            structured_summary=None,
        )

        scan_payload = self._create_scan("strict")
        scan_store = self.app.state.scan_store
        scan_store.update_scan(
            scan_payload["scan_id"],
            status="succeeded",
            summary_json={"provider_route": {"spot": "legacy-feed"}},
        )

        review_payload = self._create_review("2025-03-20")
        review_store = self.app.state.review_store
        review_store.update_review(
            review_payload["review_id"],
            status="succeeded",
            summary_json={"candidate_count": 1},
        )

        first_report = repair_dashboard_data(self.tempdir.name)
        second_report = repair_dashboard_data(self.tempdir.name)

        self.assertEqual(first_report["unresolved"], {"tasks": 0, "scans": 0, "reviews": 0})
        self.assertEqual(second_report["unresolved"], {"tasks": 0, "scans": 0, "reviews": 0})

        repaired_task = task_store.get_task(task_payload["task_id"])
        self.assertIsNotNone(repaired_task)
        self.assertEqual(
            repaired_task["config_snapshot"]["source_context"]["name"],
            "000001.SZ",
        )

        repaired_scan = scan_store.get_scan(scan_payload["scan_id"])
        self.assertIsNotNone(repaired_scan)
        self.assertIn("data_quality", repaired_scan["summary_json"])
        self.assertIn("top_formal_tickers", repaired_scan["summary_json"])

        repaired_review = review_store.get_review(review_payload["review_id"])
        self.assertIsNotNone(repaired_review)
        self.assertIn("regime_breakdown", repaired_review["summary_json"])
        self.assertIn("pool_breakdown", repaired_review["summary_json"])
        self.assertIn("tail_quality_breakdown", repaired_review["summary_json"])

    def test_create_overnight_scan_persists_and_launches_runner(self) -> None:
        payload = self._create_scan("strict")
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["market_region"], "cn_a")
        self.assertEqual(payload["mode"], "strict")
        self.assertEqual(len(self.launcher.launched_scans), 1)

        detail_response = self.client.get(f"/api/overnight/scans/{payload['scan_id']}")
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()["scan_id"], payload["scan_id"])

    def test_create_overnight_scan_normalizes_legacy_mode_alias(self) -> None:
        payload = self._create_scan("research_fallback")
        self.assertEqual(payload["mode"], "intraday_preview")
        self.assertEqual(len(self.launcher.launched_scans), 1)

        stored = self.app.state.scan_store.get_scan(payload["scan_id"])
        self.assertIsNotNone(stored)
        self.assertEqual(stored["mode"], "intraday_preview")

    def test_legacy_scan_mode_is_normalized_and_persisted_on_detail_read(self) -> None:
        payload = self._create_scan("strict")
        scan_id = payload["scan_id"]
        scan_store = self.app.state.scan_store
        updated = scan_store.update_scan(
            scan_id,
            mode="research_fallback",
            status="succeeded",
            summary_json={"mode": "research_fallback"},
        )
        self.assertIsNotNone(updated)

        response = self.client.get(f"/api/overnight/scans/{scan_id}")
        self.assertEqual(response.status_code, 200)
        detail = response.json()
        self.assertEqual(detail["mode"], "intraday_preview")
        self.assertEqual(detail["summary_snapshot"]["mode"], "intraday_preview")

        persisted = scan_store.get_scan(scan_id)
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted["mode"], "intraday_preview")
        self.assertEqual(persisted["summary_json"]["mode"], "intraday_preview")

    def test_scan_events_log_download_uses_utf8_charset(self) -> None:
        payload = self._create_scan("strict")
        artifact_dir = Path(payload["artifact_dir"])
        (artifact_dir / "events.log").write_text(
            "2025-03-20 15:00:00 [SCAN] 正在加载 A 股动态股票池。\n",
            encoding="utf-8",
        )

        response = self.client.get(f"/api/overnight/scans/{payload['scan_id']}/download/events.log")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["content-type"])
        self.assertIn("charset=utf-8", response.headers["content-type"].lower())
        self.assertIn("正在加载 A 股动态股票池。", response.text)

    def test_overnight_scan_detail_prefers_persisted_candidates(self) -> None:
        payload = self._create_scan("strict")
        scan_id = payload["scan_id"]
        artifact_dir = Path(payload["artifact_dir"])
        scan_store = OvernightScanStore(self.app.state.settings.db_path)
        scan_store.initialize()
        scan_store.update_scan(
            scan_id,
            status="succeeded",
            market_message="市场偏强",
            formal_count=1,
            watchlist_count=1,
            summary_json={
                "trade_date": "2025-03-20",
                "top_formal_tickers": ["600519.SS"],
                "validation_status": "pending",
                "data_quality": {"status": "ok", "message": "strict live route"},
                "provider_route": {"spot": "akshare_spot"},
                "bias_flags": ["survivorship_bias"],
                "universe_snapshot_date": "2025-03-20",
            },
        )
        (artifact_dir / "recommendations.json").write_text(
            json.dumps(
                {
                    "preliminary_candidates": [
                        {
                            "ticker": "600519.SS",
                            "name": "贵州茅台",
                            "pool": "主板",
                            "quality": "real",
                            "latest": 1620.0,
                            "pct": 1.8,
                            "amount": 2450000000.0,
                            "turnover": 1.2,
                            "quick_score": 78.5,
                            "total_score": 78.5,
                            "factor_breakdown": {"quick_score": 78.5},
                            "tail_metrics": None,
                            "filter_reason": None,
                            "excluded_from_final": "已通过初步筛选，等待后续深度筛分。",
                        },
                        {
                            "ticker": "300750.SZ",
                            "name": "宁德时代",
                            "pool": "创业板",
                            "quality": "proxy",
                            "latest": 210.0,
                            "pct": 0.9,
                            "amount": 860000000.0,
                            "turnover": 2.8,
                            "quick_score": 68.1,
                            "total_score": 68.1,
                            "factor_breakdown": {"quick_score": 68.1},
                            "tail_metrics": None,
                            "filter_reason": None,
                            "excluded_from_final": "已通过初步筛选，等待后续深度筛分。",
                        },
                        {
                            "ticker": "300750.SZ",
                            "name": "宁德时代",
                            "pool": "创业板",
                            "quality": "proxy",
                            "latest": 210.0,
                            "pct": 0.9,
                            "amount": 860000000.0,
                            "turnover": 2.8,
                            "quick_score": 68.1,
                            "total_score": 68.1,
                            "factor_breakdown": {"quick_score": 68.1},
                            "tail_metrics": None,
                            "filter_reason": None,
                            "excluded_from_final": "已通过初步筛选，等待后续深度筛分。",
                        }
                    ],
                    "total_score_candidates": [
                        {
                            "ticker": "600519.SS",
                            "name": "贵州茅台",
                            "pool": "主板",
                            "quality": "real",
                            "latest": 1620.0,
                            "pct": 1.8,
                            "amount": 2450000000.0,
                            "turnover": 1.2,
                            "quick_score": 78.5,
                            "total_score": 83.2,
                            "selection_stage": "formal",
                            "factor_breakdown": {"trend_strength": 22.0},
                            "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        },
                        {
                            "ticker": "300750.SZ",
                            "name": "宁德时代",
                            "pool": "创业板",
                            "quality": "proxy",
                            "latest": 210.0,
                            "pct": 0.9,
                            "amount": 860000000.0,
                            "turnover": 2.8,
                            "quick_score": 68.1,
                            "total_score": 64.3,
                            "selection_stage": "watchlist",
                            "factor_breakdown": {"trend_strength": 18.0},
                            "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        }
                    ],
                    "formal_recommendations": [],
                    "watchlist": [],
                    "rejected_candidates": [
                        {
                            "ticker": "000001.SZ",
                            "name": "平安银行",
                            "pool": "主板",
                            "quality": "missing",
                            "latest": 12.3,
                            "pct": 0.4,
                            "amount": 1200000000.0,
                            "turnover": 1.8,
                            "quick_score": 61.0,
                            "total_score": 58.4,
                            "selection_stage": "rejected",
                            "rejected_reason": "below_watchlist_threshold",
                            "factor_breakdown": {"trend_strength": 12.0},
                            "tail_metrics": {"quality": "missing"},
                            "filter_reason": None,
                            "excluded_from_final": "总分低于观察名单阈值。",
                        }
                    ],
                    "excluded_examples": [],
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        self._persist_scan_candidates(scan_id)

        response = self.client.get(f"/api/overnight/scans/{scan_id}")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["preliminary_candidates"]), 3)
        self.assertEqual(payload["preliminary_candidates"][0]["ticker"], "600519.SS")
        self.assertEqual(payload["formal_recommendations"][0]["ticker"], "600519.SS")
        self.assertEqual(payload["formal_recommendations"][0]["validation_status"], "pending")
        self.assertEqual(payload["watchlist"][0]["ticker"], "300750.SZ")
        self.assertEqual(len(payload["total_score_candidates"]), 2)
        self.assertEqual(payload["total_score_candidates"][0]["selection_stage"], "formal")
        self.assertEqual(len(payload["rejected_candidates"]), 1)
        self.assertEqual(
            payload["rejected_candidates"][0]["rejected_reason"],
            "below_watchlist_threshold",
        )
        self.assertEqual(payload["data_quality"]["status"], "ok")
        self.assertEqual(payload["provider_route"]["spot"], "akshare_spot")
        self.assertEqual(payload["universe_snapshot_date"], "2025-03-20")

    def test_create_task_is_idempotent_for_overnight_candidate(self) -> None:
        scan = self._create_scan("strict")
        scan_id = scan["scan_id"]
        scan_store = OvernightScanStore(self.app.state.settings.db_path)
        scan_store.initialize()
        scan_store.update_scan(
            scan_id,
            status="succeeded",
            market_message="市场偏强",
            formal_count=1,
            watchlist_count=1,
        )
        candidate_store = self._persist_scan_candidates(scan_id)

        first = self._create_task(
            "600519",
            source_query=(
                f"?source_type=overnight_scan&source_scan_id={scan_id}"
                "&source_trade_date=2025-03-20&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            ),
        )
        second = self._create_task(
            "600519",
            source_query=(
                f"?source_type=overnight_scan&source_scan_id={scan_id}"
                "&source_trade_date=2025-03-20&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            ),
        )

        self.assertEqual(first["task_id"], second["task_id"])
        self.assertEqual(len(self.launcher.launched_tasks), 1)
        candidate = candidate_store.get_candidate(scan_id, "600519.SS")
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["linked_task_id"], first["task_id"])

    def test_validate_scan_updates_candidates_and_summary(self) -> None:
        scan = self._create_scan("strict")
        scan_id = scan["scan_id"]
        scan_store = OvernightScanStore(self.app.state.settings.db_path)
        scan_store.initialize()
        scan_store.update_scan(
            scan_id,
            status="succeeded",
            market_message="市场偏强",
            formal_count=1,
            watchlist_count=1,
            summary_json={"trade_date": "2025-03-20", "top_formal_tickers": ["600519.SS"]},
        )
        candidate_store = self._persist_scan_candidates(scan_id)

        with patch(
            "dashboard_api.app.validate_scan_candidates",
            return_value={
                "candidates": [
                    {
                        **candidate_store.get_candidate(scan_id, "600519.SS"),
                        "validation_status": "validated",
                        "next_open_return": 1.23,
                        "next_open_date": "2025-03-21",
                        "scan_close_price": 1620.0,
                    },
                    {
                        **candidate_store.get_candidate(scan_id, "300750.SZ"),
                        "validation_status": "watchlist_only",
                        "next_open_return": None,
                        "next_open_date": None,
                        "scan_close_price": 210.0,
                    },
                ],
                "summary": {
                    "validated_formal_count": 1,
                    "avg_next_open_return": 1.23,
                    "best_candidate": {
                        "ticker": "600519.SS",
                        "name": "贵州茅台",
                        "next_open_return": 1.23,
                        "next_open_date": "2025-03-21",
                    },
                    "worst_candidate": {
                        "ticker": "600519.SS",
                        "name": "贵州茅台",
                        "next_open_return": 1.23,
                        "next_open_date": "2025-03-21",
                    },
                    "validation_status": "validated",
                    "validation_audit": {"validated_formal_count": 1},
                },
            },
        ):
            response = self.client.post(f"/api/overnight/scans/{scan_id}/validate")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["validation_status"], "validated")
        self.assertEqual(payload["validated_formal_count"], 1)
        self.assertEqual(payload["formal_recommendations"][0]["next_open_return"], 1.23)

    def test_overnight_scan_artifacts_endpoint_reads_saved_files(self) -> None:
        payload = self._create_scan("research_fallback")
        self.assertEqual(payload["mode"], "intraday_preview")
        scan_id = payload["scan_id"]
        artifact_dir = Path(payload["artifact_dir"])
        store = OvernightScanStore(self.app.state.settings.db_path)
        store.initialize()
        store.update_scan(
            scan_id,
            status="succeeded",
            progress_message="Overnight scan completed successfully.",
            market_message="市场偏强，允许扩大推荐池。",
            formal_count=1,
            watchlist_count=1,
            summary_json={
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "mode": "research_fallback",
                "market_ok": True,
                "market_message": "市场偏强，允许扩大推荐池。",
                "benchmark_pct": 1.23,
                "formal_count": 1,
                "watchlist_count": 1,
            },
        )
        scan_payload = store.get_scan(scan_id)
        self.assertIsNotNone(scan_payload)
        (artifact_dir / "scan.json").write_text(
            json.dumps(scan_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        (artifact_dir / "recommendations.json").write_text(
            json.dumps(
                {
                    "preliminary_candidates": [
                        {
                            "ticker": "600519.SS",
                            "name": "贵州茅台",
                            "pool": "主板",
                            "quality": "real",
                            "latest": 1620.0,
                            "pct": 1.8,
                            "amount": 2450000000.0,
                            "turnover": 1.2,
                            "quick_score": 78.5,
                            "total_score": 78.5,
                            "factor_breakdown": {"quick_score": 78.5},
                            "tail_metrics": None,
                            "filter_reason": None,
                            "excluded_from_final": "已通过初步筛选，等待后续深度筛分。",
                        }
                    ],
                    "formal_recommendations": [
                        {
                            "ticker": "600519.SS",
                            "name": "贵州茅台",
                            "pool": "主板",
                            "quality": "real",
                            "latest": 1620.0,
                            "pct": 1.8,
                            "amount": 2450000000.0,
                            "turnover": 1.2,
                            "quick_score": 78.5,
                            "total_score": 83.2,
                            "factor_breakdown": {"trend_strength": 22.0},
                            "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        }
                    ],
                    "total_score_candidates": [
                        {
                            "ticker": "600519.SS",
                            "name": "贵州茅台",
                            "pool": "主板",
                            "quality": "real",
                            "latest": 1620.0,
                            "pct": 1.8,
                            "amount": 2450000000.0,
                            "turnover": 1.2,
                            "quick_score": 78.5,
                            "total_score": 83.2,
                            "selection_stage": "formal",
                            "factor_breakdown": {"trend_strength": 22.0},
                            "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        },
                        {
                            "ticker": "300750.SZ",
                            "name": "宁德时代",
                            "pool": "创业板",
                            "quality": "proxy",
                            "latest": 210.0,
                            "pct": 0.9,
                            "amount": 860000000.0,
                            "turnover": 2.8,
                            "quick_score": 68.1,
                            "total_score": 64.3,
                            "selection_stage": "watchlist",
                            "factor_breakdown": {"trend_strength": 18.0},
                            "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        }
                    ],
                    "watchlist": [
                        {
                            "ticker": "300750.SZ",
                            "name": "宁德时代",
                            "pool": "创业板",
                            "quality": "proxy",
                            "latest": 210.0,
                            "pct": 0.9,
                            "amount": 860000000.0,
                            "turnover": 2.8,
                            "quick_score": 68.1,
                            "total_score": 64.3,
                            "factor_breakdown": {"trend_strength": 18.0},
                            "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                            "filter_reason": None,
                            "excluded_from_final": None,
                        }
                    ],
                    "rejected_candidates": [
                        {
                            "ticker": "000001.SZ",
                            "name": "平安银行",
                            "pool": "主板",
                            "quality": "missing",
                            "latest": 12.3,
                            "pct": 0.4,
                            "amount": 1200000000.0,
                            "turnover": 1.8,
                            "quick_score": 61.0,
                            "total_score": 58.4,
                            "selection_stage": "rejected",
                            "rejected_reason": "below_watchlist_threshold",
                            "factor_breakdown": {"trend_strength": 12.0},
                            "tail_metrics": {"quality": "missing"},
                            "filter_reason": None,
                            "excluded_from_final": "总分低于观察名单阈值。",
                        }
                    ],
                    "excluded_examples": [],
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "audit.json").write_text(
            json.dumps(
                {
                    "quality_counts": {"real": 2, "proxy": 1, "missing": 0},
                    "history_loaded": 5,
                    "formal_threshold": 75.0,
                    "watchlist_threshold": 60.0,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        response = self.client.get(f"/api/overnight/scans/{scan_id}/artifacts")
        self.assertEqual(response.status_code, 200)
        artifact_payload = response.json()
        self.assertEqual(artifact_payload["summary"]["formal_count"], 1)
        self.assertEqual(artifact_payload["summary"]["mode"], "intraday_preview")
        self.assertEqual(len(artifact_payload["preliminary_candidates"]), 1)
        self.assertEqual(
            artifact_payload["preliminary_candidates"][0]["ticker"],
            "600519.SS",
        )
        self.assertEqual(len(artifact_payload["formal_recommendations"]), 1)
        self.assertEqual(
            artifact_payload["formal_recommendations"][0]["quality"],
            "real",
        )
        self.assertEqual(len(artifact_payload["total_score_candidates"]), 2)
        self.assertEqual(
            artifact_payload["total_score_candidates"][1]["selection_stage"],
            "watchlist",
        )
        self.assertEqual(len(artifact_payload["watchlist"]), 1)
        self.assertEqual(len(artifact_payload["rejected_candidates"]), 1)
        self.assertIn("audit_json", artifact_payload["downloads"])

    def test_create_overnight_review_persists_and_launches_runner(self) -> None:
        payload = self._create_review()
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["market_region"], "cn_a")
        self.assertEqual(payload["window_days"], 60)
        self.assertEqual(payload["mode"], "strict")
        self.assertEqual(payload["return_basis"], "buy_1455_sell_next_day_1000")
        self.assertIn("data_quality", payload)
        self.assertIn("provider_route", payload)
        self.assertEqual(len(self.launcher.launched_reviews), 1)

        detail_response = self.client.get(f"/api/overnight/reviews/{payload['review_id']}")
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()["review_id"], payload["review_id"])

    def test_overnight_review_artifacts_endpoint_reads_saved_files(self) -> None:
        payload = self._create_review("2025-03-31")
        review_id = payload["review_id"]
        artifact_dir = Path(payload["artifact_dir"])

        review_summary = {
            "end_trade_date": "2025-03-31",
            "market_region": "cn_a",
            "window_days": 60,
            "mode": "strict",
            "return_basis": "next_open",
            "candidate_count": 8,
            "days_evaluated": 60,
            "days_with_formal_picks": 11,
            "avg_next_open_return": 0.83,
            "median_next_open_return": 0.42,
            "positive_pick_rate": 0.625,
            "avg_daily_equal_weight_return": 0.57,
            "avg_benchmark_next_open_return": 0.11,
            "avg_excess_return": 0.46,
            "has_valid_samples": True,
            "best_day": {
                "trade_date": "2025-03-18",
                "equal_weight_next_open_return": 2.31,
                "benchmark_next_open_return": 0.45,
                "avg_excess_return": 1.86,
                "formal_tickers": ["600519.SS", "300750.SZ"],
            },
            "worst_day": {
                "trade_date": "2025-03-07",
                "equal_weight_next_open_return": -1.42,
                "benchmark_next_open_return": -0.35,
                "avg_excess_return": -1.07,
                "formal_tickers": ["000001.SZ"],
            },
            "audit": {
                "notes": [
                    "historical_review_uses_current_live_universe",
                    "Survivorship bias exists because the replay universe is still seeded from the currently available active A-share list.",
                ],
                "missing_next_open_count": 2,
            },
        }

        review_record = {
            **payload,
            "status": "succeeded",
            "progress_message": "Overnight review completed successfully.",
            "summary_json": review_summary,
        }
        (artifact_dir / "review.json").write_text(
            json.dumps(review_record, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        (artifact_dir / "daily_results.json").write_text(
            json.dumps(
                [
                    {
                        "trade_date": "2025-03-18",
                        "formal_count": 2,
                        "watchlist_count": 1,
                        "formal_tickers": ["600519.SS", "300750.SZ"],
                        "market_message": "市场正常",
                        "benchmark_next_open_return": 0.45,
                        "equal_weight_next_open_return": 2.31,
                        "avg_excess_return": 1.86,
                    }
                ],
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (artifact_dir / "candidate_results.json").write_text(
            json.dumps(
                [
                    {
                        "trade_date": "2025-03-18",
                        "category": "formal",
                        "ticker": "600519.SS",
                        "name": "贵州茅台",
                        "quality": "real",
                        "quick_score": 72.1,
                        "total_score": 81.6,
                        "factor_breakdown": {"trend_strength": 22.0, "tail_strength": 18.5},
                        "next_trade_date": "2025-03-19",
                        "scan_close_price": 1635.2,
                        "next_open_return": 1.34,
                        "counted_in_performance": True,
                    }
                ],
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        response = self.client.get(f"/api/overnight/reviews/{review_id}/artifacts")
        self.assertEqual(response.status_code, 200)
        artifact_payload = response.json()
        self.assertEqual(artifact_payload["summary"]["return_basis"], "next_open")
        self.assertEqual(artifact_payload["summary"]["trade_count"], 8)
        self.assertEqual(artifact_payload["summary"]["days_with_trade"], 11)
        self.assertEqual(artifact_payload["summary"]["avg_strategy_return"], 0.83)
        self.assertEqual(artifact_payload["summary"]["avg_benchmark_return"], 0.11)
        self.assertEqual(artifact_payload["summary"]["days_with_formal_picks"], 11)
        self.assertEqual(len(artifact_payload["daily_results"]), 1)
        self.assertEqual(len(artifact_payload["candidate_results"]), 1)
        self.assertIn("candidate_results_json", artifact_payload["downloads"])

    def test_task_detail_exposes_extended_overnight_context(self) -> None:
        scan = self._create_scan("strict")
        scan_id = scan["scan_id"]
        scan_store = OvernightScanStore(self.app.state.settings.db_path)
        scan_store.initialize()
        scan_store.update_scan(
            scan_id,
            status="succeeded",
            market_message="市场偏强",
            formal_count=1,
            watchlist_count=1,
            summary_json={
                "trade_date": "2025-03-20",
                "provider_route": {"tail": "frozen-minute", "spot": "frozen-spot"},
                "evaluation_config_version": "overnight_phase2_v1",
                "evaluation_config_hash": "abc123def456",
            },
        )
        self._persist_scan_candidates(scan_id)

        payload = self._create_task(
            "600519",
            source_query=(
                f"?source_type=overnight_scan&source_scan_id={scan_id}"
                "&source_trade_date=2025-03-20&source_mode=strict"
                "&source_name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0"
            ),
        )

        self.assertEqual(payload["source_context"]["type"], "overnight_scan")
        self.assertEqual(payload["overnight_context"]["scan_id"], scan_id)
        self.assertEqual(payload["overnight_context"]["bucket"], "formal")
        self.assertEqual(payload["overnight_context"]["provider_route"]["tail"], "frozen-minute")
        self.assertEqual(payload["overnight_context"]["evaluation_config_hash"], "abc123def456")
        self.assertIn("factor_breakdown", payload["overnight_context"])

    def test_review_detail_returns_breakdowns_and_evaluation_hash(self) -> None:
        payload = self._create_review("2025-03-31")
        review_store = self.app.state.review_store
        updated = review_store.update_review(
            payload["review_id"],
            status="succeeded",
            summary_json={
                "end_trade_date": "2025-03-31",
                "market_region": "cn_a",
                "window_days": 60,
                "mode": "strict",
                "return_basis": "next_open",
                "candidate_count": 6,
                "days_evaluated": 60,
                "days_with_formal_picks": 12,
                "avg_next_open_return": 0.51,
                "avg_excess_return": 0.32,
                "positive_pick_rate": 0.58,
                "has_valid_samples": True,
                "evaluation_config_version": "overnight_phase2_v1",
                "evaluation_config_hash": "hash-phase2",
                "regime_breakdown": [
                    {
                        "group": "normal",
                        "days_with_formal_picks": 8,
                        "candidate_count": 12,
                        "avg_next_open_return": 0.62,
                        "avg_excess_return": 0.41,
                        "positive_pick_rate": 0.66,
                    }
                ],
                "pool_breakdown": [
                    {
                        "group": "main",
                        "days_with_formal_picks": 10,
                        "candidate_count": 14,
                        "avg_next_open_return": 0.48,
                        "avg_excess_return": 0.27,
                        "positive_pick_rate": 0.57,
                    }
                ],
                "tail_quality_breakdown": [
                    {
                        "group": "real",
                        "days_with_formal_picks": 12,
                        "candidate_count": 15,
                        "avg_next_open_return": 0.51,
                        "avg_excess_return": 0.32,
                        "positive_pick_rate": 0.58,
                    }
                ],
            },
        )
        self.assertIsNotNone(updated)

        response = self.client.get(f"/api/overnight/reviews/{payload['review_id']}")
        self.assertEqual(response.status_code, 200)
        detail = response.json()
        self.assertEqual(detail["return_basis"], "next_open")
        self.assertEqual(detail["summary_snapshot"]["trade_count"], 6)
        self.assertEqual(detail["summary_snapshot"]["days_with_trade"], 12)
        self.assertEqual(detail["summary_snapshot"]["avg_strategy_return"], 0.51)
        self.assertEqual(detail["evaluation_config_hash"], "hash-phase2")
        self.assertEqual(detail["regime_breakdown"][0]["group"], "normal")
        self.assertEqual(detail["pool_breakdown"][0]["group"], "main")
        self.assertEqual(detail["tail_quality_breakdown"][0]["group"], "real")

    def test_dashboard_shell_disables_html_caching(self) -> None:
        if not self.app.state.settings.frontend_dist.exists():
            self.skipTest("dashboard-ui/dist is not available")

        for path in ("/", "/analyze", "/index.html"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.headers.get("cache-control"),
                "no-cache, no-store, must-revalidate",
            )


if __name__ == "__main__":
    unittest.main()
