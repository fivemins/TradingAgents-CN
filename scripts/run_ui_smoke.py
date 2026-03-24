from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard_api.settings import get_settings
from dashboard_api.store import OvernightCandidateStore, OvernightReviewStore, OvernightScanStore, TaskStore

BASE_URL = "http://127.0.0.1:8123"
HTTP_OPENER = build_opener(ProxyHandler({}))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def seed_dashboard_data(data_dir: Path) -> tuple[str, str]:
    settings = get_settings(data_dir)
    settings.tasks_dir.mkdir(parents=True, exist_ok=True)
    settings.overnight_scans_dir.mkdir(parents=True, exist_ok=True)
    settings.overnight_reviews_dir.mkdir(parents=True, exist_ok=True)

    task_store = TaskStore(settings.db_path)
    scan_store = OvernightScanStore(settings.db_path)
    review_store = OvernightReviewStore(settings.db_path)
    candidate_store = OvernightCandidateStore(settings.db_path)
    task_store.initialize()
    scan_store.initialize()
    review_store.initialize()
    candidate_store.initialize()

    finished_task_id = "task-finished-001"
    running_task_id = "task-running-001"
    scan_id = "scan-smoke-001"
    review_id = "review-smoke-001"

    common_payload = {
        "analysis_date": "2025-03-20",
        "market_region": "cn_a",
        "analysts": ["market", "social", "news", "fundamentals"],
        "research_depth": 1,
        "llm_provider": "ark",
        "quick_think_llm": "doubao-seed-2.0-lite",
        "deep_think_llm": "doubao-seed-2.0-pro",
        "online_tools": True,
    }

    finished_dir = settings.tasks_dir / finished_task_id
    running_dir = settings.tasks_dir / running_task_id
    for artifact_dir in (finished_dir, running_dir):
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "reports").mkdir(parents=True, exist_ok=True)

    finished_snapshot = {
        "market_region": "cn_a",
        "symbol": "600519.SS",
        "display_symbol": "600519.SS",
        "trade_date": "2025-03-20",
        "composite_score": 61.0,
        "confidence": 0.77,
        "recommended_action": "HOLD",
        "scores": {
            "technical": {
                "score": 64.0,
                "confidence": 0.78,
                "summary": "趋势站上中期均线。",
            },
            "overnight_tail": {
                "score": 58.0,
                "confidence": 0.69,
                "summary": "尾盘资金承接温和偏强。",
            },
        },
    }
    finished_evidence = {
        "technical": {
            "strengths": [
                {
                    "signal": "站上 SMA20 / SMA60",
                    "value": "趋势偏强",
                    "impact": "positive",
                    "source": "price_history",
                    "weight": 0.2,
                }
            ],
            "risks": [],
            "raw_metrics": [],
        },
        "overnight_tail": {
            "strengths": [
                {
                    "signal": "尾盘强度",
                    "value": 0.82,
                    "impact": "positive",
                    "source": "overnight_scan",
                    "weight": 0.15,
                }
            ],
            "risks": [],
            "raw_metrics": [],
        },
    }
    finished_decision = {
        "decision": "HOLD",
        "score": 61.0,
        "confidence": 0.77,
        "summary": "技术面和尾盘来源偏正面，但分数未达到 BUY 阈值。",
        "source": "structured_snapshot",
        "primary_drivers": ["趋势站上均线", "尾盘资金承接稳定"],
        "risk_flags": ["尚未突破 BUY 阈值"],
    }

    write_json(finished_dir / "factor_snapshot.json", finished_snapshot)
    write_json(finished_dir / "evidence_snapshot.json", finished_evidence)
    write_json(finished_dir / "structured_decision.json", finished_decision)
    (finished_dir / "reports" / "market.md").write_text("# 市场分析\n\n趋势仍偏强。", encoding="utf-8")
    (finished_dir / "reports" / "sentiment.md").write_text("# 情绪分析\n\n市场情绪中性。", encoding="utf-8")
    (finished_dir / "reports" / "news.md").write_text("# 新闻分析\n\n暂无重大负面新闻。", encoding="utf-8")
    (finished_dir / "reports" / "fundamentals.md").write_text("# 基本面分析\n\n盈利能力稳定。", encoding="utf-8")
    (finished_dir / "reports" / "investment_plan.md").write_text("# 研究总结\n\n维持观察。", encoding="utf-8")
    (finished_dir / "reports" / "trader_plan.md").write_text("# 交易计划\n\n等待更强确认。", encoding="utf-8")
    (finished_dir / "final_decision.md").write_text(
        "# 最终结论\n\n- 结论摘要：维持观望。\n- 最终动作：HOLD\n",
        encoding="utf-8",
    )

    running_reports = {
        "market.md": "# 市场分析\n\n正在生成中...",
    }
    for filename, content in running_reports.items():
        (running_dir / "reports" / filename).write_text(content, encoding="utf-8")

    finished_task = task_store.create_task(
        finished_task_id,
        {**common_payload, "ticker": "600519.SS"},
        finished_dir,
        {
            "source_context": {
                "type": "overnight_scan",
                "scan_id": scan_id,
                "trade_date": "2025-03-20",
                "mode": "strict",
                "ticker": "600519.SS",
                "name": "????",
            },
            "overnight_context": {
                "source_name": "????",
                "bucket": "formal",
                "quality": "real",
                "quick_score": 78.5,
                "total_score": 82.1,
                "factor_breakdown": {"tail_strength": 18.0},
                "tail_metrics": {"quality": "real", "tail_return_pct": 0.82},
                "provider_route": {"spot": "akshare_spot"},
                "evaluation_config_hash": "smokehash001",
            },
        },
    )
    task_store.update_task(
        finished_task_id,
        status="succeeded",
        stage="completed",
        progress_message="Task completed successfully.",
        decision="HOLD",
        structured_summary={
            "composite_score": 61.0,
            "confidence": 0.77,
            "recommended_action": "HOLD",
            "primary_driver": "趋势站上中期均线",
            "primary_risk": "未达到 BUY 阈值",
        },
        started_at=finished_task["created_at"],
        finished_at=finished_task["created_at"],
        pid=None,
    )

    running_task = task_store.create_task(
        running_task_id,
        {**common_payload, "ticker": "300750.SZ"},
        running_dir,
        {"source_context": None, "overnight_context": None},
    )
    task_store.update_task(
        running_task_id,
        status="running",
        stage="market",
        progress_message="Collecting market data.",
        started_at=running_task["created_at"],
        pid=99999,
    )

    scan_dir = settings.overnight_scans_dir / scan_id
    scan_dir.mkdir(parents=True, exist_ok=True)
    scan = scan_store.create_scan(
        scan_id,
        {"trade_date": "2025-03-20", "market_region": "cn_a", "mode": "strict"},
        scan_dir,
    )
    scan_summary = {
        "market_message": "市场正常，可执行标准阈值。",
        "validated_formal_count": 1,
        "avg_next_open_return": 0.23,
        "validation_status": "validated",
        "data_quality": {"status": "ok", "message": "Akshare spot/minute ready."},
        "provider_route": {"spot": "akshare_spot", "minute": "akshare_minute_legacy"},
        "bias_flags": [],
        "universe_snapshot_date": "2025-03-20",
        "evaluation_config_version": "overnight_phase2_v1",
        "evaluation_config_hash": "smokehash001",
    }
    scan_store.update_scan(
        scan_id,
        status="succeeded",
        progress_message="Overnight scan completed successfully.",
        market_message="市场正常，可执行标准阈值。",
        formal_count=1,
        watchlist_count=1,
        summary_json=scan_summary,
        started_at=scan["created_at"],
        finished_at=scan["created_at"],
        pid=None,
    )
    candidate_store.replace_scan_candidates(
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
                "total_score": 82.1,
                "factor_breakdown": {"trend_strength": 22.0, "tail_strength": 18.0},
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
                "factor_breakdown": {"trend_strength": 18.0, "tail_strength": 11.0},
                "tail_metrics": {"quality": "proxy", "tail_return_pct": 0.32},
                "filter_reason": None,
                "excluded_from_final": None,
            }
        ],
    )
    candidate_store.link_task(scan_id, "600519.SS", finished_task_id)
    candidate_store.update_candidate(
        scan_id,
        "600519.SS",
        validation_status="validated",
        next_open_return=0.23,
        next_open_date="2025-03-21",
        scan_close_price=1620.0,
    )
    write_json(scan_dir / "scan.json", {"summary_json": scan_summary})
    write_json(
        scan_dir / "recommendations.json",
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
                    "total_score": 82.1,
                    "factor_breakdown": {"trend_strength": 22.0, "tail_strength": 18.0},
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
                    "factor_breakdown": {"trend_strength": 18.0, "tail_strength": 11.0},
                },
            ],
            "formal_recommendations": [],
            "watchlist": [],
            "excluded_examples": [],
        },
    )
    write_json(scan_dir / "audit.json", {"notes": ["Smoke test payload."], "matched_events": 0})
    write_json(scan_dir / "scan_inputs.json", {"trade_date": "2025-03-20", "mode": "strict"})
    write_json(scan_dir / "data_sources.json", {"spot": "akshare_spot", "minute": "akshare_minute_legacy"})
    write_json(
        scan_dir / "evaluation_config.json",
        {"version": "overnight_phase2_v1", "hash": "smokehash001"},
    )

    review_dir = settings.overnight_reviews_dir / review_id
    review_dir.mkdir(parents=True, exist_ok=True)
    review = review_store.create_review(
        review_id,
        {
            "end_trade_date": "2025-03-20",
            "market_region": "cn_a",
            "window_days": 60,
            "mode": "strict",
            "return_basis": "next_open",
        },
        review_dir,
    )
    review_summary = {
        "end_trade_date": "2025-03-20",
        "market_region": "cn_a",
        "window_days": 60,
        "mode": "strict",
        "return_basis": "next_open",
        "candidate_count": 12,
        "days_evaluated": 60,
        "days_with_formal_picks": 24,
        "avg_next_open_return": 0.26,
        "median_next_open_return": 0.18,
        "positive_pick_rate": 0.58,
        "avg_daily_equal_weight_return": 0.24,
        "avg_benchmark_next_open_return": 0.12,
        "avg_excess_return": 0.12,
        "has_valid_samples": True,
        "headline_message": "严格模式下正式推荐仍有正向超额。",
        "data_quality": {"status": "incomplete", "message": "部分历史日期使用当前股票池快照。"},
        "provider_route": {"spot": "akshare_spot", "minute": "akshare_minute_legacy"},
        "bias_flags": ["survivorship_bias"],
        "universe_snapshot_date": "2025-03-20",
        "survivorship_bias": True,
        "evaluation_config_version": "overnight_phase2_v1",
        "evaluation_config_hash": "smokehash001",
        "regime_breakdown": [
            {
                "group": "正常市场",
                "days_with_formal_picks": 20,
                "candidate_count": 10,
                "avg_next_open_return": 0.31,
                "avg_excess_return": 0.17,
                "positive_pick_rate": 0.61,
            }
        ],
        "pool_breakdown": [
            {
                "group": "主板",
                "days_with_formal_picks": 18,
                "candidate_count": 8,
                "avg_next_open_return": 0.28,
                "avg_excess_return": 0.14,
                "positive_pick_rate": 0.57,
            }
        ],
        "tail_quality_breakdown": [
            {
                "group": "real",
                "days_with_formal_picks": 24,
                "candidate_count": 12,
                "avg_next_open_return": 0.26,
                "avg_excess_return": 0.12,
                "positive_pick_rate": 0.58,
            }
        ],
        "audit": {"notes": ["存在存活偏差，但已显式标记。"]},
    }
    review_store.update_review(
        review_id,
        status="succeeded",
        progress_message="Overnight review completed successfully.",
        summary_json=review_summary,
        started_at=review["created_at"],
        finished_at=review["created_at"],
        pid=None,
    )
    write_json(review_dir / "review.json", {"summary_json": review_summary})
    write_json(
        review_dir / "daily_results.json",
        [
            {
                "trade_date": "2025-03-19",
                "formal_count": 1,
                "watchlist_count": 1,
                "formal_tickers": ["600519.SS"],
                "market_message": "市场正常",
                "benchmark_next_open_return": 0.12,
                "equal_weight_next_open_return": 0.23,
                "avg_excess_return": 0.11,
            }
        ],
    )
    write_json(
        review_dir / "candidate_results.json",
        [
            {
                "trade_date": "2025-03-19",
                "ticker": "600519.SS",
                "category": "formal",
                "next_open_return": 0.23,
            }
        ],
    )
    write_json(review_dir / "review_inputs.json", {"end_trade_date": "2025-03-20"})
    write_json(review_dir / "data_sources.json", {"spot": "akshare_spot", "minute": "akshare_minute_legacy"})
    write_json(
        review_dir / "evaluation_config.json",
        {"version": "overnight_phase2_v1", "hash": "smokehash001"},
    )

    return finished_task_id, running_task_id


def wait_for_server(base_url: str, timeout_seconds: float = 45.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with HTTP_OPENER.open(f"{base_url}/api/health", timeout=2) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("Dashboard server did not become healthy in time.")


def main() -> None:
    frontend_index = PROJECT_ROOT / "dashboard-ui" / "dist" / "index.html"
    if not frontend_index.exists():
        raise SystemExit("dashboard-ui/dist is missing. Run `npm run build` first.")
    node_path = shutil.which("node")
    if not node_path:
        raise SystemExit("node is not available on PATH.")
    playwright_module = PROJECT_ROOT / "dashboard-ui" / "node_modules" / "playwright" / "index.mjs"
    if not playwright_module.exists():
        raise SystemExit("dashboard-ui/node_modules/playwright is missing. Run `npm install` in dashboard-ui first.")

    with tempfile.TemporaryDirectory(prefix="tradingagents-ui-smoke-") as tempdir:
        data_dir = Path(tempdir)
        finished_task_id, running_task_id = seed_dashboard_data(data_dir)
        env = os.environ.copy()
        env["TRADINGAGENTS_DASHBOARD_DATA_DIR"] = str(data_dir)
        env["NO_PROXY"] = "127.0.0.1,localhost"
        env["no_proxy"] = "127.0.0.1,localhost"
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "dashboard_api.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                "8123",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            wait_for_server(BASE_URL)
            subprocess.run(
                [
                    node_path,
                    "scripts/ui_smoke_check.mjs",
                    BASE_URL,
                    finished_task_id,
                    running_task_id,
                ],
                cwd=PROJECT_ROOT,
                check=True,
            )
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
            if server.stdout:
                smoke_log = PROJECT_ROOT / "output" / "playwright" / "ui-smoke-server.log"
                smoke_log.parent.mkdir(parents=True, exist_ok=True)
                smoke_log.write_text(server.stdout.read(), encoding="utf-8", errors="ignore")


if __name__ == "__main__":
    main()
