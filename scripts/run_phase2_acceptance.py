from __future__ import annotations

import argparse
import json
import os
import sys
import time
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard_api.app import create_app
from dashboard_api.overnight_review_runner import run_review
from dashboard_api.overnight_runner import run_scan
from dashboard_api.runner import run_task
from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.models import MarketRegime, TailMetrics


SAMPLE_CODES = ["600519", "300750", "000001"]
SCAN_DATE = "2025-03-20"
REVIEW_END_DATE = "2025-03-19"
REVIEW_TRADE_DATES = ["2025-03-17", "2025-03-18", "2025-03-19"]

COL_CODE = "\u4ee3\u7801"
COL_NAME = "\u540d\u79f0"
COL_LAST = "\u6700\u65b0\u4ef7"
COL_PCT = "\u6da8\u8dcc\u5e45"
COL_AMOUNT = "\u6210\u4ea4\u989d"
COL_TURNOVER = "\u6362\u624b\u7387"
COL_PREV = "\u6628\u6536"
COL_OPEN = "\u4eca\u5f00"
COL_HIGH = "\u6700\u9ad8"
COL_LOW = "\u6700\u4f4e"


class DummyLauncher:
    def launch(self, _: str) -> int:
        return 9999

    def launch_task(self, _: str) -> int:
        return 9999

    def launch_overnight_scan(self, _: str) -> int:
        return 9999

    def launch_overnight_review(self, _: str) -> int:
        return 9999


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def run_task_with_retry(task_id: str, attempts: int = 3, delay_seconds: float = 3.0) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            run_task(task_id)
            return
        except Exception as exc:  # pragma: no cover - used for live acceptance
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(delay_seconds * attempt)
    raise RuntimeError(f"task {task_id} failed after {attempts} attempts: {last_error}") from last_error


def make_spot_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            COL_CODE: SAMPLE_CODES,
            COL_NAME: ["Kweichow Moutai", "CATL", "Ping An Bank"],
            COL_LAST: [108.6, 84.2, 11.3],
            COL_PCT: [2.4, 1.8, 1.2],
            COL_AMOUNT: [1.8e9, 1.2e9, 9.5e8],
            COL_TURNOVER: [2.5, 3.2, 1.8],
            COL_PREV: [106.0, 82.0, 11.1],
            COL_OPEN: [106.4, 82.5, 11.15],
            COL_HIGH: [108.8, 84.5, 11.35],
            COL_LOW: [106.2, 82.4, 11.05],
        }
    )


def make_history_frame(code: str) -> pd.DataFrame:
    base = {"600519": 85.0, "300750": 62.0, "000001": 9.0}[code]
    dates = pd.bdate_range(end="2025-03-21", periods=80)
    rows: list[dict[str, Any]] = []
    for index, dt in enumerate(dates):
        close = base + index * 0.25
        rows.append(
            {
                "Date": dt.strftime("%Y-%m-%d"),
                "Open": round(close - 0.35, 4),
                "High": round(close + 0.6, 4),
                "Low": round(close - 0.7, 4),
                "Close": round(close, 4),
                "Volume": 1000 + index * 20,
                "Turnover": 9.0e8 + index * 2.5e7,
                "TurnoverRate": 1.5 + index * 0.02,
            }
        )
    frame = pd.DataFrame(rows)

    overrides: dict[str, dict[str, dict[str, float]]] = {
        "600519": {
            "2025-03-17": {"Open": 104.30, "High": 105.70, "Low": 103.80, "Close": 105.20},
            "2025-03-18": {"Open": 105.50, "High": 106.90, "Low": 105.10, "Close": 106.40},
            "2025-03-19": {"Open": 106.60, "High": 108.10, "Low": 106.20, "Close": 107.70},
            "2025-03-20": {"Open": 107.90, "High": 109.40, "Low": 107.50, "Close": 109.00},
            "2025-03-21": {"Open": 109.60, "High": 110.50, "Low": 109.10, "Close": 110.10},
        },
        "300750": {
            "2025-03-17": {"Open": 81.10, "High": 82.20, "Low": 80.90, "Close": 81.80},
            "2025-03-18": {"Open": 82.00, "High": 83.40, "Low": 81.70, "Close": 83.10},
            "2025-03-19": {"Open": 83.20, "High": 84.40, "Low": 82.80, "Close": 84.00},
            "2025-03-20": {"Open": 84.30, "High": 85.80, "Low": 84.00, "Close": 85.40},
            "2025-03-21": {"Open": 85.70, "High": 86.50, "Low": 85.30, "Close": 86.10},
        },
        "000001": {
            "2025-03-17": {"Open": 27.72, "High": 28.10, "Low": 27.60, "Close": 27.95},
            "2025-03-18": {"Open": 28.05, "High": 28.60, "Low": 27.95, "Close": 28.45},
            "2025-03-19": {"Open": 28.55, "High": 29.05, "Low": 28.40, "Close": 28.95},
            "2025-03-20": {"Open": 29.00, "High": 29.50, "Low": 28.85, "Close": 29.40},
            "2025-03-21": {"Open": 29.55, "High": 30.05, "Low": 29.30, "Close": 29.90},
        },
    }

    for date_str, payload in overrides[code].items():
        mask = frame["Date"] == date_str
        for column, value in payload.items():
            frame.loc[mask, column] = value

    return frame


def make_tail_metrics(ticker: str) -> TailMetrics:
    return {
        "600519.SS": TailMetrics(True, source="frozen-minute", tail_return_pct=0.95, tail_amount_ratio=0.20, last10_return_pct=0.40, close_at_high_ratio=0.95, auction_strength=0.33, rows=31, quality="real", provider_chain=["frozen-minute"]),
        "300750.SZ": TailMetrics(True, source="frozen-minute", tail_return_pct=0.78, tail_amount_ratio=0.17, last10_return_pct=0.28, close_at_high_ratio=0.90, auction_strength=0.26, rows=31, quality="real", provider_chain=["frozen-minute"]),
        "000001.SZ": TailMetrics(True, source="frozen-minute", tail_return_pct=0.62, tail_amount_ratio=0.14, last10_return_pct=0.18, close_at_high_ratio=0.86, auction_strength=0.21, rows=31, quality="real", provider_chain=["frozen-minute"]),
    }[ticker]


def score_map(ticker: str) -> tuple[float, float, dict[str, float]]:
    return {
        "600519.SS": (78.0, 84.5, {"trend_strength": 24.0, "tail_strength": 20.0, "relative_strength": 16.0}),
        "300750.SZ": (74.0, 81.2, {"trend_strength": 21.0, "tail_strength": 18.0, "relative_strength": 15.0}),
        "000001.SZ": (68.0, 76.4, {"trend_strength": 18.0, "tail_strength": 16.0, "relative_strength": 13.0}),
    }[ticker]


def write_frozen_inputs(data_dir: Path) -> None:
    universe_dir = data_dir / "overnight_cache" / "universe"
    history_dir = data_dir / "frozen_inputs" / "history"
    tail_dir = data_dir / "frozen_inputs" / "tail"
    universe_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    tail_dir.mkdir(parents=True, exist_ok=True)

    snapshot = pd.DataFrame(
        [
            {"code": "600519", "name": "Kweichow Moutai", "pool": "main", "score": 0.98, "latest": 108.6, "pct": 2.4, "amount": 1.8e9, "turnover": 2.5},
            {"code": "300750", "name": "CATL", "pool": "gem", "score": 0.92, "latest": 84.2, "pct": 1.8, "amount": 1.2e9, "turnover": 3.2},
            {"code": "000001", "name": "Ping An Bank", "pool": "main", "score": 0.88, "latest": 11.3, "pct": 1.2, "amount": 9.5e8, "turnover": 1.8},
        ]
    )
    for trade_date in REVIEW_TRADE_DATES + [SCAN_DATE]:
        snapshot.to_parquet(universe_dir / f"{trade_date}.parquet", index=False)

    for code in SAMPLE_CODES:
        make_history_frame(code).to_parquet(history_dir / f"{code}.parquet", index=False)
        ticker = build_security_profile(code, "cn_a").normalized_ticker
        (tail_dir / f"{code}.json").write_text(
            json.dumps(asdict(make_tail_metrics(ticker)), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    frozen_root = data_dir / "frozen_inputs"
    frozen_root.mkdir(parents=True, exist_ok=True)
    make_spot_frame().to_parquet(frozen_root / "spot.parquet", index=False)


def load_history_fixture(data_dir: Path, profile: Any) -> pd.DataFrame:
    code = str(profile.normalized_ticker).split(".", 1)[0]
    return pd.read_parquet(data_dir / "frozen_inputs" / "history" / f"{code}.parquet")


def load_tail_fixture(data_dir: Path, profile: Any) -> TailMetrics:
    code = str(profile.normalized_ticker).split(".", 1)[0]
    payload = json.loads((data_dir / "frozen_inputs" / "tail" / f"{code}.json").read_text(encoding="utf-8"))
    return TailMetrics(**payload)


def build_validation_payload(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    returns = {"600519.SS": 0.72, "300750.SZ": 0.41, "000001.SZ": -0.18}
    names = {"600519.SS": "Kweichow Moutai", "300750.SZ": "CATL", "000001.SZ": "Ping An Bank"}
    enriched: list[dict[str, Any]] = []
    formal: list[tuple[str, float]] = []
    for candidate in candidates:
        ticker = candidate["ticker"]
        if candidate["bucket"] == "formal":
            next_return = returns[ticker]
            formal.append((ticker, next_return))
            enriched.append({**candidate, "validation_status": "validated", "next_open_return": next_return, "next_open_date": "2025-03-21", "scan_close_price": candidate.get("latest")})
        else:
            enriched.append({**candidate, "validation_status": "watchlist_only", "next_open_return": None, "next_open_date": None, "scan_close_price": candidate.get("latest")})
    best = max(formal, key=lambda item: item[1])
    worst = min(formal, key=lambda item: item[1])
    return {
        "candidates": enriched,
        "summary": {
            "validated_formal_count": len(formal),
            "avg_next_open_return": round(sum(item[1] for item in formal) / len(formal), 4),
            "best_candidate": {"ticker": best[0], "name": names[best[0]], "next_open_return": best[1], "next_open_date": "2025-03-21"},
            "worst_candidate": {"ticker": worst[0], "name": names[worst[0]], "next_open_return": worst[1], "next_open_date": "2025-03-21"},
            "validation_status": "validated",
        },
    }


def run_acceptance(data_dir: Path) -> dict[str, Any]:
    os.environ["TRADINGAGENTS_DASHBOARD_DATA_DIR"] = str(data_dir)
    write_frozen_inputs(data_dir)
    app = create_app(data_dir=data_dir, launcher=DummyLauncher())
    summary: dict[str, Any] = {"data_dir": str(data_dir), "scan": None, "tasks": [], "review": None}

    with TestClient(app) as client, ExitStack() as stack:
        stack.enter_context(patch("tradingagents.overnight.scanner.load_market_spot_table", side_effect=lambda: pd.read_parquet(data_dir / "frozen_inputs" / "spot.parquet")))
        stack.enter_context(patch("tradingagents.overnight.scanner.load_index_snapshot", return_value={"sh": 0.5, "hs300": 0.4, "cyb": 0.6}))
        stack.enter_context(patch("tradingagents.overnight.scanner.evaluate_market_regime", return_value=MarketRegime(market_ok=True, market_message="Frozen regime: constructive", benchmark_pct=0.4)))
        stack.enter_context(patch("tradingagents.overnight.scanner.load_risk_stocks", return_value=(set(), {"matched_events": 0, "risk_codes": 0, "scanned_days": 0})))
        stack.enter_context(patch("tradingagents.overnight.scanner.load_history_frame", side_effect=lambda profile, *_: load_history_fixture(data_dir, profile)))
        stack.enter_context(patch("tradingagents.overnight.scanner.load_tail_metrics", side_effect=lambda profile, *_: load_tail_fixture(data_dir, profile)))
        stack.enter_context(patch("tradingagents.overnight.scanner.calc_quick_score", side_effect=lambda snapshot, *_: score_map(snapshot.code)[0]))
        stack.enter_context(patch("tradingagents.overnight.scanner.calculate_total_score", side_effect=lambda snapshot, *_: (score_map(snapshot.code)[1], score_map(snapshot.code)[2])))

        scan_response = client.post("/api/overnight/scans", json={"trade_date": SCAN_DATE, "market_region": "cn_a", "mode": "strict"})
        ensure(scan_response.status_code == 201, f"create scan failed: {scan_response.text}")
        scan_id = scan_response.json()["scan_id"]
        run_scan(scan_id)
        scan_detail = client.get(f"/api/overnight/scans/{scan_id}").json()
        ensure(scan_detail["status"] == "succeeded", "scan did not succeed")
        ensure(scan_detail["evaluation_config_hash"], "scan missing evaluation_config_hash")
        ensure(len(scan_detail["formal_recommendations"]) == 3, "scan should produce three formal candidates")
        summary["scan"] = {"scan_id": scan_id, "artifact_dir": scan_detail["artifact_dir"], "evaluation_config_hash": scan_detail["evaluation_config_hash"]}

        for code in SAMPLE_CODES:
            task_payload = {
                "ticker": code,
                "analysis_date": SCAN_DATE,
                "market_region": "cn_a",
                "analysts": ["market", "social", "news", "fundamentals"],
                "research_depth": 1,
                "llm_provider": "ark",
                "quick_think_llm": "doubao-seed-2.0-lite",
                "deep_think_llm": "doubao-seed-2.0-pro",
                "online_tools": True,
            }
            task_response = client.post(
                f"/api/tasks?source_type=overnight_scan&source_scan_id={scan_id}&source_trade_date={SCAN_DATE}&source_mode=strict&source_name={code}",
                json=task_payload,
            )
            ensure(task_response.status_code == 201, f"create task failed for {code}: {task_response.text}")
            task_id = task_response.json()["task_id"]
            run_task_with_retry(task_id)
            task_detail = client.get(f"/api/tasks/{task_id}").json()
            ensure(task_detail["status"] == "succeeded", f"task {code} did not succeed")
            ensure(task_detail["overnight_context"], f"task {code} missing overnight context")
            factor_snapshot = json.loads((Path(task_detail["artifact_dir"]) / "factor_snapshot.json").read_text(encoding="utf-8"))
            ensure("overnight_tail" in factor_snapshot.get("scores", {}), f"task {code} missing overnight_tail factor")
            summary["tasks"].append({"ticker": task_detail["ticker"], "task_id": task_id, "artifact_dir": task_detail["artifact_dir"], "decision": task_detail["decision"]})

        with patch("dashboard_api.app.validate_scan_candidates", return_value=build_validation_payload(app.state.candidate_store.list_candidates(scan_id))):
            validate_response = client.post(f"/api/overnight/scans/{scan_id}/validate")
        ensure(validate_response.status_code == 200, f"validate scan failed: {validate_response.text}")

        stack.enter_context(patch("tradingagents.overnight.review._load_trade_dates", return_value=REVIEW_TRADE_DATES))
        stack.enter_context(patch("tradingagents.overnight.review._build_benchmark_map", return_value={"2025-03-17": 0.15, "2025-03-18": -0.05, "2025-03-19": 0.10}))
        stack.enter_context(patch("tradingagents.overnight.review.load_history_frame", side_effect=lambda profile, *_: load_history_fixture(data_dir, profile)))
        stack.enter_context(patch("tradingagents.overnight.review.load_index_snapshot", return_value={"sh": 0.5, "hs300": 0.4, "cyb": 0.6}))
        stack.enter_context(patch("tradingagents.overnight.review.evaluate_market_regime", return_value=MarketRegime(market_ok=True, market_message="Frozen regime: constructive", benchmark_pct=0.4)))
        stack.enter_context(patch("tradingagents.overnight.review.load_risk_stocks", return_value=(set(), {"matched_events": 0, "risk_codes": 0, "scanned_days": 0})))
        stack.enter_context(patch("tradingagents.overnight.review.load_tail_metrics", side_effect=lambda profile, *_: load_tail_fixture(data_dir, profile)))
        stack.enter_context(patch("tradingagents.overnight.review.calc_quick_score", side_effect=lambda snapshot, *_: score_map(snapshot.code)[0]))
        stack.enter_context(patch("tradingagents.overnight.review.calculate_total_score", side_effect=lambda snapshot, *_: (score_map(snapshot.code)[1], score_map(snapshot.code)[2])))

        review_response = client.post("/api/overnight/reviews", json={"end_trade_date": REVIEW_END_DATE, "market_region": "cn_a"})
        ensure(review_response.status_code == 201, f"create review failed: {review_response.text}")
        review_id = review_response.json()["review_id"]
        run_review(review_id)
        review_detail = client.get(f"/api/overnight/reviews/{review_id}").json()
        ensure(review_detail["status"] == "succeeded", "review did not succeed")
        ensure(review_detail["evaluation_config_hash"], "review missing evaluation_config_hash")
        ensure(review_detail["regime_breakdown"], "review missing regime breakdown")
        ensure(review_detail["pool_breakdown"], "review missing pool breakdown")
        ensure(review_detail["tail_quality_breakdown"], "review missing tail-quality breakdown")
        summary["review"] = {"review_id": review_id, "artifact_dir": review_detail["artifact_dir"], "evaluation_config_hash": review_detail["evaluation_config_hash"]}

    summary_path = data_dir / "phase2_acceptance_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TradingAgents-CN phase 2 acceptance workflow.")
    parser.add_argument("--data-dir", default=str(Path("dashboard_data_phase2_acceptance").resolve()))
    args = parser.parse_args()
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    summary = run_acceptance(data_dir)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Acceptance summary: {data_dir / 'phase2_acceptance_summary.json'}")


if __name__ == "__main__":
    main()
