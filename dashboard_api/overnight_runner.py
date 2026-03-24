from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path

from dashboard_api.runtime import append_event
from dashboard_api.settings import get_settings
from dashboard_api.store import OvernightCandidateStore, OvernightScanStore, utc_now
from tradingagents.overnight.config import build_evaluation_config_payload, get_default_evaluation_config
from tradingagents.overnight.scanner import run_overnight_scan
from tradingagents.qveris.auth import build_qveris_auth_summary
from tradingagents.text_cleaning import clean_structure


def run_scan(scan_id: str) -> None:
    settings = get_settings()
    store = OvernightScanStore(settings.db_path)
    candidate_store = OvernightCandidateStore(settings.db_path)
    store.initialize()
    candidate_store.initialize()

    scan = store.get_scan(scan_id)
    if not scan:
        raise SystemExit(f"Scan {scan_id} was not found.")

    artifact_dir = Path(scan["artifact_dir"])
    events_log = artifact_dir / "events.log"
    scan_json = artifact_dir / "scan.json"
    recommendations_json = artifact_dir / "recommendations.json"
    audit_json = artifact_dir / "audit.json"
    scan_inputs_json = artifact_dir / "scan_inputs.json"
    data_sources_json = artifact_dir / "data_sources.json"
    evaluation_config_json = artifact_dir / "evaluation_config.json"

    artifact_dir.mkdir(parents=True, exist_ok=True)
    qveris_auth = build_qveris_auth_summary()
    append_event(events_log, "SYSTEM", "Overnight scan runner started.")
    store.update_scan(
        scan_id,
        status="running",
        progress_message="Initializing overnight market scanner.",
        started_at=utc_now(),
        error_message=None,
    )
    scan_json.write_text(json.dumps(store.get_scan(scan_id), indent=2), encoding="utf-8")
    scan_inputs_json.write_text(
        json.dumps(
            {
                "trade_date": scan["trade_date"],
                "market_region": scan["market_region"],
                "mode": scan["mode"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    evaluation_config_json.write_text(
        json.dumps(
            build_evaluation_config_payload(get_default_evaluation_config()),
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def on_progress(message: str) -> None:
        store.update_scan(scan_id, progress_message=message)
        append_event(events_log, "SCAN", message)

    try:
        result = run_overnight_scan(
            trade_date=scan["trade_date"],
            mode=scan["mode"],
            data_dir=settings.data_dir,
            progress=on_progress,
        )
        result = clean_structure(result)
        recommendations_payload = {
            "preliminary_candidates": result.get("preliminary_candidates", []),
            "total_score_candidates": result.get("total_score_candidates", []),
            "formal_recommendations": result.get("formal_recommendations", []),
            "watchlist": result.get("watchlist", []),
            "rejected_candidates": result.get("rejected_candidates", []),
            "excluded_examples": result.get("excluded_examples", []),
        }
        recommendations_json.write_text(
            json.dumps(recommendations_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        audit_json.write_text(
            json.dumps(result.get("audit", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        data_sources_json.write_text(
            json.dumps(
                {
                    "provider_route": result.get("summary", {}).get("provider_route", {}),
                    "data_quality": result.get("summary", {}).get("data_quality", {}),
                    "bias_flags": result.get("summary", {}).get("bias_flags", []),
                    "snapshot_path": result.get("audit", {}).get("snapshot_path"),
                    "qveris_enabled": qveris_auth.get("configured", False),
                    "qveris_routes": result.get("audit", {}).get("qveris_routes", []),
                    "qveris_tool_ids": result.get("audit", {}).get("qveris_tool_ids", []),
                    "qveris_fallback_count": result.get("audit", {}).get("qveris_fallback_count", 0),
                    "qveris_enabled_capabilities": result.get("audit", {}).get("qveris_enabled_capabilities", []),
                    "qveris_skipped_capabilities": result.get("audit", {}).get("qveris_skipped_capabilities", {}),
                    "qveris_batch_calls": result.get("audit", {}).get("qveris_batch_calls", {}),
                    "qveris_requested_codes": result.get("audit", {}).get("qveris_requested_codes", {}),
                    "qveris_resolved_codes": result.get("audit", {}).get("qveris_resolved_codes", {}),
                    "qveris_budget_policy": result.get("audit", {}).get("qveris_budget_policy", {}),
                    "qveris_skip_reasons": result.get("audit", {}).get("qveris_skip_reasons", []),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        summary = result.get("summary", {})
        summary = {
            **summary,
            "top_formal_tickers": [
                item["ticker"] for item in recommendations_payload["formal_recommendations"][:3]
            ],
            "scored_count": len(recommendations_payload["total_score_candidates"]),
            "rejected_count": len(recommendations_payload["rejected_candidates"]),
            "validated_formal_count": 0,
            "avg_next_open_return": None,
            "best_candidate": None,
            "worst_candidate": None,
            "validation_status": "pending"
            if recommendations_payload["formal_recommendations"]
            else "empty",
        }
        candidate_store.replace_scan_candidates(
            scan_id=scan_id,
            trade_date=scan["trade_date"],
            market_region=scan["market_region"],
            formal_candidates=recommendations_payload["formal_recommendations"],
            watchlist_candidates=recommendations_payload["watchlist"],
        )
        updated = store.update_scan(
            scan_id,
            status="succeeded",
            progress_message="Overnight scan completed successfully.",
            summary_json=summary,
            market_message=summary.get("market_message", ""),
            formal_count=summary.get("formal_count", 0),
            watchlist_count=summary.get("watchlist_count", 0),
            finished_at=utc_now(),
        )
        scan_json.write_text(
            json.dumps(updated or store.get_scan(scan_id), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        append_event(
            events_log,
            "SYSTEM",
            f"Overnight scan completed with {summary.get('formal_count', 0)} formal recommendations.",
        )
    except Exception as exc:
        append_event(events_log, "ERROR", f"{type(exc).__name__}: {exc}")
        append_event(events_log, "TRACEBACK", traceback.format_exc())
        store.update_scan(
            scan_id,
            status="failed",
            progress_message="Overnight scan failed during execution.",
            error_message=f"{type(exc).__name__}: {exc}",
            finished_at=utc_now(),
        )
        scan_json.write_text(
            json.dumps(store.get_scan(scan_id), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an overnight scan task.")
    parser.add_argument("scan_id", help="Overnight scan identifier to execute.")
    args = parser.parse_args()
    run_scan(args.scan_id)


if __name__ == "__main__":
    main()
