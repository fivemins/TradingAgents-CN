from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dashboard_api.app import (
    ensure_review_compat,
    ensure_scan_compat,
    ensure_task_compat,
    ensure_task_structured_summary,
    load_review_artifacts,
    load_scan_artifacts,
)
from dashboard_api.compat import values_differ
from dashboard_api.settings import get_settings
from dashboard_api.store import (
    OvernightCandidateStore,
    OvernightReviewStore,
    OvernightScanStore,
    TaskStore,
)


def _task_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    for key in ("config_snapshot", "structured_summary", "progress_message", "error_message", "decision"):
        if values_differ(before.get(key), after.get(key)):
            return True
    return False


def _scan_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    for key in (
        "summary_json",
        "market_message",
        "formal_count",
        "watchlist_count",
        "progress_message",
        "error_message",
    ):
        if values_differ(before.get(key), after.get(key)):
            return True
    return False


def _review_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    for key in ("summary_json", "progress_message", "error_message"):
        if values_differ(before.get(key), after.get(key)):
            return True
    return False


def repair_dashboard_data(data_dir: str | Path | None = None) -> dict[str, Any]:
    settings = get_settings(data_dir)
    task_store = TaskStore(settings.db_path)
    scan_store = OvernightScanStore(settings.db_path)
    review_store = OvernightReviewStore(settings.db_path)
    candidate_store = OvernightCandidateStore(settings.db_path)
    task_store.initialize()
    scan_store.initialize()
    review_store.initialize()
    candidate_store.initialize()

    report: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "data_dir": str(settings.data_dir),
        "checked": {"tasks": 0, "scans": 0, "reviews": 0},
        "repaired": {"tasks": 0, "scans": 0, "reviews": 0},
        "unresolved": {"tasks": 0, "scans": 0, "reviews": 0},
        "errors": [],
    }

    for task in task_store.list_tasks(limit=None):
        report["checked"]["tasks"] += 1
        before = task
        try:
            task = ensure_task_structured_summary(task_store, task)
            repaired = ensure_task_compat(task_store, task)
            if _task_changed(before, repaired):
                report["repaired"]["tasks"] += 1
        except Exception as exc:  # pragma: no cover - defensive maintenance path
            report["unresolved"]["tasks"] += 1
            report["errors"].append(
                {"kind": "task", "id": task.get("task_id"), "message": str(exc)}
            )

    for scan in scan_store.list_scans(limit=None):
        report["checked"]["scans"] += 1
        before = scan
        try:
            candidate_rows = candidate_store.list_candidates(scan["scan_id"])
            default_top = [
                candidate["ticker"]
                for candidate in candidate_rows
                if candidate.get("bucket") == "formal"
            ][:3]
            artifact_payload = load_scan_artifacts(
                Path(scan["artifact_dir"]),
                scan=scan,
                candidate_store=candidate_store,
                task_store=task_store,
            )
            merged = dict(scan)
            if not merged.get("summary_json"):
                merged["summary_json"] = artifact_payload.get("summary") or {}
            repaired = ensure_scan_compat(
                scan_store,
                merged,
                default_top_formal_tickers=default_top,
            )
            if _scan_changed(before, repaired):
                report["repaired"]["scans"] += 1
        except Exception as exc:  # pragma: no cover - defensive maintenance path
            report["unresolved"]["scans"] += 1
            report["errors"].append(
                {"kind": "scan", "id": scan.get("scan_id"), "message": str(exc)}
            )

    for review in review_store.list_reviews(limit=None):
        report["checked"]["reviews"] += 1
        before = review
        try:
            artifact_payload = load_review_artifacts(Path(review["artifact_dir"]))
            merged = dict(review)
            if not merged.get("summary_json"):
                merged["summary_json"] = artifact_payload.get("summary") or {}
            repaired = ensure_review_compat(review_store, merged)
            if _review_changed(before, repaired):
                report["repaired"]["reviews"] += 1
        except Exception as exc:  # pragma: no cover - defensive maintenance path
            report["unresolved"]["reviews"] += 1
            report["errors"].append(
                {"kind": "review", "id": review.get("review_id"), "message": str(exc)}
            )

    report_dir = settings.data_dir / "maintenance"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"repair_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report
