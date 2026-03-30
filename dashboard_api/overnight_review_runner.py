from __future__ import annotations

import argparse
from dataclasses import replace
import json
import traceback
from pathlib import Path

from dashboard_api.runtime import append_event, reset_event_log
from dashboard_api.settings import get_settings
from dashboard_api.store import OvernightReviewStore, utc_now
from tradingagents.overnight.config import build_evaluation_config_payload, get_default_evaluation_config
from tradingagents.overnight.review import run_overnight_review
from tradingagents.text_cleaning import clean_structure


def run_review(review_id: str) -> None:
    settings = get_settings()
    store = OvernightReviewStore(settings.db_path)
    store.initialize()

    review = store.get_review(review_id)
    if not review:
        raise SystemExit(f"Review {review_id} was not found.")

    artifact_dir = Path(review["artifact_dir"])
    events_log = artifact_dir / "events.log"
    review_json = artifact_dir / "review.json"
    daily_results_json = artifact_dir / "daily_results.json"
    candidate_results_json = artifact_dir / "candidate_results.json"
    review_inputs_json = artifact_dir / "review_inputs.json"
    data_sources_json = artifact_dir / "data_sources.json"
    evaluation_config_json = artifact_dir / "evaluation_config.json"

    artifact_dir.mkdir(parents=True, exist_ok=True)
    reset_event_log(events_log)
    append_event(events_log, "SYSTEM", "Overnight review runner started.")
    store.update_review(
        review_id,
        status="running",
        progress_message="Initializing overnight history review.",
        started_at=utc_now(),
        error_message=None,
    )
    review_json.write_text(json.dumps(store.get_review(review_id), indent=2), encoding="utf-8")
    review_inputs_json.write_text(
        json.dumps(
            {
                "end_trade_date": review["end_trade_date"],
                "market_region": review["market_region"],
                "window_days": int(review["window_days"]),
                "mode": review["mode"],
                "return_basis": review["return_basis"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    active_evaluation_config = replace(
        get_default_evaluation_config(),
        review_return_basis=review["return_basis"],
    )
    evaluation_config_json.write_text(
        json.dumps(
            build_evaluation_config_payload(active_evaluation_config),
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def on_progress(message: str) -> None:
        store.update_review(review_id, progress_message=message)
        append_event(events_log, "REVIEW", message)

    try:
        result = run_overnight_review(
            end_trade_date=review["end_trade_date"],
            data_dir=settings.data_dir,
            progress=on_progress,
            window_days=int(review["window_days"]),
            mode=review["mode"],
            return_basis=review["return_basis"],
            evaluation_config=active_evaluation_config,
        )
        result = clean_structure(result)
        daily_results_json.write_text(
            json.dumps(result.get("daily_results", []), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        candidate_results_json.write_text(
            json.dumps(result.get("candidate_results", []), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        data_sources_json.write_text(
            json.dumps(
                {
                    "provider_route": result.get("summary", {}).get("provider_route", {}),
                    "data_quality": result.get("summary", {}).get("data_quality", {}),
                    "bias_flags": result.get("summary", {}).get("bias_flags", []),
                    "survivorship_bias": result.get("summary", {}).get("survivorship_bias"),
                    "universe_snapshot_dates": result.get("audit", {}).get("universe_snapshot_dates", {}),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        summary = result.get("summary", {})
        updated = store.update_review(
            review_id,
            status="succeeded",
            progress_message="Overnight review completed successfully.",
            summary_json={
                **summary,
                "audit": result.get("audit", {}),
            },
            finished_at=utc_now(),
        )
        review_json.write_text(
            json.dumps(updated or store.get_review(review_id), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        append_event(
            events_log,
            "SYSTEM",
            "Overnight review completed successfully.",
        )
    except Exception as exc:
        append_event(events_log, "ERROR", f"{type(exc).__name__}: {exc}")
        append_event(events_log, "TRACEBACK", traceback.format_exc())
        store.update_review(
            review_id,
            status="failed",
            progress_message="Overnight review failed during execution.",
            error_message=f"{type(exc).__name__}: {exc}",
            finished_at=utc_now(),
        )
        review_json.write_text(
            json.dumps(store.get_review(review_id), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an overnight historical review task.")
    parser.add_argument("review_id", help="Overnight review identifier to execute.")
    args = parser.parse_args()
    run_review(args.review_id)


if __name__ == "__main__":
    main()
