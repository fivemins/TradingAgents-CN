from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path

from dashboard_api.runtime import (
    append_event,
    build_structured_summary,
    detect_stage_update,
    serialize_final_state,
    summarize_message,
    write_report_files,
)
from dashboard_api.settings import get_settings
from dashboard_api.store import TaskStore, utc_now
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.text_cleaning import clean_structure, clean_text


def derive_recur_limit(research_depth: int) -> int:
    return max(100, research_depth * 60)


def build_config(task: dict) -> dict:
    config = DEFAULT_CONFIG.copy()
    config["max_debate_rounds"] = task["research_depth"]
    config["max_risk_discuss_rounds"] = task["research_depth"]
    config["max_recur_limit"] = derive_recur_limit(task["research_depth"])
    config["quick_think_llm"] = task["quick_think_llm"]
    config["deep_think_llm"] = task["deep_think_llm"]
    config["llm_provider"] = task["llm_provider"]
    config["online_tools"] = task["online_tools"]
    config["market_region"] = task["market_region"]
    config["results_dir"] = "./results"

    snapshot = task["config_snapshot"]
    llm_base_url = snapshot.get("llm_base_url")
    if llm_base_url:
        config["llm_base_url"] = llm_base_url
        config["backend_url"] = llm_base_url

    if snapshot.get("source_context"):
        config["source_context"] = snapshot.get("source_context")
    if snapshot.get("overnight_context"):
        config["overnight_context"] = snapshot.get("overnight_context")

    return config


def run_task(task_id: str) -> None:
    settings = get_settings()
    store = TaskStore(settings.db_path)
    store.initialize()

    task = store.get_task(task_id)
    if not task:
        raise SystemExit(f"Task {task_id} was not found.")

    artifact_dir = Path(task["artifact_dir"])
    report_dir = artifact_dir / "reports"
    events_log = artifact_dir / "events.log"
    task_json = artifact_dir / "task.json"
    final_state_path = artifact_dir / "final_state.json"
    factor_snapshot_path = artifact_dir / "factor_snapshot.json"
    evidence_snapshot_path = artifact_dir / "evidence_snapshot.json"
    structured_decision_path = artifact_dir / "structured_decision.json"

    artifact_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    task_json.write_text(json.dumps(task, indent=2), encoding="utf-8")
    append_event(events_log, "SYSTEM", "Runner started.")
    store.update_task(
        task_id,
        status="running",
        stage="initializing",
        progress_message="Initializing analysis graph.",
        started_at=utc_now(),
        error_message=None,
    )

    config = build_config(task)
    graph = TradingAgentsGraph(
        selected_analysts=task["analysts"],
        config=config,
        debug=False,
    )

    init_state = graph.propagator.create_initial_state(
        task["ticker"],
        task["analysis_date"],
        source_context=task["config_snapshot"].get("source_context"),
        overnight_context=task["config_snapshot"].get("overnight_context"),
    )
    args = graph.propagator.get_graph_args()

    written_values: dict[str, str] = {}
    seen_values: dict[str, str] = {}
    final_state = None

    try:
        for chunk in graph.graph.stream(init_state, **args):
            final_state = chunk

            stage, message = detect_stage_update(chunk, seen_values)
            if stage and message:
                store.update_task(task_id, stage=stage, progress_message=message)
                append_event(events_log, stage.upper(), message)

            write_report_files(report_dir, chunk, written_values)

            latest_message = summarize_message(chunk)
            if latest_message:
                append_event(events_log, "MESSAGE", latest_message)

        if not final_state:
            raise RuntimeError("The analysis finished without producing a final state.")

        final_state = graph.enrich_final_state(
            final_state,
            task["ticker"],
            task["analysis_date"],
        )
        final_state = clean_structure(final_state)
        serialized_state = clean_structure(serialize_final_state(final_state))
        decision = graph.process_signal(
            final_state["final_trade_decision"],
            final_state.get("structured_decision"),
        )

        final_state_path.write_text(
            json.dumps(serialized_state, indent=2),
            encoding="utf-8",
        )
        factor_snapshot_path.write_text(
            json.dumps(final_state.get("factor_snapshot", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        evidence_snapshot_path.write_text(
            json.dumps(final_state.get("evidence_snapshot", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        structured_decision_path.write_text(
            json.dumps(final_state.get("structured_decision", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        structured_summary = build_structured_summary(
            final_state.get("factor_snapshot"),
            final_state.get("structured_decision"),
            final_action=decision,
        )
        (artifact_dir / "final_decision.md").write_text(
            clean_text(final_state["final_trade_decision"]) or "",
            encoding="utf-8",
        )
        store.update_task(
            task_id,
            status="succeeded",
            stage="completed",
            progress_message="Analysis completed successfully.",
            decision=decision,
            structured_summary=structured_summary,
            finished_at=utc_now(),
        )
        task_json.write_text(
            json.dumps(store.get_task(task_id), indent=2),
            encoding="utf-8",
        )
        append_event(events_log, "SYSTEM", f"Task completed with decision {decision}.")
    except Exception as exc:
        append_event(events_log, "ERROR", f"{type(exc).__name__}: {exc}")
        append_event(events_log, "TRACEBACK", traceback.format_exc())
        store.update_task(
            task_id,
            status="failed",
            progress_message="Task failed during execution.",
            error_message=f"{type(exc).__name__}: {exc}",
            finished_at=utc_now(),
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a TradingAgents dashboard task.")
    parser.add_argument("task_id", help="Task identifier to execute.")
    args = parser.parse_args()
    run_task(args.task_id)


if __name__ == "__main__":
    main()
