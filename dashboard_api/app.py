from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from dashboard_api.catalog import (
    QUICK_MODEL_OPTIONS,
    build_options_payload,
    get_provider_base_url,
    is_valid_model,
)
from dashboard_api.compat import (
    normalize_overnight_context,
    normalize_review_artifact_payload,
    normalize_review_record,
    normalize_scan_artifact_payload,
    normalize_scan_record,
    normalize_source_context,
    normalize_structured_payload,
    normalize_task_record,
    values_differ,
)
from dashboard_api.launcher import TaskLauncher
from dashboard_api.readiness import collect_readiness
from dashboard_api.runtime import REPORT_FILE_MAP, build_structured_summary
from dashboard_api.schemas import (
    OvernightScanArtifactsResponse,
    OvernightScanCreateRequest,
    OvernightScanDetail,
    OvernightScanListResponse,
    OvernightScanSummary,
    OvernightReviewArtifactsResponse,
    OvernightReviewCreateRequest,
    OvernightReviewDetail,
    OvernightReviewListResponse,
    OvernightReviewSummary,
    SystemReadinessResponse,
    TaskArtifactsResponse,
    TaskCreateRequest,
    TaskDetail,
    TaskListResponse,
    TaskListStats,
    TaskOptionsResponse,
    TaskSummary,
)
from dashboard_api.settings import get_settings
from dashboard_api.store import (
    OvernightCandidateStore,
    OvernightReviewStore,
    OvernightScanStore,
    TaskStore,
    utc_now,
)
from tradingagents.market_utils import build_security_profile
from tradingagents.overnight.validation import validate_scan_candidates
from tradingagents.text_cleaning import clean_source_name, clean_structure


DEFAULT_TASK_ARTIFACTS = {
    "task.json",
    "final_state.json",
    "factor_snapshot.json",
    "evidence_snapshot.json",
    "structured_decision.json",
    "final_decision.md",
    "events.log",
    *REPORT_FILE_MAP.values(),
}
DEFAULT_SCAN_ARTIFACTS = {
    "scan.json",
    "recommendations.json",
    "audit.json",
    "scan_inputs.json",
    "data_sources.json",
    "evaluation_config.json",
    "events.log",
}
DEFAULT_REVIEW_ARTIFACTS = {
    "review.json",
    "daily_results.json",
    "candidate_results.json",
    "review_inputs.json",
    "data_sources.json",
    "evaluation_config.json",
    "events.log",
}
INDEX_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}


def derive_recur_limit(research_depth: int) -> int:
    return max(100, research_depth * 60)


def build_index_response(frontend_dist: Path) -> FileResponse:
    return FileResponse(frontend_dist / "index.html", headers=INDEX_CACHE_HEADERS)


def create_app(
    data_dir: str | Path | None = None,
    launcher: TaskLauncher | None = None,
) -> FastAPI:
    settings = get_settings(data_dir)
    task_store = TaskStore(settings.db_path)
    scan_store = OvernightScanStore(settings.db_path)
    review_store = OvernightReviewStore(settings.db_path)
    candidate_store = OvernightCandidateStore(settings.db_path)
    task_store.initialize()
    scan_store.initialize()
    review_store.initialize()
    candidate_store.initialize()
    settings.tasks_dir.mkdir(parents=True, exist_ok=True)
    settings.overnight_scans_dir.mkdir(parents=True, exist_ok=True)
    settings.overnight_reviews_dir.mkdir(parents=True, exist_ok=True)

    app = FastAPI(
        title="TradingAgents Dashboard API",
        version="0.2.0",
        description="Local web dashboard API for TradingAgents and overnight scan workflows.",
    )
    app.state.launcher = launcher or TaskLauncher(
        project_root=settings.project_root,
        data_dir=settings.data_dir,
    )
    app.state.store = task_store
    app.state.task_store = task_store
    app.state.scan_store = scan_store
    app.state.review_store = review_store
    app.state.candidate_store = candidate_store
    app.state.settings = settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/system/readiness", response_model=SystemReadinessResponse)
    def get_system_readiness(refresh: bool = Query(default=False)) -> dict[str, Any]:
        return collect_readiness(settings, refresh=refresh)

    @app.get("/api/options", response_model=TaskOptionsResponse)
    def get_options() -> dict[str, Any]:
        return build_options_payload()

    @app.post("/api/tasks", response_model=TaskDetail, status_code=201)
    def create_task(
        request: TaskCreateRequest,
        source_type: str | None = Query(default=None),
        source_scan_id: str | None = Query(default=None),
        source_trade_date: str | None = Query(default=None),
        source_mode: str | None = Query(default=None),
        source_name: str | None = Query(default=None),
    ) -> TaskDetail:
        if request.llm_provider.lower() not in QUICK_MODEL_OPTIONS:
            raise HTTPException(status_code=400, detail="Unsupported LLM provider.")

        if not is_valid_model(
            request.llm_provider,
            request.quick_think_llm,
            request.deep_think_llm,
        ):
            raise HTTPException(
                status_code=400,
                detail="The selected models are not valid for the chosen provider.",
            )

        security_profile = build_security_profile(request.ticker, request.market_region)

        source_context = build_source_context(
            source_type=source_type,
            source_scan_id=source_scan_id,
            source_trade_date=source_trade_date,
            source_mode=source_mode,
            ticker=security_profile.normalized_ticker,
            source_name=source_name,
        )
        overnight_candidate = None
        overnight_scan = None

        if source_type == "overnight_scan" and source_scan_id:
            overnight_candidate = candidate_store.get_candidate(
                source_scan_id,
                security_profile.normalized_ticker,
            )
            overnight_scan = scan_store.get_scan(source_scan_id)
            if overnight_candidate and overnight_candidate.get("linked_task_id"):
                existing_task = task_store.get_task(overnight_candidate["linked_task_id"])
                if existing_task:
                    existing_task = ensure_task_structured_summary(task_store, existing_task)
                    return build_task_detail(existing_task, candidate_store, scan_store)

        task_id = uuid4().hex
        artifact_dir = settings.tasks_dir / task_id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        config_snapshot = {
            "llm_provider": request.llm_provider,
            "llm_base_url": get_provider_base_url(request.llm_provider),
            "quick_think_llm": request.quick_think_llm,
            "deep_think_llm": request.deep_think_llm,
            "online_tools": request.online_tools,
            "market_region": request.market_region,
            "security_profile": security_profile.to_dict(),
            "max_debate_rounds": request.research_depth,
            "max_risk_discuss_rounds": request.research_depth,
            "max_recur_limit": derive_recur_limit(request.research_depth),
            "source_context": source_context,
            "overnight_context": build_task_overnight_snapshot(
                source_context,
                overnight_candidate,
                overnight_scan,
            ),
        }
        payload = request.model_dump()
        payload["ticker"] = security_profile.normalized_ticker

        task = task_store.create_task(
            task_id=task_id,
            payload=payload,
            artifact_dir=artifact_dir,
            config_snapshot=config_snapshot,
        )

        if source_type == "overnight_scan" and source_scan_id:
            candidate_store.link_task(
                source_scan_id,
                security_profile.normalized_ticker,
                task_id,
            )

        try:
            pid = launch_task_process(app.state.launcher, task_id)
            task = task_store.update_task(
                task_id,
                pid=pid,
                progress_message="Task queued. Runner process started.",
            )
        except Exception as exc:
            task = task_store.update_task(
                task_id,
                status="failed",
                error_message=f"Failed to launch task runner: {exc}",
                progress_message="Failed to start runner process.",
                finished_at=utc_now(),
            )
            raise HTTPException(
                status_code=500,
                detail="Task was created but the runner process could not be started.",
            ) from exc

        return build_task_detail(task, task_store, candidate_store, scan_store)

    @app.get("/api/tasks", response_model=TaskListResponse)
    def list_tasks(
        status: str | None = Query(default=None),
        source_type: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> TaskListResponse:
        raw_tasks = task_store.list_tasks(status=status, limit=None)
        if source_type:
            raw_tasks = [
                task
                for task in raw_tasks
                if ((task.get("config_snapshot") or {}).get("source_context") or {}).get("type")
                == source_type
            ]
        tasks = [ensure_task_structured_summary(task_store, task) for task in raw_tasks[:limit]]
        items = [TaskSummary(**build_task_summary_payload(task, task_store)) for task in tasks]
        stats = TaskListStats(**task_store.get_stats())
        return TaskListResponse(items=items, stats=stats)

    @app.get("/api/tasks/{task_id}", response_model=TaskDetail)
    def get_task(task_id: str) -> TaskDetail:
        task = task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")
        task = ensure_task_structured_summary(task_store, task)
        return build_task_detail(task, task_store, candidate_store, scan_store)

    @app.get("/api/tasks/{task_id}/artifacts", response_model=TaskArtifactsResponse)
    def get_artifacts(task_id: str) -> TaskArtifactsResponse:
        task = task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")

        artifact_dir = Path(task["artifact_dir"])
        report_files = {
            "market_report": artifact_dir / "reports" / "market.md",
            "sentiment_report": artifact_dir / "reports" / "sentiment.md",
            "news_report": artifact_dir / "reports" / "news.md",
            "fundamentals_report": artifact_dir / "reports" / "fundamentals.md",
            "trader_investment_plan": artifact_dir / "reports" / "trader_plan.md",
            "investment_plan": artifact_dir / "reports" / "investment_plan.md",
            "final_trade_decision": artifact_dir / "final_decision.md",
        }
        reports = {
            key: clean_structure(path.read_text(encoding="utf-8")) if path.exists() else ""
            for key, path in report_files.items()
        }

        return TaskArtifactsResponse(
            task_id=task_id,
            reports=reports,
            downloads=build_task_download_urls(task_id),
            structured=build_structured_payload(artifact_dir),
        )

    @app.post("/api/tasks/{task_id}/terminate", response_model=TaskDetail)
    def terminate_task(task_id: str) -> TaskDetail:
        task = task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")

        terminated = terminate_task_record(task_store, task)
        return build_task_detail(terminated, task_store, candidate_store, scan_store)

    @app.delete("/api/tasks/{task_id}")
    def delete_task(task_id: str) -> dict[str, bool]:
        task = task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")

        if task.get("status") in {"queued", "running"}:
            task = terminate_task_record(task_store, task)

        artifact_dir = Path(task["artifact_dir"])
        if artifact_dir.exists():
            shutil.rmtree(artifact_dir)

        unlink_overnight_candidate_task(candidate_store, task)
        task_store.delete_task(task_id)
        return {"ok": True}

    @app.get("/api/tasks/{task_id}/download/{artifact_name}")
    def download_artifact(task_id: str, artifact_name: str) -> FileResponse:
        task = task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")
        if artifact_name not in DEFAULT_TASK_ARTIFACTS:
            raise HTTPException(status_code=404, detail="Artifact not found.")

        artifact_dir = Path(task["artifact_dir"])
        if artifact_name in REPORT_FILE_MAP.values():
            file_path = artifact_dir / "reports" / artifact_name
        else:
            file_path = artifact_dir / artifact_name
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Artifact file is missing.")
        return FileResponse(file_path, filename=artifact_name)

    @app.post("/api/overnight/scans", response_model=OvernightScanDetail, status_code=201)
    def create_overnight_scan(request: OvernightScanCreateRequest) -> OvernightScanDetail:
        scan_id = uuid4().hex
        artifact_dir = settings.overnight_scans_dir / scan_id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        scan = scan_store.create_scan(
            scan_id=scan_id,
            payload=request.model_dump(),
            artifact_dir=artifact_dir,
        )
        try:
            pid = launch_scan_process(app.state.launcher, scan_id)
            scan = scan_store.update_scan(
                scan_id,
                pid=pid,
                progress_message="Scan queued. Runner process started.",
            )
        except Exception as exc:
            scan = scan_store.update_scan(
                scan_id,
                status="failed",
                error_message=f"Failed to launch overnight scan runner: {exc}",
                progress_message="Failed to start overnight scan runner.",
                finished_at=utc_now(),
            )
            raise HTTPException(
                status_code=500,
                detail="Scan was created but the runner process could not be started.",
            ) from exc

        return build_scan_detail(scan, scan_store, candidate_store, task_store)

    @app.get("/api/overnight/scans", response_model=OvernightScanListResponse)
    def list_overnight_scans(
        status: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> OvernightScanListResponse:
        scans = [
            build_scan_summary(scan, scan_store)
            for scan in scan_store.list_scans(status=status, limit=limit)
        ]
        return OvernightScanListResponse(items=scans)

    @app.get("/api/overnight/scans/{scan_id}", response_model=OvernightScanDetail)
    def get_overnight_scan(scan_id: str) -> OvernightScanDetail:
        scan = scan_store.get_scan(scan_id)
        if not scan:
            raise HTTPException(status_code=404, detail="Overnight scan not found.")
        return build_scan_detail(scan, scan_store, candidate_store, task_store)

    @app.post("/api/overnight/scans/{scan_id}/validate", response_model=OvernightScanDetail)
    def validate_overnight_scan(scan_id: str) -> OvernightScanDetail:
        scan = scan_store.get_scan(scan_id)
        if not scan:
            raise HTTPException(status_code=404, detail="Overnight scan not found.")
        if scan["status"] != "succeeded":
            raise HTTPException(status_code=400, detail="Only completed scans can be validated.")

        candidates = candidate_store.list_candidates(scan_id)
        if not candidates:
            raise HTTPException(status_code=400, detail="This scan has no persisted candidates.")

        validation_result = validate_scan_candidates(
            trade_date=scan["trade_date"],
            market_region=scan["market_region"],
            candidates=candidates,
            data_dir=settings.data_dir,
        )
        for candidate in validation_result["candidates"]:
            candidate_store.update_candidate(
                scan_id,
                candidate["ticker"],
                validation_status=candidate.get("validation_status"),
                next_open_return=candidate.get("next_open_return"),
                next_open_date=candidate.get("next_open_date"),
                scan_close_price=candidate.get("scan_close_price"),
                updated_at=utc_now(),
            )

        summary_snapshot = {
            **(scan.get("summary_json") or {}),
            **validation_result["summary"],
        }
        scan = scan_store.update_scan(
            scan_id,
            summary_json=summary_snapshot,
            progress_message="Next-open validation updated.",
        ) or scan_store.get_scan(scan_id)
        if not scan:
            raise HTTPException(status_code=500, detail="Scan validation state could not be refreshed.")
        return build_scan_detail(scan, scan_store, candidate_store, task_store)

    @app.get(
        "/api/overnight/scans/{scan_id}/artifacts",
        response_model=OvernightScanArtifactsResponse,
    )
    def get_overnight_scan_artifacts(scan_id: str) -> OvernightScanArtifactsResponse:
        scan = scan_store.get_scan(scan_id)
        if not scan:
            raise HTTPException(status_code=404, detail="Overnight scan not found.")
        payload = load_scan_artifacts(
            Path(scan["artifact_dir"]),
            scan=scan,
            candidate_store=candidate_store,
            task_store=task_store,
        )
        return OvernightScanArtifactsResponse(
            scan_id=scan_id,
            summary=payload.get("summary", {}),
            preliminary_candidates=payload.get("preliminary_candidates", []),
            total_score_candidates=payload.get("total_score_candidates", []),
            formal_recommendations=payload.get("formal_recommendations", []),
            watchlist=payload.get("watchlist", []),
            rejected_candidates=payload.get("rejected_candidates", []),
            excluded_examples=payload.get("excluded_examples", []),
            audit=payload.get("audit", {}),
            downloads=build_scan_download_urls(scan_id),
        )

    @app.get("/api/overnight/scans/{scan_id}/download/{artifact_name}")
    def download_scan_artifact(scan_id: str, artifact_name: str) -> FileResponse:
        scan = scan_store.get_scan(scan_id)
        if not scan:
            raise HTTPException(status_code=404, detail="Overnight scan not found.")
        if artifact_name not in DEFAULT_SCAN_ARTIFACTS:
            raise HTTPException(status_code=404, detail="Artifact not found.")
        file_path = Path(scan["artifact_dir"]) / artifact_name
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Artifact file is missing.")
        return FileResponse(file_path, filename=artifact_name)

    @app.post("/api/overnight/reviews", response_model=OvernightReviewDetail, status_code=201)
    def create_overnight_review(
        request: OvernightReviewCreateRequest,
    ) -> OvernightReviewDetail:
        review_id = uuid4().hex
        artifact_dir = settings.overnight_reviews_dir / review_id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        review = review_store.create_review(
            review_id=review_id,
            payload={
                **request.model_dump(),
                "window_days": 60,
                "mode": "strict",
                "return_basis": "next_open",
            },
            artifact_dir=artifact_dir,
        )
        try:
            pid = launch_review_process(app.state.launcher, review_id)
            review = review_store.update_review(
                review_id,
                pid=pid,
                progress_message="Review queued. Runner process started.",
            )
        except Exception as exc:
            review = review_store.update_review(
                review_id,
                status="failed",
                error_message=f"Failed to launch overnight review runner: {exc}",
                progress_message="Failed to start overnight review runner.",
                finished_at=utc_now(),
            )
            raise HTTPException(
                status_code=500,
                detail="Review was created but the runner process could not be started.",
            ) from exc

        return build_review_detail(review, review_store)

    @app.get("/api/overnight/reviews", response_model=OvernightReviewListResponse)
    def list_overnight_reviews(
        status: str | None = Query(default=None),
        limit: int = Query(default=20, ge=1, le=100),
    ) -> OvernightReviewListResponse:
        reviews = [
            build_review_summary(review, review_store)
            for review in review_store.list_reviews(status=status, limit=limit)
        ]
        return OvernightReviewListResponse(items=reviews)

    @app.get("/api/overnight/reviews/{review_id}", response_model=OvernightReviewDetail)
    def get_overnight_review(review_id: str) -> OvernightReviewDetail:
        review = review_store.get_review(review_id)
        if not review:
            raise HTTPException(status_code=404, detail="Overnight review not found.")
        return build_review_detail(review, review_store)

    @app.get(
        "/api/overnight/reviews/{review_id}/artifacts",
        response_model=OvernightReviewArtifactsResponse,
    )
    def get_overnight_review_artifacts(review_id: str) -> OvernightReviewArtifactsResponse:
        review = review_store.get_review(review_id)
        if not review:
            raise HTTPException(status_code=404, detail="Overnight review not found.")
        payload = load_review_artifacts(Path(review["artifact_dir"]))
        summary_payload = payload.get("summary") or review.get("summary_json") or {}
        audit_payload = payload.get("audit") or (
            summary_payload.get("audit", {}) if isinstance(summary_payload, dict) else {}
        )
        return OvernightReviewArtifactsResponse(
            review_id=review_id,
            summary=summary_payload,
            daily_results=payload.get("daily_results", []),
            candidate_results=payload.get("candidate_results", []),
            audit=audit_payload,
            downloads=build_review_download_urls(review_id),
        )

    @app.get("/api/overnight/reviews/{review_id}/download/{artifact_name}")
    def download_review_artifact(review_id: str, artifact_name: str) -> FileResponse:
        review = review_store.get_review(review_id)
        if not review:
            raise HTTPException(status_code=404, detail="Overnight review not found.")
        if artifact_name not in DEFAULT_REVIEW_ARTIFACTS:
            raise HTTPException(status_code=404, detail="Artifact not found.")
        file_path = Path(review["artifact_dir"]) / artifact_name
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Artifact file is missing.")
        return FileResponse(file_path, filename=artifact_name)

    if settings.frontend_dist.exists():

        @app.get("/", include_in_schema=False)
        def serve_dashboard_index() -> FileResponse:
            return build_index_response(settings.frontend_dist)

        @app.get("/{path_name:path}", include_in_schema=False)
        def serve_dashboard_app(path_name: str) -> FileResponse:
            if path_name.startswith("api/"):
                raise HTTPException(status_code=404, detail="Not found.")
            candidate = settings.frontend_dist / path_name
            if candidate.is_file():
                if candidate.name == "index.html":
                    return build_index_response(settings.frontend_dist)
                return FileResponse(candidate)
            return build_index_response(settings.frontend_dist)

    return app


def launch_task_process(launcher: Any, task_id: str) -> int:
    if hasattr(launcher, "launch_task"):
        return int(launcher.launch_task(task_id))
    return int(launcher.launch(task_id))


def launch_scan_process(launcher: Any, scan_id: str) -> int:
    if hasattr(launcher, "launch_overnight_scan"):
        return int(launcher.launch_overnight_scan(scan_id))
    return int(launcher.launch(scan_id))


def launch_review_process(launcher: Any, review_id: str) -> int:
    if hasattr(launcher, "launch_overnight_review"):
        return int(launcher.launch_overnight_review(review_id))
    return int(launcher.launch(review_id))


def terminate_task_process(pid: int | None) -> None:
    if not pid:
        return
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        return
    try:
        os.kill(int(pid), 15)
    except ProcessLookupError:
        return


def terminate_task_record(task_store: TaskStore, task: dict[str, Any]) -> dict[str, Any]:
    if task.get("status") in {"queued", "running"}:
        terminate_task_process(task.get("pid"))
        updated = task_store.update_task(
            task["task_id"],
            status="failed",
            stage="completed",
            progress_message="Task terminated by user.",
            error_message="Task terminated by user.",
            finished_at=utc_now(),
            pid=None,
        )
        if updated:
            return updated
    return task


def unlink_overnight_candidate_task(
    candidate_store: OvernightCandidateStore,
    task: dict[str, Any],
) -> None:
    source_context = ((task.get("config_snapshot") or {}).get("source_context") or {})
    if source_context.get("type") != "overnight_scan":
        return
    scan_id = source_context.get("scan_id")
    ticker = source_context.get("ticker") or task.get("ticker")
    if not scan_id or not ticker:
        return
    candidate = candidate_store.get_candidate(scan_id, ticker)
    if not candidate or candidate.get("linked_task_id") != task.get("task_id"):
        return
    candidate_store.update_candidate(
        scan_id,
        ticker,
        linked_task_id=None,
        updated_at=utc_now(),
    )


def build_source_context(
    source_type: str | None,
    source_scan_id: str | None,
    source_trade_date: str | None,
    source_mode: str | None,
    ticker: str,
    source_name: str | None,
) -> dict[str, Any] | None:
    if not source_type:
        return None
    return {
        "type": source_type,
        "scan_id": source_scan_id,
        "trade_date": source_trade_date,
        "mode": source_mode,
        "ticker": ticker,
        "name": clean_source_name(source_name, ticker),
    }


def build_task_overnight_snapshot(
    source_context: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
    scan: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not source_context or source_context.get("type") != "overnight_scan":
        return None
    if not candidate:
        return None
    summary = (scan or {}).get("summary_json") or {}
    return clean_structure(
        {
            "scan_id": source_context.get("scan_id"),
            "scan_trade_date": source_context.get("trade_date"),
            "scan_mode": source_context.get("mode"),
            "source_name": source_context.get("name"),
            "bucket": candidate.get("bucket"),
            "quality": candidate.get("quality"),
            "quick_score": candidate.get("quick_score"),
            "total_score": candidate.get("total_score"),
            "factor_breakdown": candidate.get("factor_breakdown"),
            "tail_metrics": candidate.get("tail_metrics"),
            "provider_route": summary.get("provider_route"),
            "evaluation_config_version": summary.get("evaluation_config_version"),
            "evaluation_config_hash": summary.get("evaluation_config_hash"),
        }
    )


def build_task_download_urls(task_id: str) -> dict[str, str]:
    return {
        "task_json": f"/api/tasks/{task_id}/download/task.json",
        "final_state_json": f"/api/tasks/{task_id}/download/final_state.json",
        "factor_snapshot_json": f"/api/tasks/{task_id}/download/factor_snapshot.json",
        "evidence_snapshot_json": f"/api/tasks/{task_id}/download/evidence_snapshot.json",
        "structured_decision_json": f"/api/tasks/{task_id}/download/structured_decision.json",
        "final_decision_markdown": f"/api/tasks/{task_id}/download/final_decision.md",
        "events_log": f"/api/tasks/{task_id}/download/events.log",
    }


def build_scan_download_urls(scan_id: str) -> dict[str, str]:
    return {
        "scan_json": f"/api/overnight/scans/{scan_id}/download/scan.json",
        "recommendations_json": f"/api/overnight/scans/{scan_id}/download/recommendations.json",
        "audit_json": f"/api/overnight/scans/{scan_id}/download/audit.json",
        "scan_inputs_json": f"/api/overnight/scans/{scan_id}/download/scan_inputs.json",
        "data_sources_json": f"/api/overnight/scans/{scan_id}/download/data_sources.json",
        "evaluation_config_json": f"/api/overnight/scans/{scan_id}/download/evaluation_config.json",
        "events_log": f"/api/overnight/scans/{scan_id}/download/events.log",
    }


def build_review_download_urls(review_id: str) -> dict[str, str]:
    return {
        "review_json": f"/api/overnight/reviews/{review_id}/download/review.json",
        "daily_results_json": f"/api/overnight/reviews/{review_id}/download/daily_results.json",
        "candidate_results_json": f"/api/overnight/reviews/{review_id}/download/candidate_results.json",
        "review_inputs_json": f"/api/overnight/reviews/{review_id}/download/review_inputs.json",
        "data_sources_json": f"/api/overnight/reviews/{review_id}/download/data_sources.json",
        "evaluation_config_json": f"/api/overnight/reviews/{review_id}/download/evaluation_config.json",
        "events_log": f"/api/overnight/reviews/{review_id}/download/events.log",
    }


def build_structured_payload(artifact_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in ("factor_snapshot", "evidence_snapshot", "structured_decision"):
        path = artifact_dir / f"{name}.json"
        if not path.exists():
            result[name] = None
            continue
        try:
            payload = clean_structure(json.loads(path.read_text(encoding="utf-8")))
            result[name] = payload if payload else None
        except json.JSONDecodeError:
            result[name] = None
    return normalize_structured_payload(result)


def ensure_task_compat(store: TaskStore, task: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_task_record(task)
    update_fields: dict[str, Any] = {}
    for key in ("config_snapshot", "structured_summary", "progress_message", "error_message", "decision"):
        if values_differ(task.get(key), normalized.get(key)):
            update_fields[key] = normalized.get(key)
    if update_fields:
        updated = store.update_task(task["task_id"], **update_fields)
        if updated:
            return normalize_task_record(updated)
        return normalize_task_record({**task, **update_fields})
    return normalized


def ensure_scan_compat(
    store: OvernightScanStore,
    scan: dict[str, Any],
    default_top_formal_tickers: list[str] | None = None,
) -> dict[str, Any]:
    normalized = normalize_scan_record(
        scan,
        default_top_formal_tickers=default_top_formal_tickers,
    )
    update_fields: dict[str, Any] = {}
    for key in (
        "summary_json",
        "market_message",
        "formal_count",
        "watchlist_count",
        "progress_message",
        "error_message",
    ):
        if values_differ(scan.get(key), normalized.get(key)):
            update_fields[key] = normalized.get(key)
    if update_fields:
        updated = store.update_scan(scan["scan_id"], **update_fields)
        if updated:
            return normalize_scan_record(
                updated,
                default_top_formal_tickers=default_top_formal_tickers,
            )
        return normalize_scan_record(
            {**scan, **update_fields},
            default_top_formal_tickers=default_top_formal_tickers,
        )
    return normalized


def ensure_review_compat(store: OvernightReviewStore, review: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_review_record(review)
    update_fields: dict[str, Any] = {}
    for key in ("summary_json", "progress_message", "error_message"):
        if values_differ(review.get(key), normalized.get(key)):
            update_fields[key] = normalized.get(key)
    if update_fields:
        updated = store.update_review(review["review_id"], **update_fields)
        if updated:
            return normalize_review_record(updated)
        return normalize_review_record({**review, **update_fields})
    return normalized


def ensure_task_structured_summary(
    store: TaskStore,
    task: dict[str, Any],
) -> dict[str, Any]:
    task = ensure_task_compat(store, task)
    if task.get("structured_summary"):
        return task

    artifact_dir = Path(task["artifact_dir"])
    structured_payload = build_structured_payload(artifact_dir)
    summary = build_structured_summary(
        clean_structure(structured_payload.get("factor_snapshot")),
        clean_structure(structured_payload.get("structured_decision")),
        final_action=task.get("decision"),
    )
    if not summary:
        return task
    updated = store.update_task(task["task_id"], structured_summary=summary)
    return ensure_task_compat(store, updated or {**task, "structured_summary": summary})


def build_task_summary_payload(task: dict[str, Any], store: TaskStore | None = None) -> dict[str, Any]:
    normalized = ensure_task_compat(store, task) if store else normalize_task_record(task)
    payload = dict(normalized)
    payload["source_context"] = payload.get("source_context")
    return payload


def build_overnight_context(
    task: dict[str, Any],
    candidate_store: OvernightCandidateStore | None,
    scan_store: OvernightScanStore | None,
) -> dict[str, Any] | None:
    source_context = ((task.get("config_snapshot") or {}).get("source_context") or {})
    stored_context = ((task.get("config_snapshot") or {}).get("overnight_context") or {})
    if source_context.get("type") != "overnight_scan" or not candidate_store:
        return normalize_overnight_context(stored_context, task.get("ticker"))
    if not scan_store:
        return None
    scan_id = source_context.get("scan_id")
    ticker = source_context.get("ticker") or task.get("ticker")
    if not scan_id or not ticker:
        return normalize_overnight_context(stored_context, task.get("ticker"))
    candidate = candidate_store.get_candidate(scan_id, ticker)
    if not candidate:
        return normalize_overnight_context(stored_context, task.get("ticker"))
    scan = scan_store.get_scan(scan_id)
    summary = (scan or {}).get("summary_json") or {}
    return normalize_overnight_context(
        {
            "scan_id": scan_id,
            "scan_trade_date": source_context.get("trade_date"),
            "scan_mode": source_context.get("mode"),
            "source_name": source_context.get("name"),
            "bucket": candidate.get("bucket"),
            "quality": candidate.get("quality"),
            "quick_score": candidate.get("quick_score"),
            "total_score": candidate.get("total_score"),
            "factor_breakdown": candidate.get("factor_breakdown"),
            "tail_metrics": candidate.get("tail_metrics"),
            "provider_route": summary.get("provider_route") or stored_context.get("provider_route"),
            "evaluation_config_version": summary.get("evaluation_config_version") or stored_context.get("evaluation_config_version"),
            "evaluation_config_hash": summary.get("evaluation_config_hash") or stored_context.get("evaluation_config_hash"),
            "validation_status": candidate.get("validation_status"),
            "next_open_return": candidate.get("next_open_return"),
            "next_open_date": candidate.get("next_open_date"),
        },
        task.get("ticker"),
    )


def build_task_detail(
    task: dict[str, Any],
    task_store: TaskStore | None = None,
    candidate_store: OvernightCandidateStore | None = None,
    scan_store: OvernightScanStore | None = None,
) -> TaskDetail:
    normalized_task = ensure_task_compat(task_store, task) if task_store else normalize_task_record(task)
    artifact_dir = Path(normalized_task["artifact_dir"])
    report_status = {
        "market_report": (artifact_dir / "reports" / "market.md").exists(),
        "sentiment_report": (artifact_dir / "reports" / "sentiment.md").exists(),
        "news_report": (artifact_dir / "reports" / "news.md").exists(),
        "fundamentals_report": (artifact_dir / "reports" / "fundamentals.md").exists(),
        "investment_plan": (artifact_dir / "reports" / "investment_plan.md").exists(),
        "trader_investment_plan": (artifact_dir / "reports" / "trader_plan.md").exists(),
        "final_trade_decision": (artifact_dir / "final_decision.md").exists(),
        "final_state": (artifact_dir / "final_state.json").exists(),
        "factor_snapshot": (artifact_dir / "factor_snapshot.json").exists(),
        "evidence_snapshot": (artifact_dir / "evidence_snapshot.json").exists(),
        "structured_decision": (artifact_dir / "structured_decision.json").exists(),
        "events_log": (artifact_dir / "events.log").exists(),
    }
    detail = {
        **build_task_summary_payload(normalized_task),
        "overnight_context": build_overnight_context(normalized_task, candidate_store, scan_store),
        "download_urls": build_task_download_urls(normalized_task["task_id"]),
        "report_status": report_status,
    }
    return TaskDetail(**detail)


def load_scan_artifacts(
    artifact_dir: Path,
    scan: dict[str, Any] | None = None,
    candidate_store: OvernightCandidateStore | None = None,
    task_store: TaskStore | None = None,
) -> dict[str, Any]:
    summary = {}
    recommendations = {
        "preliminary_candidates": [],
        "total_score_candidates": [],
        "formal_recommendations": [],
        "watchlist": [],
        "rejected_candidates": [],
        "excluded_examples": [],
    }
    audit = {}

    scan_path = artifact_dir / "scan.json"
    if scan_path.exists():
        try:
            scan_payload = json.loads(scan_path.read_text(encoding="utf-8"))
            summary = clean_structure(
                scan_payload.get("summary_json") or scan_payload.get("summary_snapshot") or {}
            )
        except json.JSONDecodeError:
            summary = {}

    recommendations_path = artifact_dir / "recommendations.json"
    if recommendations_path.exists():
        try:
            recommendations = clean_structure(
                json.loads(recommendations_path.read_text(encoding="utf-8"))
            )
        except json.JSONDecodeError:
            recommendations = recommendations.copy()

    audit_path = artifact_dir / "audit.json"
    if audit_path.exists():
        try:
            audit = clean_structure(json.loads(audit_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            audit = {}

    payload = {
        "summary": summary,
        "preliminary_candidates": recommendations.get("preliminary_candidates", []),
        "total_score_candidates": recommendations.get("total_score_candidates", []),
        "formal_recommendations": recommendations.get("formal_recommendations", []),
        "watchlist": recommendations.get("watchlist", []),
        "rejected_candidates": recommendations.get("rejected_candidates", []),
        "excluded_examples": recommendations.get("excluded_examples", []),
        "audit": audit,
    }
    if scan and candidate_store and task_store:
        candidate_rows = candidate_store.list_candidates(scan["scan_id"])
        if candidate_rows:
            payload["formal_recommendations"] = [
                enrich_candidate_task_state(candidate, task_store)
                for candidate in candidate_rows
                if candidate.get("bucket") == "formal"
            ]
            payload["watchlist"] = [
                enrich_candidate_task_state(candidate, task_store)
                for candidate in candidate_rows
                if candidate.get("bucket") == "watchlist"
            ]
            payload["summary"] = scan.get("summary_json") or summary
    return normalize_scan_artifact_payload(payload)


def load_review_artifacts(artifact_dir: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    daily_results: list[dict[str, Any]] = []
    candidate_results: list[dict[str, Any]] = []
    audit: dict[str, Any] = {}

    review_path = artifact_dir / "review.json"
    if review_path.exists():
        try:
            review_payload = json.loads(review_path.read_text(encoding="utf-8"))
            summary = clean_structure(
                review_payload.get("summary_json") or review_payload.get("summary_snapshot") or {}
            )
        except json.JSONDecodeError:
            summary = {}

    daily_path = artifact_dir / "daily_results.json"
    if daily_path.exists():
        try:
            daily_results = clean_structure(json.loads(daily_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            daily_results = []

    candidate_path = artifact_dir / "candidate_results.json"
    if candidate_path.exists():
        try:
            candidate_results = clean_structure(
                json.loads(candidate_path.read_text(encoding="utf-8"))
            )
        except json.JSONDecodeError:
            candidate_results = []

    if isinstance(summary, dict):
        audit = summary.get("audit", {}) if isinstance(summary.get("audit"), dict) else {}

    return normalize_review_artifact_payload(
        {
        "summary": summary,
        "daily_results": daily_results,
        "candidate_results": candidate_results,
        "audit": audit,
        }
    )


def build_scan_summary(
    scan: dict[str, Any],
    scan_store: OvernightScanStore | None = None,
    default_top_formal_tickers: list[str] | None = None,
) -> OvernightScanSummary:
    normalized_scan = (
        ensure_scan_compat(
            scan_store,
            scan,
            default_top_formal_tickers=default_top_formal_tickers,
        )
        if scan_store
        else normalize_scan_record(scan, default_top_formal_tickers=default_top_formal_tickers)
    )
    summary_snapshot = clean_structure(normalized_scan.get("summary_json") or {})
    summary_payload = {
        **normalized_scan,
        "summary_snapshot": summary_snapshot,
        "top_formal_tickers": summary_snapshot.get("top_formal_tickers", []),
        "scored_count": summary_snapshot.get("scored_count", 0),
        "rejected_count": summary_snapshot.get("rejected_count", 0),
        "validated_formal_count": summary_snapshot.get("validated_formal_count", 0),
        "avg_next_open_return": summary_snapshot.get("avg_next_open_return"),
        "best_candidate": summary_snapshot.get("best_candidate"),
        "worst_candidate": summary_snapshot.get("worst_candidate"),
        "validation_status": summary_snapshot.get("validation_status"),
        "data_quality": summary_snapshot.get("data_quality"),
        "provider_route": summary_snapshot.get("provider_route"),
        "bias_flags": summary_snapshot.get("bias_flags", []),
        "universe_snapshot_date": summary_snapshot.get("universe_snapshot_date"),
        "evaluation_config_version": summary_snapshot.get("evaluation_config_version"),
        "evaluation_config_hash": summary_snapshot.get("evaluation_config_hash"),
    }
    return OvernightScanSummary(**summary_payload)


def enrich_candidate_task_state(
    candidate: dict[str, Any],
    task_store: TaskStore,
) -> dict[str, Any]:
    payload = dict(candidate)
    linked_task_id = payload.get("linked_task_id")
    if not linked_task_id:
        payload["linked_task_status"] = None
        payload["linked_task_decision"] = None
        return payload
    linked_task = task_store.get_task(linked_task_id)
    if not linked_task:
        payload["linked_task_status"] = None
        payload["linked_task_decision"] = None
        return payload
    payload["linked_task_status"] = linked_task.get("status")
    payload["linked_task_decision"] = linked_task.get("decision")
    return payload


def build_scan_detail(
    scan: dict[str, Any],
    scan_store: OvernightScanStore,
    candidate_store: OvernightCandidateStore,
    task_store: TaskStore,
) -> OvernightScanDetail:
    artifact_dir = Path(scan["artifact_dir"])
    artifact_payload = load_scan_artifacts(
        artifact_dir,
        scan=scan,
        candidate_store=candidate_store,
        task_store=task_store,
    )
    default_top_formal_tickers = [
        candidate["ticker"] for candidate in artifact_payload.get("formal_recommendations", [])[:3]
    ]
    normalized_scan = ensure_scan_compat(
        scan_store,
        scan,
        default_top_formal_tickers=default_top_formal_tickers,
    )
    summary_snapshot = normalized_scan.get("summary_json") or artifact_payload.get("summary") or {}
    detail_payload = {
        **build_scan_summary(
            normalized_scan,
            default_top_formal_tickers=default_top_formal_tickers,
        ).model_dump(),
        "artifact_dir": normalized_scan["artifact_dir"],
        "summary_snapshot": summary_snapshot,
        "download_urls": build_scan_download_urls(normalized_scan["scan_id"]),
        "preliminary_candidates": artifact_payload.get("preliminary_candidates", []),
        "total_score_candidates": artifact_payload.get("total_score_candidates", []),
        "formal_recommendations": artifact_payload.get("formal_recommendations", []),
        "watchlist": artifact_payload.get("watchlist", []),
        "rejected_candidates": artifact_payload.get("rejected_candidates", []),
        "excluded_examples": artifact_payload.get("excluded_examples", []),
        "audit": artifact_payload.get("audit", {}),
    }
    return OvernightScanDetail(**detail_payload)


def build_review_summary(
    review: dict[str, Any],
    review_store: OvernightReviewStore | None = None,
) -> OvernightReviewSummary:
    normalized_review = ensure_review_compat(review_store, review) if review_store else normalize_review_record(review)
    summary_snapshot = clean_structure(normalized_review.get("summary_json"))
    return OvernightReviewSummary(
        **{
            **normalized_review,
            "summary_snapshot": summary_snapshot,
            "data_quality": (summary_snapshot or {}).get("data_quality"),
            "provider_route": (summary_snapshot or {}).get("provider_route"),
            "bias_flags": (summary_snapshot or {}).get("bias_flags", []),
            "universe_snapshot_date": (summary_snapshot or {}).get("universe_snapshot_date"),
            "survivorship_bias": bool((summary_snapshot or {}).get("survivorship_bias")),
            "evaluation_config_version": (summary_snapshot or {}).get("evaluation_config_version"),
            "evaluation_config_hash": (summary_snapshot or {}).get("evaluation_config_hash"),
            "regime_breakdown": (summary_snapshot or {}).get("regime_breakdown", []),
            "pool_breakdown": (summary_snapshot or {}).get("pool_breakdown", []),
            "tail_quality_breakdown": (summary_snapshot or {}).get("tail_quality_breakdown", []),
        }
    )


def build_review_detail(
    review: dict[str, Any],
    review_store: OvernightReviewStore | None = None,
) -> OvernightReviewDetail:
    normalized_review = ensure_review_compat(review_store, review) if review_store else normalize_review_record(review)
    artifact_payload = load_review_artifacts(Path(normalized_review["artifact_dir"]))
    summary_snapshot = clean_structure(normalized_review.get("summary_json") or artifact_payload.get("summary"))
    detail_payload = {
        **build_review_summary(normalized_review).model_dump(),
        "artifact_dir": normalized_review["artifact_dir"],
        "summary_snapshot": summary_snapshot,
        "download_urls": build_review_download_urls(normalized_review["review_id"]),
        "audit": clean_structure(artifact_payload.get("audit", {})),
    }
    return OvernightReviewDetail(**detail_payload)


def main() -> None:
    uvicorn.run(
        "dashboard_api.app:create_app",
        factory=True,
        host="127.0.0.1",
        port=8000,
    )


if __name__ == "__main__":
    main()
