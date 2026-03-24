from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable

from tradingagents.qveris import QVerisUsageTracker

from .artifacts import build_scan_result
from .config import (
    OvernightEvaluationConfig,
    build_evaluation_config_payload,
    get_default_evaluation_config,
)
from .filters import check_buy_filters, load_risk_stocks
from .market_regime import evaluate_market_regime, load_index_snapshot
from .models import Candidate, OvernightMode, TailMetrics
from .scoring import (
    calc_quick_score,
    calculate_total_score,
    pick_history_enrichment_list,
    pick_tail_enrichment_list,
    split_quality_counts,
)
from .tail_data import load_history_frame, load_tail_metrics_batch
from .universe import (
    build_dynamic_pool_from_frame,
    build_snapshots_from_pool_frame,
    get_spot_provider_route,
    load_market_spot_table,
    persist_universe_snapshot,
)


ProgressCallback = Callable[[str], None]


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _summarize_tail_route(tails: dict[str, TailMetrics]) -> str:
    routes: set[str] = set()
    for tail in tails.values():
        chain = tail.provider_chain or ([tail.source] if tail.source else [])
        for route in chain:
            if not route or route == "snapshot_proxy":
                continue
            routes.add(route)
    if not routes:
        return "akshare_minute"
    if len(routes) == 1:
        return next(iter(routes))
    return "mixed"


def _build_empty_scan_result(
    *,
    trade_date: str,
    mode: OvernightMode,
    market_message: str,
    benchmark_pct: float,
    spot_route: str,
    index_route: str,
    notes: list[str],
    evaluation_payload: dict,
    qveris_tracker: QVerisUsageTracker,
) -> dict:
    summary = {
        "trade_date": trade_date,
        "market_region": "cn_a",
        "mode": mode,
        "market_ok": True,
        "market_message": market_message,
        "benchmark_pct": benchmark_pct,
        "formal_count": 0,
        "watchlist_count": 0,
        "universe_count": 0,
        "passed_filters": 0,
        "failed_filters": 0,
        "scored_count": 0,
        "rejected_count": 0,
        "tail_quality_counts": {"real": 0, "proxy": 0, "missing": 0},
        "data_quality": {
            "status": "incomplete",
            "message": "Dynamic pool is empty.",
        },
        "provider_route": {
            "spot": spot_route,
            "index": index_route,
            "history": "akshare_daily",
            "tail": "akshare_minute",
            "risk": "akshare_news",
        },
        "bias_flags": notes,
        "universe_snapshot_date": None,
        "evaluation_config_version": evaluation_payload["version"],
        "evaluation_config_hash": evaluation_payload["short_hash"],
    }
    audit = {
        "mode": mode,
        "pool_source": spot_route,
        "notes": notes,
        "risk_event_summary": {"matched_events": 0, "risk_codes": 0, "scanned_days": 0},
        "quality_counts": {"real": 0, "proxy": 0, "missing": 0},
        **qveris_tracker.to_audit_dict(),
    }
    return build_scan_result(
        summary=summary,
        preliminary_candidates=[],
        total_score_candidates=[],
        formal_recommendations=[],
        watchlist=[],
        rejected_candidates=[],
        excluded_examples=[],
        audit=audit,
    )


def run_overnight_scan(
    trade_date: str,
    mode: OvernightMode,
    data_dir: str | Path,
    progress: ProgressCallback | None = None,
    evaluation_config: OvernightEvaluationConfig | None = None,
) -> dict:
    evaluation_config = evaluation_config or get_default_evaluation_config()
    params = evaluation_config.live_scan_params
    evaluation_payload = build_evaluation_config_payload(evaluation_config)
    root = Path(data_dir)
    cache_root = root / "overnight_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    qveris_tracker = QVerisUsageTracker()

    notes: list[str] = []
    if progress:
        progress("正在评估市场环境。")
    index_snapshot = load_index_snapshot(trade_date, usage_tracker=qveris_tracker)
    regime = evaluate_market_regime(index_snapshot.values, trade_date)
    index_route = index_snapshot.provider_route or "unavailable"
    index_bias_flags = list(index_snapshot.bias_flags)

    if trade_date != _today():
        notes.append("historical_trade_date_uses_current_live_universe")
        regime.notes.append("historical_scan_uses_current_live_universe")
        if regime.market_message:
            regime.market_message += " | 历史日期扫描仍依赖当前可获取的活跃股票池或快照。"

    if progress:
        progress("正在加载 A 股动态股票池。")
    raw_spot = load_market_spot_table(cache_root=cache_root, usage_tracker=qveris_tracker)
    if raw_spot.empty:
        raise RuntimeError("无法加载 A 股市场现货总表，隔夜扫描已中止。")
    spot_route = get_spot_provider_route(raw_spot, "spot_unavailable")

    pool_frame = build_dynamic_pool_from_frame(raw_spot, params)
    snapshot_path: Path | None = None
    if pool_frame.empty:
        return _build_empty_scan_result(
            trade_date=trade_date,
            mode=mode,
            market_message=f"{regime.market_message} | 当前动态池为空。",
            benchmark_pct=regime.benchmark_pct,
            spot_route=spot_route,
            index_route=index_route,
            notes=list(dict.fromkeys(notes + regime.notes + index_bias_flags + ["dynamic_pool_empty"])),
            evaluation_payload=evaluation_payload,
            qveris_tracker=qveris_tracker,
        )

    try:
        snapshot_path = persist_universe_snapshot(pool_frame, cache_root, trade_date)
    except Exception as exc:
        notes.append(f"universe_snapshot_failed:{type(exc).__name__}")

    snapshots = build_snapshots_from_pool_frame(pool_frame, raw_spot)
    if progress:
        progress("正在加载风险事件过滤。")
    risk_codes, risk_summary = load_risk_stocks(trade_date, cache_root / "risk")

    passed: list[Candidate] = []
    failed: list[Candidate] = []
    for snapshot in snapshots:
        ok, reason = check_buy_filters(snapshot, risk_codes, params)
        candidate = Candidate(snapshot=snapshot, passed=ok, filter_reason=reason)
        if ok:
            candidate.quick_score = calc_quick_score(snapshot, regime.benchmark_pct)
            passed.append(candidate)
        else:
            failed.append(candidate)

    passed.sort(key=lambda item: item.quick_score, reverse=True)
    preliminary_candidates = list(passed)
    history_requested_codes = set(pick_history_enrichment_list(passed, params))

    if progress:
        progress("正在补充日线与尾盘分时数据。")
    histories: dict[str, object] = {}
    for code in history_requested_codes:
        snapshot = next((item.snapshot for item in passed if item.snapshot.code == code), None)
        if not snapshot:
            continue
        history = load_history_frame(snapshot.profile, trade_date, cache_root)
        if history is not None and not history.empty:
            histories[code] = history

    for candidate in passed:
        candidate.has_history = candidate.snapshot.code in histories

    tail_requested_codes = set(pick_tail_enrichment_list(passed, params))
    tail_snapshots = [
        candidate.snapshot
        for candidate in passed
        if candidate.snapshot.code in tail_requested_codes and candidate.has_history
    ]
    tails = load_tail_metrics_batch(
        tail_snapshots,
        trade_date,
        mode,
        cache_root,
        params.tail_start_time,
        params.tail_last_window_minutes,
        usage_tracker=qveris_tracker,
    )

    if progress:
        progress("正在计算综合评分与结果分层。")
    scored: list[Candidate] = []
    rejected_candidates: list[Candidate] = []
    excluded_examples: list[Candidate] = []

    for candidate in passed:
        code = candidate.snapshot.code
        candidate.tail_metrics = tails.get(code)
        if not candidate.has_history:
            candidate.selection_stage = "rejected"
            if code in history_requested_codes:
                candidate.rejected_reason = "missing_history"
                candidate.excluded_from_final = "missing_history"
            else:
                candidate.rejected_reason = "no_total_score"
                candidate.excluded_from_final = "no_total_score"
            rejected_candidates.append(candidate)
            excluded_examples.append(candidate)
            continue

        history = histories.get(code)
        if history is None:
            candidate.selection_stage = "rejected"
            candidate.rejected_reason = "no_total_score"
            candidate.excluded_from_final = "no_total_score"
            rejected_candidates.append(candidate)
            excluded_examples.append(candidate)
            continue

        total, breakdown = calculate_total_score(
            candidate.snapshot,
            history,  # type: ignore[arg-type]
            candidate.tail_metrics,
            regime.benchmark_pct,
            risk_codes,
            params,
        )
        candidate.total_score = total
        candidate.factor_breakdown = breakdown
        candidate.selection_stage = "scored"
        scored.append(candidate)

    scored.sort(key=lambda item: item.total_score, reverse=True)

    formal_threshold = params.formal_score + regime.formal_threshold_delta
    formal_limit = params.formal_max_total
    if regime.formal_limit_cap is not None:
        formal_limit = min(formal_limit, regime.formal_limit_cap)

    formal_recommendations = [
        item
        for item in scored
        if item.total_score >= formal_threshold and item.has_real_tail
    ][:formal_limit]
    for candidate in formal_recommendations:
        candidate.selection_stage = "formal"

    selected_codes = {item.snapshot.code for item in formal_recommendations}
    watch_threshold = params.watchlist_score if regime.market_ok else max(params.watchlist_score - 2.0, 0.0)
    watchlist_pool = [
        item
        for item in scored
        if item.snapshot.code not in selected_codes
        and item.total_score >= watch_threshold
        and item.quality in {"real", "proxy"}
    ]
    watchlist = watchlist_pool[: params.watchlist_max_total]
    watchlist_codes = {item.snapshot.code for item in watchlist}
    for candidate in watchlist:
        candidate.selection_stage = "watchlist"

    watchlist_pool_codes = {item.snapshot.code for item in watchlist_pool}
    for candidate in scored:
        code = candidate.snapshot.code
        if code in selected_codes or code in watchlist_codes:
            continue
        candidate.selection_stage = "rejected"
        if code in watchlist_pool_codes:
            candidate.rejected_reason = "watchlist_capacity_trim"
            candidate.excluded_from_final = "watchlist_capacity_trim"
        elif candidate.quality not in {"real", "proxy"}:
            candidate.rejected_reason = "tail_quality_ineligible"
            candidate.excluded_from_final = "tail_quality_ineligible"
        elif candidate.total_score < watch_threshold:
            candidate.rejected_reason = "below_watchlist_threshold"
            candidate.excluded_from_final = "below_watchlist_threshold"
        else:
            candidate.rejected_reason = "no_total_score"
            candidate.excluded_from_final = "no_total_score"
        rejected_candidates.append(candidate)
        excluded_examples.append(candidate)

    total_score_candidates = list(scored)
    quality_counts = split_quality_counts(scored)
    bias_flags = list(dict.fromkeys(notes + regime.notes + index_bias_flags))
    qveris_audit = qveris_tracker.to_audit_dict()
    qveris_routes = qveris_audit.get("qveris_routes", [])

    data_quality_status = "ok"
    data_quality_message = "Strict scan completed with live spot and minute routing."
    if mode == "research_fallback" and quality_counts.get("proxy", 0) > 0:
        data_quality_status = "research_fallback"
        data_quality_message = "Research fallback introduced proxy tail data for watchlist candidates."
    elif mode == "strict" and quality_counts.get("missing", 0) > 0:
        data_quality_status = "incomplete"
        data_quality_message = "Strict scan could not obtain real tail data for part of the candidate set."
    if qveris_routes:
        data_quality_message += " QVeris fallback was used for live market data."
    if "index_daily_fallback" in bias_flags:
        data_quality_message += " Index snapshot fell back to daily bars."

    summary = {
        "trade_date": trade_date,
        "market_region": "cn_a",
        "mode": mode,
        "market_ok": regime.market_ok,
        "market_message": regime.market_message,
        "benchmark_pct": regime.benchmark_pct,
        "formal_count": len(formal_recommendations),
        "watchlist_count": len(watchlist),
        "universe_count": len(snapshots),
        "passed_filters": len(passed),
        "failed_filters": len(failed),
        "scored_count": len(total_score_candidates),
        "rejected_count": len(rejected_candidates),
        "tail_quality_counts": quality_counts,
        "data_quality": {
            "status": data_quality_status,
            "message": data_quality_message,
            "real_tail_loaded": sum(1 for item in tails.values() if item.quality == "real"),
            "proxy_tail_loaded": sum(1 for item in tails.values() if item.quality == "proxy"),
            "missing_tail": sum(1 for item in tails.values() if item.quality in {"missing", "invalid"}),
        },
        "provider_route": {
            "spot": spot_route,
            "index": index_route,
            "history": "akshare_daily",
            "tail": _summarize_tail_route(tails),
            "risk": "akshare_news",
        },
        "bias_flags": bias_flags,
        "universe_snapshot_date": trade_date if snapshot_path else None,
        "evaluation_config_version": evaluation_payload["version"],
        "evaluation_config_hash": evaluation_payload["short_hash"],
    }
    audit = {
        "mode": mode,
        "pool_source": spot_route,
        "notes": bias_flags,
        "risk_event_summary": risk_summary,
        "quality_counts": quality_counts,
        "history_requested": len(history_requested_codes),
        "history_loaded": len(histories),
        "tail_requested": len(tail_requested_codes),
        "tail_loaded": sum(1 for item in tails.values() if item.quality == "real"),
        "tail_proxy": sum(1 for item in tails.values() if item.quality == "proxy"),
        "formal_threshold": formal_threshold,
        "watchlist_threshold": watch_threshold,
        "formal_limit": formal_limit,
        "data_quality": summary["data_quality"],
        "provider_route": summary["provider_route"],
        "universe_snapshot_date": summary["universe_snapshot_date"],
        "snapshot_path": str(snapshot_path) if snapshot_path else None,
        "evaluation_config_version": evaluation_payload["version"],
        "evaluation_config_hash": evaluation_payload["short_hash"],
        **qveris_audit,
    }
    return build_scan_result(
        summary=summary,
        preliminary_candidates=preliminary_candidates,
        total_score_candidates=total_score_candidates,
        formal_recommendations=formal_recommendations,
        watchlist=watchlist,
        rejected_candidates=rejected_candidates,
        excluded_examples=excluded_examples[:20],
        audit=audit,
    )
