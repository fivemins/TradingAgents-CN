from __future__ import annotations

import json
from typing import Any

from tradingagents.text_cleaning import clean_source_name, clean_structure, clean_text


VALID_OVERNIGHT_QUALITIES = {"real", "proxy", "missing", "invalid"}
VALID_SELECTION_STAGES = {"preliminary", "scored", "formal", "watchlist", "rejected"}
DEFAULT_SCAN_DATA_QUALITY = {
    "status": "unknown",
    "message": "历史扫描记录缺少数据质量摘要。",
}
DEFAULT_REVIEW_DATA_QUALITY = {
    "status": "unknown",
    "message": "历史验证记录缺少数据质量摘要。",
}


def values_differ(left: Any, right: Any) -> bool:
    return json.dumps(clean_structure(left), sort_keys=True, ensure_ascii=False, default=str) != json.dumps(
        clean_structure(right), sort_keys=True, ensure_ascii=False, default=str
    )


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        cleaned = clean_structure(value)
        return cleaned if isinstance(cleaned, dict) else {}
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        cleaned = clean_structure(value)
        return cleaned if isinstance(cleaned, list) else []
    return []


def _as_string_list(value: Any) -> list[str]:
    results: list[str] = []
    for item in _as_list(value):
        text = clean_text(item if isinstance(item, str) else str(item))
        if text:
            results.append(text)
    return results


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_source_context(
    value: Any,
    fallback_ticker: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = _as_dict(value)
    ticker = clean_text(payload.get("ticker")) or fallback_ticker
    payload["ticker"] = ticker
    payload["name"] = clean_source_name(payload.get("name"), ticker)
    return payload


def normalize_overnight_context(
    value: Any,
    fallback_ticker: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = _as_dict(value)
    payload["source_name"] = clean_source_name(
        payload.get("source_name"),
        fallback_ticker or clean_text(payload.get("ticker")),
    )
    payload["factor_breakdown"] = _as_dict(payload.get("factor_breakdown"))
    payload["tail_metrics"] = _as_dict(payload.get("tail_metrics")) or None
    payload["provider_route"] = _as_dict(payload.get("provider_route")) or None
    return payload


def normalize_structured_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = _as_dict(value)
    normalized = {
        "composite_score": _as_float(payload.get("composite_score")),
        "confidence": _as_float(payload.get("confidence")),
        "recommended_action": clean_text(payload.get("recommended_action")),
        "primary_driver": clean_text(payload.get("primary_driver")),
        "primary_risk": clean_text(payload.get("primary_risk")),
    }
    if not any(item is not None for item in normalized.values()):
        return None
    return normalized


def normalize_task_record(task: dict[str, Any]) -> dict[str, Any]:
    payload = clean_structure(dict(task))
    config_snapshot = _as_dict(payload.get("config_snapshot"))
    source_context = normalize_source_context(
        config_snapshot.get("source_context"),
        payload.get("ticker"),
    )
    overnight_context = normalize_overnight_context(
        config_snapshot.get("overnight_context"),
        payload.get("ticker"),
    )
    config_snapshot["source_context"] = source_context
    if overnight_context:
        config_snapshot["overnight_context"] = overnight_context
    elif "overnight_context" in config_snapshot:
        config_snapshot["overnight_context"] = None
    payload["config_snapshot"] = config_snapshot
    payload["source_context"] = source_context
    payload["structured_summary"] = normalize_structured_summary(payload.get("structured_summary"))
    payload["progress_message"] = clean_text(payload.get("progress_message")) or ""
    payload["error_message"] = clean_text(payload.get("error_message"))
    payload["decision"] = clean_text(payload.get("decision"))
    return payload


def normalize_candidate(value: Any, default_bucket: str | None = None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = _as_dict(value)
    ticker = clean_text(payload.get("ticker"))
    if not ticker:
        return None
    quality = payload.get("quality")
    normalized_quality = quality if quality in VALID_OVERNIGHT_QUALITIES else "missing"
    selection_stage = clean_text(payload.get("selection_stage"))
    normalized = {
        **payload,
        "ticker": ticker,
        "name": clean_source_name(payload.get("name"), ticker) or ticker,
        "bucket": clean_text(payload.get("bucket")) or default_bucket,
        "pool": clean_text(payload.get("pool")) or "--",
        "quality": normalized_quality,
        "latest": _as_float(payload.get("latest"), 0.0) or 0.0,
        "pct": _as_float(payload.get("pct"), 0.0) or 0.0,
        "amount": _as_float(payload.get("amount"), 0.0) or 0.0,
        "turnover": _as_float(payload.get("turnover"), 0.0) or 0.0,
        "quick_score": _as_float(payload.get("quick_score"), 0.0) or 0.0,
        "total_score": _as_float(payload.get("total_score"), 0.0) or 0.0,
        "factor_breakdown": _as_dict(payload.get("factor_breakdown")),
        "selection_stage": selection_stage if selection_stage in VALID_SELECTION_STAGES else None,
        "rejected_reason": clean_text(payload.get("rejected_reason")),
        "tail_metrics": _as_dict(payload.get("tail_metrics")) or None,
        "filter_reason": clean_text(payload.get("filter_reason")),
        "excluded_from_final": clean_text(payload.get("excluded_from_final")),
        "linked_task_id": clean_text(payload.get("linked_task_id")),
        "linked_task_status": clean_text(payload.get("linked_task_status")),
        "linked_task_decision": clean_text(payload.get("linked_task_decision")),
        "validation_status": clean_text(payload.get("validation_status")),
        "next_open_return": _as_float(payload.get("next_open_return")),
        "next_open_date": clean_text(payload.get("next_open_date")),
        "scan_close_price": _as_float(payload.get("scan_close_price")),
    }
    return normalized


def normalize_candidate_list(value: Any, default_bucket: str | None = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in _as_list(value):
        normalized = normalize_candidate(item, default_bucket=default_bucket)
        if normalized:
            results.append(normalized)
    return results


def normalize_validated_candidate(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = _as_dict(value)
    ticker = clean_text(payload.get("ticker"))
    if not ticker:
        return None
    return {
        "ticker": ticker,
        "name": clean_source_name(payload.get("name"), ticker) or ticker,
        "next_open_return": _as_float(payload.get("next_open_return")),
        "next_open_date": clean_text(payload.get("next_open_date")),
    }


def normalize_breakdown_list(value: Any) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in _as_list(value):
        if not isinstance(item, dict):
            continue
        payload = _as_dict(item)
        results.append(
            {
                "group": clean_text(payload.get("group")) or "未分组",
                "days_with_formal_picks": _as_int(payload.get("days_with_formal_picks"), 0),
                "candidate_count": _as_int(payload.get("candidate_count"), 0),
                "avg_next_open_return": _as_float(payload.get("avg_next_open_return")),
                "avg_excess_return": _as_float(payload.get("avg_excess_return")),
                "positive_pick_rate": _as_float(payload.get("positive_pick_rate")),
            }
        )
    return results


def normalize_scan_summary_snapshot(
    value: Any,
    default_top_formal_tickers: list[str] | None = None,
) -> dict[str, Any]:
    payload = _as_dict(value)
    top_formal_tickers = _as_string_list(payload.get("top_formal_tickers"))
    if not top_formal_tickers and default_top_formal_tickers:
        top_formal_tickers = [item for item in default_top_formal_tickers if item]
    payload["top_formal_tickers"] = top_formal_tickers
    payload["scored_count"] = _as_int(payload.get("scored_count"), 0)
    payload["rejected_count"] = _as_int(payload.get("rejected_count"), 0)
    payload["validated_formal_count"] = _as_int(payload.get("validated_formal_count"), 0)
    payload["avg_next_open_return"] = _as_float(payload.get("avg_next_open_return"))
    payload["best_candidate"] = normalize_validated_candidate(payload.get("best_candidate"))
    payload["worst_candidate"] = normalize_validated_candidate(payload.get("worst_candidate"))
    payload["validation_status"] = clean_text(payload.get("validation_status"))
    payload["data_quality"] = _as_dict(payload.get("data_quality")) or dict(DEFAULT_SCAN_DATA_QUALITY)
    payload["provider_route"] = _as_dict(payload.get("provider_route"))
    payload["bias_flags"] = _as_string_list(payload.get("bias_flags"))
    payload["universe_snapshot_date"] = clean_text(payload.get("universe_snapshot_date"))
    payload["evaluation_config_version"] = clean_text(payload.get("evaluation_config_version"))
    payload["evaluation_config_hash"] = clean_text(payload.get("evaluation_config_hash"))
    return payload


def normalize_scan_record(
    scan: dict[str, Any],
    default_top_formal_tickers: list[str] | None = None,
) -> dict[str, Any]:
    payload = clean_structure(dict(scan))
    payload["market_message"] = clean_text(payload.get("market_message")) or ""
    payload["progress_message"] = clean_text(payload.get("progress_message")) or ""
    payload["error_message"] = clean_text(payload.get("error_message"))
    payload["formal_count"] = _as_int(payload.get("formal_count"), 0)
    payload["watchlist_count"] = _as_int(payload.get("watchlist_count"), 0)
    payload["summary_json"] = normalize_scan_summary_snapshot(
        payload.get("summary_json"),
        default_top_formal_tickers=default_top_formal_tickers,
    )
    return payload


def normalize_scan_artifact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    summary = normalize_scan_summary_snapshot(payload.get("summary"))
    return {
        "summary": summary,
        "preliminary_candidates": normalize_candidate_list(
            payload.get("preliminary_candidates"),
            default_bucket="preliminary",
        ),
        "total_score_candidates": normalize_candidate_list(
            payload.get("total_score_candidates"),
            default_bucket="scored",
        ),
        "formal_recommendations": normalize_candidate_list(
            payload.get("formal_recommendations"),
            default_bucket="formal",
        ),
        "watchlist": normalize_candidate_list(payload.get("watchlist"), default_bucket="watchlist"),
        "rejected_candidates": normalize_candidate_list(
            payload.get("rejected_candidates"),
            default_bucket="rejected",
        ),
        "excluded_examples": normalize_candidate_list(
            payload.get("excluded_examples"),
            default_bucket="excluded",
        ),
        "audit": _as_dict(payload.get("audit")),
    }


def normalize_review_summary_snapshot(value: Any) -> dict[str, Any]:
    payload = _as_dict(value)
    payload["data_quality"] = _as_dict(payload.get("data_quality")) or dict(DEFAULT_REVIEW_DATA_QUALITY)
    payload["provider_route"] = _as_dict(payload.get("provider_route"))
    payload["bias_flags"] = _as_string_list(payload.get("bias_flags"))
    payload["universe_snapshot_date"] = clean_text(payload.get("universe_snapshot_date"))
    payload["survivorship_bias"] = bool(payload.get("survivorship_bias"))
    payload["evaluation_config_version"] = clean_text(payload.get("evaluation_config_version"))
    payload["evaluation_config_hash"] = clean_text(payload.get("evaluation_config_hash"))
    payload["regime_breakdown"] = normalize_breakdown_list(payload.get("regime_breakdown"))
    payload["pool_breakdown"] = normalize_breakdown_list(payload.get("pool_breakdown"))
    payload["tail_quality_breakdown"] = normalize_breakdown_list(payload.get("tail_quality_breakdown"))
    payload["audit"] = _as_dict(payload.get("audit"))
    return payload


def normalize_review_record(review: dict[str, Any]) -> dict[str, Any]:
    payload = clean_structure(dict(review))
    payload["progress_message"] = clean_text(payload.get("progress_message")) or ""
    payload["error_message"] = clean_text(payload.get("error_message"))
    payload["summary_json"] = normalize_review_summary_snapshot(payload.get("summary_json"))
    return payload


def normalize_review_artifact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    summary = normalize_review_summary_snapshot(payload.get("summary"))
    return {
        "summary": summary,
        "daily_results": _as_list(payload.get("daily_results")),
        "candidate_results": _as_list(payload.get("candidate_results")),
        "audit": _as_dict(payload.get("audit")) or _as_dict(summary.get("audit")),
    }


def normalize_structured_payload(value: Any) -> dict[str, Any]:
    payload = _as_dict(value)
    normalized: dict[str, Any] = {}
    for key in ("factor_snapshot", "evidence_snapshot", "structured_decision"):
        item = payload.get(key)
        normalized[key] = _as_dict(item) or None
    return normalized
