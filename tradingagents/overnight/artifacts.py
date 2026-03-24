from __future__ import annotations

from typing import Any

from .models import Candidate
from .universe import pool_label


def candidate_to_dict(candidate: Candidate) -> dict[str, Any]:
    snapshot = candidate.snapshot
    tail = candidate.tail_metrics
    return {
        "ticker": snapshot.code,
        "name": snapshot.name,
        "pool": pool_label(snapshot.pool),
        "quality": candidate.quality,
        "latest": round(snapshot.latest, 2),
        "pct": round(snapshot.pct, 2),
        "amount": round(snapshot.amount, 2),
        "turnover": round(snapshot.turnover, 2),
        "quick_score": round(candidate.quick_score, 1),
        "total_score": round(candidate.total_score, 1),
        "factor_breakdown": {
            key: round(value, 2) for key, value in candidate.factor_breakdown.items()
        },
        "selection_stage": candidate.selection_stage,
        "rejected_reason": candidate.rejected_reason,
        "tail_metrics": None
        if not tail
        else {
            "source": tail.source,
            "quality": tail.quality,
            "tail_return_pct": tail.tail_return_pct,
            "tail_amount_ratio": tail.tail_amount_ratio,
            "last10_return_pct": tail.last10_return_pct,
            "close_at_high_ratio": tail.close_at_high_ratio,
            "auction_strength": tail.auction_strength,
            "rows": tail.rows,
            "note": tail.note,
            "provider_chain": tail.provider_chain,
        },
        "filter_reason": candidate.filter_reason,
        "excluded_from_final": candidate.excluded_from_final,
    }


def preliminary_candidate_to_dict(candidate: Candidate) -> dict[str, Any]:
    payload = candidate_to_dict(candidate)
    payload["total_score"] = round(candidate.quick_score, 1)
    payload["factor_breakdown"] = {
        "quick_score": round(candidate.quick_score, 2),
    }
    payload["selection_stage"] = "preliminary"
    payload["tail_metrics"] = None
    payload["excluded_from_final"] = candidate.excluded_from_final or None
    return payload


def build_scan_result(
    summary: dict[str, Any],
    preliminary_candidates: list[Candidate],
    total_score_candidates: list[Candidate],
    formal_recommendations: list[Candidate],
    watchlist: list[Candidate],
    rejected_candidates: list[Candidate],
    excluded_examples: list[Candidate],
    audit: dict[str, Any],
) -> dict[str, Any]:
    return {
        "summary": summary,
        "preliminary_candidates": [preliminary_candidate_to_dict(item) for item in preliminary_candidates],
        "total_score_candidates": [candidate_to_dict(item) for item in total_score_candidates],
        "formal_recommendations": [candidate_to_dict(item) for item in formal_recommendations],
        "watchlist": [candidate_to_dict(item) for item in watchlist],
        "rejected_candidates": [candidate_to_dict(item) for item in rejected_candidates],
        "excluded_examples": [candidate_to_dict(item) for item in excluded_examples],
        "audit": audit,
    }
