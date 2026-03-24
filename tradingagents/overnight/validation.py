from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from tradingagents.market_utils import build_security_profile

from .tail_data import load_history_frame


logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
    except TypeError:
        # Mixed object values can raise TypeError in pd.isna(); fall through to string coercion.
        logger.debug("Falling back to string coercion while normalizing validation numeric values.")
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _normalize_history(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    if "Date" not in normalized.columns:
        raise ValueError("History frame is missing the Date column.")
    normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce")
    return normalized.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)


def _evaluate_candidate_return(
    history: pd.DataFrame,
    trade_date: str,
) -> tuple[str, float | None, str | None, float | None]:
    normalized = _normalize_history(history)
    current_rows = normalized[normalized["Date"].dt.strftime("%Y-%m-%d") == trade_date]
    if current_rows.empty:
        return "unavailable", None, None, None

    current_index = current_rows.index[-1]
    current_close = _safe_float(normalized.loc[current_index].get("Close"))
    if current_close is None or current_close <= 0:
        return "unavailable", None, None, None

    if current_index + 1 >= len(normalized):
        return "pending", None, None, round(current_close, 4)

    next_row = normalized.loc[current_index + 1]
    next_open = _safe_float(next_row.get("Open"))
    next_date = next_row["Date"].strftime("%Y-%m-%d")
    if next_open is None or next_open <= 0:
        return "unavailable", None, next_date, round(current_close, 4)

    next_open_return = round((next_open - current_close) / current_close * 100, 4)
    return "validated", next_open_return, next_date, round(current_close, 4)


def validate_scan_candidates(
    *,
    trade_date: str,
    market_region: str,
    candidates: list[dict[str, Any]],
    data_dir: str | Path,
) -> dict[str, Any]:
    cache_root = Path(data_dir) / "overnight_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    validated_formal: list[dict[str, Any]] = []
    updated_candidates: list[dict[str, Any]] = []
    unavailable_count = 0
    pending_count = 0

    for candidate in candidates:
        updated = dict(candidate)
        bucket = candidate.get("bucket")
        if bucket != "formal":
            updated.setdefault("validation_status", "watchlist_only")
            updated_candidates.append(updated)
            continue

        try:
            profile = build_security_profile(candidate["ticker"], market_region)
            history = load_history_frame(profile, trade_date, cache_root)
        except Exception:
            history = None

        if history is None or history.empty:
            updated["validation_status"] = "unavailable"
            updated["next_open_return"] = None
            updated["next_open_date"] = None
            updated["scan_close_price"] = candidate.get("latest")
            unavailable_count += 1
            updated_candidates.append(updated)
            continue

        status, next_open_return, next_open_date, scan_close_price = _evaluate_candidate_return(
            history,
            trade_date,
        )
        updated["validation_status"] = status
        updated["next_open_return"] = next_open_return
        updated["next_open_date"] = next_open_date
        updated["scan_close_price"] = scan_close_price if scan_close_price is not None else candidate.get("latest")

        if status == "validated" and next_open_return is not None:
            validated_formal.append(updated)
        elif status == "pending":
            pending_count += 1
        else:
            unavailable_count += 1

        updated_candidates.append(updated)

    avg_next_open_return = None
    if validated_formal:
        avg_next_open_return = round(
            sum(item["next_open_return"] for item in validated_formal) / len(validated_formal),
            4,
        )

    best_candidate = None
    worst_candidate = None
    if validated_formal:
        best = max(validated_formal, key=lambda item: item["next_open_return"])
        worst = min(validated_formal, key=lambda item: item["next_open_return"])
        best_candidate = {
            "ticker": best["ticker"],
            "name": best["name"],
            "next_open_return": best["next_open_return"],
            "next_open_date": best["next_open_date"],
        }
        worst_candidate = {
            "ticker": worst["ticker"],
            "name": worst["name"],
            "next_open_return": worst["next_open_return"],
            "next_open_date": worst["next_open_date"],
        }

    if validated_formal:
        validation_status = "validated"
    elif pending_count:
        validation_status = "pending"
    elif unavailable_count:
        validation_status = "unavailable"
    else:
        validation_status = "empty"

    return {
        "candidates": updated_candidates,
        "summary": {
            "validated_formal_count": len(validated_formal),
            "avg_next_open_return": avg_next_open_return,
            "best_candidate": best_candidate,
            "worst_candidate": worst_candidate,
            "validation_status": validation_status,
            "validation_audit": {
                "validated_formal_count": len(validated_formal),
                "pending_count": pending_count,
                "unavailable_count": unavailable_count,
            },
        },
    }
