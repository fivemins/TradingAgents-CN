from __future__ import annotations

import math

import pandas as pd

from .models import Candidate, OvernightSnapshot, ScanParams, TailMetrics


def _clamp(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def calc_quick_score(snapshot: OvernightSnapshot, benchmark_pct: float = 0.0) -> float:
    score = 0.0
    if 0.5 <= snapshot.pct <= 2.5:
        score += 28.0
    elif 0 < snapshot.pct <= 4.0:
        score += 12.0
    else:
        score += 4.0

    if snapshot.position >= 85:
        score += 22.0
    elif snapshot.position >= 70:
        score += 16.0
    elif snapshot.position >= 55:
        score += 9.0

    if snapshot.dist_to_high <= 0.6:
        score += 10.0
    elif snapshot.dist_to_high <= 1.2:
        score += 7.0

    if snapshot.latest > snapshot.open_price:
        score += 10.0
    if 0.4 <= snapshot.intraday_return_from_open <= 2.5:
        score += 8.0

    if snapshot.amount >= 20e8:
        score += 10.0
    elif snapshot.amount >= 10e8:
        score += 8.0
    elif snapshot.amount >= 5e8:
        score += 6.0

    if snapshot.is_main:
        if 3 <= snapshot.turnover <= 12:
            score += 7.0
        elif 1 <= snapshot.turnover < 3:
            score += 4.0
    else:
        if 5 <= snapshot.turnover <= 18:
            score += 7.0
        elif 2 <= snapshot.turnover < 5:
            score += 4.0

    excess = snapshot.pct - benchmark_pct
    if excess > 1.0:
        score += 8.0
    elif excess > 0.3:
        score += 5.0
    elif excess > -0.2:
        score += 2.0
    return round(_clamp(score), 1)


def calc_trend_score(history: pd.DataFrame) -> float:
    if history is None or history.empty or "Close" not in history.columns:
        return 0.0

    frame = history.copy()
    frame["Close"] = pd.to_numeric(frame["Close"], errors="coerce")
    frame = frame.dropna(subset=["Close"])
    if len(frame) < 20:
        return 0.0

    close = frame["Close"]
    score = 0.0
    ma5 = float(close.tail(5).mean()) if len(close) >= 5 else 0.0
    ma10 = float(close.tail(10).mean()) if len(close) >= 10 else 0.0
    ma20 = float(close.tail(20).mean())
    last_close = float(close.iloc[-1])

    if last_close > ma5 > ma10 > ma20 > 0:
        score += 8.5
    elif last_close > ma5 > ma10 > 0:
        score += 6.0
    elif last_close > ma5 > 0:
        score += 3.0

    if len(close) >= 4:
        ret_3d = (close.iloc[-1] / close.iloc[-4] - 1) * 100
        if ret_3d > 1.0:
            score += 4.0
        elif ret_3d >= -0.5:
            score += 2.0
    if len(close) >= 6:
        ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) * 100
        if ret_5d > 1.5:
            score += 4.5
        elif ret_5d >= -0.5:
            score += 2.0

    if len(close) >= 20:
        low_20 = float(close.tail(20).min())
        high_20 = float(close.tail(20).max())
        if high_20 > low_20:
            pos20 = (last_close - low_20) / (high_20 - low_20) * 100
            if pos20 >= 80:
                score += 5.0
            elif pos20 >= 60:
                score += 3.0
            elif pos20 >= 40:
                score += 1.5

    return round(min(score, 25.0), 2)


def calc_tail_strength_score(tail: TailMetrics | None) -> float:
    if not tail or not tail.has_real_tail_data:
        return 0.0

    score = 0.0
    if 0.35 <= tail.tail_return_pct <= 1.60:
        score += 9.0
    elif 0.15 <= tail.tail_return_pct < 0.35:
        score += 5.5
    elif 1.60 < tail.tail_return_pct <= 2.40:
        score += 6.5
    elif tail.tail_return_pct > 2.40:
        score += 3.0

    if tail.tail_amount_ratio >= 0.24:
        score += 6.0
    elif tail.tail_amount_ratio >= 0.18:
        score += 4.5
    elif tail.tail_amount_ratio >= 0.12:
        score += 2.5

    if tail.last10_return_pct >= 0.28:
        score += 4.0
    elif tail.last10_return_pct >= 0.12:
        score += 2.5
    elif tail.last10_return_pct >= 0.05:
        score += 1.0

    if tail.close_at_high_ratio >= 0.88:
        score += 1.5
    elif tail.close_at_high_ratio >= 0.75:
        score += 0.8

    if tail.auction_strength >= 0.06:
        score += 1.5
    elif tail.auction_strength >= 0.02:
        score += 0.8
    return round(min(score, 22.0), 2)


def calc_capital_score(snapshot: OvernightSnapshot) -> float:
    score = 0.0
    if snapshot.amount >= 20e8:
        score += 6.0
    elif snapshot.amount >= 10e8:
        score += 4.5
    elif snapshot.amount >= 5e8:
        score += 3.0

    if snapshot.is_main:
        if 3 <= snapshot.turnover <= 12:
            score += 5.5
        elif 1 <= snapshot.turnover < 3:
            score += 3.0
    else:
        if 5 <= snapshot.turnover <= 18:
            score += 5.5
        elif 2 <= snapshot.turnover < 5:
            score += 3.0

    if snapshot.pct > 0 and snapshot.amount >= 5e8:
        score += 3.5
    return round(min(score, 15.0), 2)


def calc_relative_strength_score(snapshot: OvernightSnapshot, benchmark_pct: float) -> float:
    excess = snapshot.pct - benchmark_pct
    score = 0.0
    if excess > 1.5:
        score += 8.0
    elif excess > 0.8:
        score += 6.0
    elif excess > 0.2:
        score += 4.0
    elif excess > -0.3:
        score += 2.0
    return round(min(score, 12.0), 2)


def calc_volatility_control_score(snapshot: OvernightSnapshot, params: ScanParams) -> float:
    score = 0.0
    max_amp = params.max_amplitude_main if snapshot.is_main else params.max_amplitude_gem
    if snapshot.amplitude <= max_amp * 0.7:
        score += 4.5
    elif snapshot.amplitude <= max_amp:
        score += 3.0
    if snapshot.dist_to_limit is None or snapshot.dist_to_limit > 5.0:
        score += 3.0
    elif snapshot.dist_to_limit > 2.5:
        score += 1.5
    return round(min(score, 10.0), 2)


def calc_liquidity_score(snapshot: OvernightSnapshot) -> float:
    if snapshot.amount >= 20e8:
        return 8.0
    if snapshot.amount >= 10e8:
        return 6.0
    if snapshot.amount >= 5e8:
        return 4.0
    if snapshot.amount >= 2e8:
        return 2.5
    if snapshot.amount >= 1e8:
        return 1.0
    return 0.0


def calc_event_risk_penalty(snapshot: OvernightSnapshot, risk_stocks: set[str]) -> float:
    return 5.0 if snapshot.code in risk_stocks else 0.0


def calculate_total_score(
    snapshot: OvernightSnapshot,
    history: pd.DataFrame,
    tail: TailMetrics | None,
    benchmark_pct: float,
    risk_stocks: set[str],
    params: ScanParams,
) -> tuple[float, dict[str, float]]:
    trend = calc_trend_score(history)
    tail_score = calc_tail_strength_score(tail)
    capital = calc_capital_score(snapshot)
    relative_strength = calc_relative_strength_score(snapshot, benchmark_pct)
    volatility = calc_volatility_control_score(snapshot, params)
    liquidity = calc_liquidity_score(snapshot)
    event_penalty = calc_event_risk_penalty(snapshot, risk_stocks)
    total = trend + tail_score + capital + relative_strength + volatility + liquidity - event_penalty
    return round(total, 1), {
        "trend_strength": trend,
        "tail_strength": tail_score,
        "capital_behavior": capital,
        "relative_strength": relative_strength,
        "volatility_control": volatility,
        "liquidity": liquidity,
        "event_risk_penalty": -event_penalty,
    }


def pick_history_enrichment_list(candidates: list[Candidate], params: ScanParams) -> list[str]:
    qualified = [cand for cand in candidates if cand.quick_score >= params.quick_score_floor]
    qualified.sort(key=lambda item: item.quick_score, reverse=True)
    limit = min(params.history_fetch_limit, max(params.formal_max_total * 2, 8))
    return [cand.snapshot.code for cand in qualified[:limit]]


def pick_tail_enrichment_list(candidates: list[Candidate], params: ScanParams) -> list[str]:
    eligible = [cand for cand in candidates if cand.has_history and cand.quick_score >= params.quick_score_floor]
    eligible.sort(
        key=lambda item: (
            item.quick_score,
            item.snapshot.amount / 1e8,
            item.snapshot.position,
            -item.snapshot.dist_to_high,
        ),
        reverse=True,
    )
    return [cand.snapshot.code for cand in eligible[: params.tail_fetch_limit]]


def split_quality_counts(candidates: list[Candidate]) -> dict[str, int]:
    return {
        "real": sum(1 for item in candidates if item.quality == "real"),
        "proxy": sum(1 for item in candidates if item.quality == "proxy"),
        "missing": sum(1 for item in candidates if item.quality in {"missing", "invalid"}),
    }
