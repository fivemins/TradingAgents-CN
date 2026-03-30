from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable

import akshare as ak
import pandas as pd

from tradingagents.market_utils import (
    build_security_profile,
    call_with_proxy_fallback,
    call_with_proxy_or_empty_fallback,
)

from .artifacts import candidate_to_dict
from .config import (
    OvernightEvaluationConfig,
    build_evaluation_config_payload,
    get_default_evaluation_config,
)
from .filters import check_buy_filters, load_risk_stocks
from .market_regime import evaluate_market_regime, load_index_snapshot
from .models import (
    Candidate,
    MarketRegime,
    OvernightMode,
    OvernightSnapshot,
    ReviewReturnBasis,
    ScanParams,
    normalize_review_return_basis,
)
from .scoring import calc_quick_score, calculate_total_score, split_quality_counts
from .tail_data import (
    load_history_frame,
    load_intraday_minute_frame,
    load_tail_metrics,
    normalize_intraday_minute_df,
    pick_intraday_price_near_time,
)
from .universe import (
    build_dynamic_pool_from_frame,
    classify_pool,
    load_market_spot_table,
    load_universe_snapshot,
)


ProgressCallback = Callable[[str], None]

DEFAULT_WINDOW_DAYS = 60
DEFAULT_RETURN_BASIS = "buy_1455_sell_next_day_1000"
ENTRY_TARGET_TIME = "14:55"
EXIT_TARGET_TIME = "10:00"
HS300_INDEX_SYMBOL = "000300"
logger = logging.getLogger(__name__)


def _find_column(frame: pd.DataFrame, keywords: list[str]) -> str | None:
    lowered = [keyword.lower() for keyword in keywords]
    for column in frame.columns:
        name = str(column).strip().lower()
        if any(keyword in name for keyword in lowered):
            return str(column)
    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
    except TypeError:
        # Mixed object values can raise TypeError in pd.isna(); fall through to string coercion.
        logger.debug("Falling back to string coercion while normalizing review numeric values.")
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _load_hs300_daily_frame() -> pd.DataFrame:
    frame = call_with_proxy_fallback(ak.stock_zh_index_daily, symbol="sh000300")
    if frame is None or frame.empty:
        raise RuntimeError("Unable to load HS300 trading calendar for overnight review.")

    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    date_col = _find_column(normalized, ["date", "鏃ユ湡"])
    if not date_col:
        raise RuntimeError("HS300 daily history is missing a date column.")

    normalized[date_col] = pd.to_datetime(normalized[date_col], errors="coerce")
    return normalized.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)


def _load_trade_dates(end_trade_date: str, window_days: int) -> list[str]:
    normalized = _load_hs300_daily_frame()
    date_col = _find_column(normalized, ["date", "日期"])
    matched = normalized[normalized[date_col] <= pd.Timestamp(end_trade_date)]
    if matched.empty:
        raise RuntimeError(f"No trading dates found on or before {end_trade_date}.")
    dates = matched[date_col].dt.strftime("%Y-%m-%d").tolist()
    return dates[-window_days:]


def _build_benchmark_map() -> dict[str, float | None]:
    try:
        normalized = _load_hs300_daily_frame()
    except RuntimeError:
        return {}
    date_col = _find_column(normalized, ["date", "日期"])
    open_col = _find_column(normalized, ["open", "开盘"])
    close_col = _find_column(normalized, ["close", "收盘"])
    if not date_col or not open_col or not close_col:
        return {}

    result: dict[str, float | None] = {}
    for index in range(len(normalized)):
        row = normalized.iloc[index]
        trade_date = row[date_col].strftime("%Y-%m-%d")
        close_price = _safe_float(row[close_col], default=0.0)
        next_open = None
        if index + 1 < len(normalized):
            next_open = _safe_float(normalized.iloc[index + 1][open_col], default=0.0)
        if close_price > 0 and next_open and next_open > 0:
            result[trade_date] = round((next_open - close_price) / close_price * 100, 4)
        else:
            result[trade_date] = None
    return result


def _build_next_trade_date_map(frame: pd.DataFrame) -> dict[str, str | None]:
    date_col = _find_column(frame, ["date", "鏃ユ湡"])
    if not date_col:
        return {}
    result: dict[str, str | None] = {}
    for index in range(len(frame)):
        trade_date = frame.iloc[index][date_col].strftime("%Y-%m-%d")
        next_trade_date = None
        if index + 1 < len(frame):
            next_trade_date = frame.iloc[index + 1][date_col].strftime("%Y-%m-%d")
        result[trade_date] = next_trade_date
    return result


def _index_minute_cache_path(cache_root: Path, trade_date: str) -> Path:
    directory = cache_root / "index_minute" / trade_date
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{HS300_INDEX_SYMBOL}.csv"


def _load_hs300_minute_frame(
    trade_date: str,
    cache_root: Path,
) -> tuple[pd.DataFrame, str]:
    cache_path = _index_minute_cache_path(cache_root, trade_date)
    if cache_path.exists():
        cached = pd.read_csv(cache_path)
        normalized = normalize_intraday_minute_df(cached)
        if not normalized.empty:
            return normalized, f"disk_cache:{trade_date}"

    start = f"{trade_date} 09:30:00"
    end = f"{trade_date} 15:05:00"
    frame = call_with_proxy_or_empty_fallback(
        ak.index_zh_a_hist_min_em,
        symbol=HS300_INDEX_SYMBOL,
        period="1",
        start_date=start,
        end_date=end,
    )
    normalized = normalize_intraday_minute_df(frame)
    if not normalized.empty:
        normalized.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return normalized, "akshare_index_minute"


def _build_intraday_benchmark_map(
    trade_dates: list[str],
    cache_root: Path,
) -> dict[str, dict[str, Any]]:
    daily = _load_hs300_daily_frame()
    next_trade_dates = _build_next_trade_date_map(daily)
    result: dict[str, dict[str, Any]] = {}
    for trade_date in trade_dates:
        next_trade_date = next_trade_dates.get(trade_date)
        if not next_trade_date:
            result[trade_date] = {
                "next_trade_date": None,
                "entry_price": None,
                "entry_time_used": None,
                "exit_price": None,
                "exit_time_used": None,
                "benchmark_return": None,
            }
            continue

        entry_frame, _entry_source = _load_hs300_minute_frame(trade_date, cache_root)
        exit_frame, _exit_source = _load_hs300_minute_frame(next_trade_date, cache_root)
        entry_price, entry_time_used = pick_intraday_price_near_time(
            entry_frame,
            trade_date,
            ENTRY_TARGET_TIME,
            prefer_on_tie="before",
        )
        exit_price, exit_time_used = pick_intraday_price_near_time(
            exit_frame,
            next_trade_date,
            EXIT_TARGET_TIME,
            prefer_on_tie="after",
        )
        benchmark_return = None
        if entry_price and entry_price > 0 and exit_price and exit_price > 0:
            benchmark_return = round((exit_price - entry_price) / entry_price * 100, 4)

        result[trade_date] = {
            "next_trade_date": next_trade_date,
            "entry_price": round(entry_price, 4) if entry_price else None,
            "entry_time_used": entry_time_used,
            "exit_price": round(exit_price, 4) if exit_price else None,
            "exit_time_used": exit_time_used,
            "benchmark_return": benchmark_return,
        }
    return result


def _build_universe_from_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    universe: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        code = str(row["code"]).zfill(6)
        profile = build_security_profile(code, "cn_a")
        universe.append(
            {
                "code": profile.normalized_ticker,
                "profile": profile,
                "name": str(row.get("name") or code),
                "pool": str(row.get("pool") or classify_pool(code)),
            }
        )
    return universe


def _build_review_universe(params: ScanParams) -> list[dict[str, Any]]:
    raw_spot = load_market_spot_table()
    if raw_spot.empty:
        raise RuntimeError("Unable to load the current A-share universe for overnight review.")

    pool_frame = build_dynamic_pool_from_frame(raw_spot, params)
    if pool_frame.empty:
        raise RuntimeError("The current A-share universe is empty after dynamic filtering.")
    return _build_universe_from_frame(pool_frame)


def _load_review_universe_for_date(
    trade_date: str,
    cache_root: Path,
    params: ScanParams,
) -> tuple[list[dict[str, Any]], str | None, list[str]]:
    snapshot = load_universe_snapshot(cache_root, trade_date)
    if not snapshot.empty:
        return _build_universe_from_frame(snapshot), trade_date, []

    snapshot_dir = cache_root / "universe"
    if snapshot_dir.exists():
        snapshot_dates = sorted(path.stem for path in snapshot_dir.glob("*.parquet"))
        preferred_dates = [date for date in snapshot_dates if date < trade_date]
        fallback_dates = list(reversed(preferred_dates)) + [
            date for date in reversed(snapshot_dates) if date not in preferred_dates
        ]
        for candidate_date in fallback_dates:
            if candidate_date == trade_date:
                continue
            candidate_snapshot = load_universe_snapshot(cache_root, candidate_date)
            if candidate_snapshot.empty:
                continue
            return (
                _build_universe_from_frame(candidate_snapshot),
                candidate_date,
                ["survivorship_bias", "saved_snapshot_fallback"],
            )

    return _build_review_universe(params), None, ["survivorship_bias", "live_universe_fallback"]


def _normalize_history(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    if "Date" in normalized.columns:
        normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce")
    return normalized.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)


def _limit_rate(snapshot_pool: str) -> float:
    return 0.2 if snapshot_pool in {"gem", "star"} else 0.1


def _build_snapshot_for_date(
    profile: Any,
    name: str,
    pool: str,
    history: pd.DataFrame,
    trade_date: str,
) -> OvernightSnapshot | None:
    if history.empty:
        return None

    normalized = _normalize_history(history)
    current_rows = normalized[normalized["Date"].dt.strftime("%Y-%m-%d") == trade_date]
    if current_rows.empty:
        return None

    current_index = current_rows.index[-1]
    if current_index <= 0:
        return None
    current_row = normalized.loc[current_index]
    previous_row = normalized.loc[current_index - 1]

    close_price = _safe_float(current_row.get("Close"), default=0.0)
    open_price = _safe_float(current_row.get("Open"), default=0.0)
    high_price = _safe_float(current_row.get("High"), default=0.0)
    low_price = _safe_float(current_row.get("Low"), default=0.0)
    pre_close = _safe_float(previous_row.get("Close"), default=0.0)

    if min(close_price, open_price, high_price, low_price, pre_close) <= 0:
        return None

    amount = _safe_float(current_row.get("Turnover"), default=0.0)
    if amount <= 0:
        volume = _safe_float(current_row.get("Volume"), default=0.0)
        amount = volume * close_price
    turnover = _safe_float(current_row.get("TurnoverRate"), default=0.0)

    pct = ((close_price - pre_close) / pre_close * 100) if pre_close > 0 else 0.0
    intraday_return = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0.0
    position = ((close_price - low_price) / (high_price - low_price) * 100) if high_price > low_price else 50.0
    dist_to_high = ((high_price - close_price) / high_price * 100) if high_price > 0 else 999.0
    amplitude = ((high_price - low_price) / low_price * 100) if low_price > 0 else 999.0
    limit_rate = _limit_rate(pool)
    upper_limit = round(pre_close * (1 + limit_rate), 4)
    dist_to_limit = ((upper_limit - close_price) / upper_limit * 100) if upper_limit > 0 else None

    return OvernightSnapshot(
        profile=profile,
        name=name,
        latest=round(close_price, 4),
        pre_close=round(pre_close, 4),
        open_price=round(open_price, 4),
        high=round(high_price, 4),
        low=round(low_price, 4),
        amount=round(amount, 4),
        turnover=round(turnover, 4),
        upper_limit=upper_limit,
        raw=current_row.to_dict(),
        pct=round(pct, 4),
        intraday_return_from_open=round(intraday_return, 4),
        position=round(position, 4),
        dist_to_high=round(dist_to_high, 4),
        amplitude=round(amplitude, 4),
        dist_to_limit=round(dist_to_limit, 4) if dist_to_limit is not None else None,
        pool=pool,  # type: ignore[arg-type]
    )


def _history_slice(history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if history.empty:
        return history
    normalized = _normalize_history(history)
    return normalized[normalized["Date"] <= pd.Timestamp(trade_date)].reset_index(drop=True)


def _next_open_return(history: pd.DataFrame, trade_date: str) -> tuple[float | None, str | None, float | None]:
    if history.empty:
        return None, None, None
    normalized = _normalize_history(history)
    current_rows = normalized[normalized["Date"].dt.strftime("%Y-%m-%d") == trade_date]
    if current_rows.empty:
        return None, None, None
    current_index = current_rows.index[-1]
    current_close = _safe_float(normalized.loc[current_index].get("Close"), default=0.0)
    if current_close <= 0 or current_index + 1 >= len(normalized):
        return None, None, current_close if current_close > 0 else None
    next_row = normalized.loc[current_index + 1]
    next_open = _safe_float(next_row.get("Open"), default=0.0)
    if next_open <= 0:
        return None, None, current_close
    next_date = next_row["Date"].strftime("%Y-%m-%d")
    return round((next_open - current_close) / current_close * 100, 4), next_date, current_close


def _next_trade_date_from_history(history: pd.DataFrame, trade_date: str) -> str | None:
    if history.empty:
        return None
    normalized = _normalize_history(history)
    current_rows = normalized[normalized["Date"].dt.strftime("%Y-%m-%d") == trade_date]
    if current_rows.empty:
        return None
    current_index = current_rows.index[-1]
    if current_index + 1 >= len(normalized):
        return None
    return normalized.loc[current_index + 1]["Date"].strftime("%Y-%m-%d")


def _evaluate_intraday_trade(
    profile: Any,
    trade_date: str,
    next_trade_date: str | None,
    cache_root: Path,
) -> dict[str, Any]:
    if not next_trade_date:
        return {
            "entry_price": None,
            "entry_time_used": None,
            "exit_price": None,
            "exit_time_used": None,
            "strategy_return": None,
            "counted_in_performance": False,
            "skipped_reason": "missing_next_trade_date",
        }

    entry_frame, _entry_source = load_intraday_minute_frame(profile, trade_date, cache_root)
    exit_frame, _exit_source = load_intraday_minute_frame(profile, next_trade_date, cache_root)
    entry_price, entry_time_used = pick_intraday_price_near_time(
        entry_frame,
        trade_date,
        ENTRY_TARGET_TIME,
        prefer_on_tie="before",
    )
    exit_price, exit_time_used = pick_intraday_price_near_time(
        exit_frame,
        next_trade_date,
        EXIT_TARGET_TIME,
        prefer_on_tie="after",
    )

    strategy_return = None
    skipped_reason = None
    counted_in_performance = False
    if entry_price is None or entry_price <= 0:
        skipped_reason = "missing_entry_price"
    elif exit_price is None or exit_price <= 0:
        skipped_reason = "missing_exit_price"
    else:
        strategy_return = round((exit_price - entry_price) / entry_price * 100, 4)
        counted_in_performance = True

    return {
        "entry_price": round(entry_price, 4) if entry_price else None,
        "entry_time_used": entry_time_used,
        "exit_price": round(exit_price, 4) if exit_price else None,
        "exit_time_used": exit_time_used,
        "strategy_return": strategy_return,
        "counted_in_performance": counted_in_performance,
        "skipped_reason": skipped_reason,
    }


def _build_watchlist(
    scored: list[Candidate],
    formal_codes: set[str],
    threshold: float,
    limit: int,
) -> list[Candidate]:
    return [
        item
        for item in scored
        if item.snapshot.code not in formal_codes and item.total_score >= threshold and item.quality == "real"
    ][:limit]


def _regime_label(regime: MarketRegime) -> str:
    if not regime.market_ok:
        return "risk_off"
    if regime.formal_threshold_delta > 0:
        return "cautious"
    return "normal"


def _aggregate_daily_breakdown(
    rows: list[dict[str, Any]],
    group_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        group = row.get(group_key)
        if not group:
            continue
        grouped.setdefault(str(group), []).append(row)

    breakdown: list[dict[str, Any]] = []
    for group, items in sorted(grouped.items()):
        returns = [
            float(item["strategy_return"] if item.get("strategy_return") is not None else item["equal_weight_next_open_return"])
            for item in items
            if item.get("strategy_return") is not None or item.get("equal_weight_next_open_return") is not None
        ]
        excess = [
            float(item["excess_return"] if item.get("excess_return") is not None else item["avg_excess_return"])
            for item in items
            if item.get("excess_return") is not None or item.get("avg_excess_return") is not None
        ]
        breakdown.append(
            {
                "group": group,
                "days_with_formal_picks": len(returns),
                "candidate_count": sum(
                    int(item.get("trade_count") or item.get("formal_count") or 0)
                    for item in items
                ),
                "avg_next_open_return": round(mean(returns), 4) if returns else None,
                "avg_excess_return": round(mean(excess), 4) if excess else None,
                "positive_pick_rate": (
                    round(sum(1 for value in returns if value > 0) / len(returns), 4)
                    if returns
                    else None
                ),
            }
        )
    return breakdown


def _aggregate_candidate_breakdown(
    rows: list[dict[str, Any]],
    group_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        group = row.get(group_key)
        if not group:
            continue
        grouped.setdefault(str(group), []).append(row)

    breakdown: list[dict[str, Any]] = []
    for group, items in sorted(grouped.items()):
        returns = [
            float(item["strategy_return"] if item.get("strategy_return") is not None else item["next_open_return"])
            for item in items
            if item.get("strategy_return") is not None or item.get("next_open_return") is not None
        ]
        excess = [
            float(item["excess_return"])
            for item in items
            if item.get("excess_return") is not None
        ]
        breakdown.append(
            {
                "group": group,
                "days_with_formal_picks": len({str(item.get("trade_date")) for item in items}),
                "candidate_count": len(items),
                "avg_next_open_return": round(mean(returns), 4) if returns else None,
                "avg_excess_return": round(mean(excess), 4) if excess else None,
                "positive_pick_rate": (
                    round(sum(1 for value in returns if value > 0) / len(returns), 4)
                    if returns
                    else None
                ),
            }
        )
    return breakdown


def run_overnight_review(
    end_trade_date: str,
    data_dir: str | Path,
    progress: ProgressCallback | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    mode: OvernightMode = "strict",
    return_basis: ReviewReturnBasis | None = None,
    evaluation_config: OvernightEvaluationConfig | None = None,
) -> dict[str, Any]:
    evaluation_config = evaluation_config or get_default_evaluation_config()
    active_mode = mode or evaluation_config.review_mode
    active_window_days = window_days or evaluation_config.review_window_days
    active_return_basis = normalize_review_return_basis(
        return_basis or evaluation_config.review_return_basis,
        default=normalize_review_return_basis(evaluation_config.review_return_basis),
    )
    if active_mode != "strict":
        raise ValueError("Overnight historical review only supports strict mode.")

    params = evaluation_config.review_scan_params
    evaluation_payload = build_evaluation_config_payload(evaluation_config)
    root = Path(data_dir)
    cache_root = root / "overnight_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    if progress:
        progress("Initializing overnight history review.")
    trade_dates = _load_trade_dates(end_trade_date, active_window_days)
    benchmark_map = (
        _build_intraday_benchmark_map(trade_dates, cache_root)
        if active_return_basis == "buy_1455_sell_next_day_1000"
        else _build_benchmark_map()
    )

    if progress:
        progress("Loading replay universe snapshots.")
    daily_universes: dict[str, list[dict[str, Any]]] = {}
    universe_snapshot_dates: dict[str, str | None] = {}
    bias_flags_by_day: dict[str, list[str]] = {}
    all_universe_items: dict[str, dict[str, Any]] = {}
    for trade_date in trade_dates:
        day_universe, snapshot_date, bias_flags = _load_review_universe_for_date(
            trade_date,
            cache_root,
            params,
        )
        daily_universes[trade_date] = day_universe
        universe_snapshot_dates[trade_date] = snapshot_date
        bias_flags_by_day[trade_date] = bias_flags
        for item in day_universe:
            all_universe_items[item["code"]] = item

    earliest_date = trade_dates[0]
    universe = list(all_universe_items.values())

    histories: dict[str, pd.DataFrame] = {}
    history_failures = 0
    for index, item in enumerate(universe, start=1):
        if progress and (index == 1 or index % 20 == 0 or index == len(universe)):
            progress(f"Loading historical bars for replay universe ({index}/{len(universe)}).")
        history = load_history_frame(item["profile"], end_trade_date, cache_root)
        if history is None or history.empty:
            history_failures += 1
            continue
        normalized = _normalize_history(history)
        if normalized[normalized["Date"] >= pd.Timestamp(earliest_date)].empty:
            history_failures += 1
            continue
        histories[item["code"]] = normalized

    if not histories:
        raise RuntimeError("Replay universe history could not be loaded for overnight review.")

    candidate_results: list[dict[str, Any]] = []
    daily_results: list[dict[str, Any]] = []
    all_pick_returns: list[float] = []
    daily_equal_weight_returns: list[float] = []
    daily_benchmark_returns: list[float] = []
    missing_next_open_count = 0
    missing_trade_price_count = 0
    missing_real_tail_count = 0
    days_with_formal_picks = 0
    days_with_trade = 0

    if progress:
        progress("Replaying historical overnight scans.")

    for day_index, trade_date in enumerate(trade_dates, start=1):
        if progress and (day_index == 1 or day_index % 5 == 0 or day_index == len(trade_dates)):
            progress(f"Replaying overnight scan for {trade_date} ({day_index}/{len(trade_dates)}).")

        index_snapshot = load_index_snapshot(trade_date)
        regime = evaluate_market_regime(index_snapshot.values, trade_date)
        risk_codes, _risk_summary = load_risk_stocks(trade_date, cache_root / "risk_review")

        snapshots: list[OvernightSnapshot] = []
        day_universe = daily_universes.get(trade_date, [])
        for item in day_universe:
            history = histories.get(item["code"])
            if history is None:
                continue
            snapshot = _build_snapshot_for_date(
                profile=item["profile"],
                name=item["name"],
                pool=item["pool"],
                history=history,
                trade_date=trade_date,
            )
            if snapshot:
                snapshots.append(snapshot)

        passed: list[Candidate] = []
        failed_count = 0
        for snapshot in snapshots:
            ok, reason = check_buy_filters(snapshot, risk_codes, params)
            candidate = Candidate(snapshot=snapshot, passed=ok, filter_reason=reason)
            if ok:
                candidate.quick_score = calc_quick_score(snapshot, regime.benchmark_pct)
                passed.append(candidate)
            else:
                failed_count += 1

        passed.sort(key=lambda item: item.quick_score, reverse=True)
        history_candidates = [cand for cand in passed if cand.quick_score >= params.quick_score_floor]

        for candidate in history_candidates:
            history = histories.get(candidate.snapshot.code)
            sliced = _history_slice(history, trade_date) if history is not None else pd.DataFrame()
            candidate.has_history = len(sliced) >= 20

        tail_candidates = sorted(
            [cand for cand in history_candidates if cand.has_history],
            key=lambda item: (
                item.quick_score,
                item.snapshot.amount / 1e8,
                item.snapshot.position,
                -item.snapshot.dist_to_high,
            ),
            reverse=True,
        )[: params.tail_fetch_limit]

        tails: dict[str, Any] = {}
        for candidate in tail_candidates:
            tails[candidate.snapshot.code] = load_tail_metrics(
                candidate.snapshot.profile,
                candidate.snapshot,
                trade_date,
                active_mode,
                cache_root,
                params.tail_start_time,
                params.tail_last_window_minutes,
            )

        scored: list[Candidate] = []
        for candidate in history_candidates:
            history = histories.get(candidate.snapshot.code)
            if history is None or not candidate.has_history:
                continue
            sliced = _history_slice(history, trade_date)
            candidate.tail_metrics = tails.get(candidate.snapshot.code)
            if candidate.tail_metrics is None or candidate.tail_metrics.quality != "real":
                missing_real_tail_count += 1
            total, breakdown = calculate_total_score(
                candidate.snapshot,
                sliced,
                candidate.tail_metrics,
                regime.benchmark_pct,
                risk_codes,
                params,
            )
            candidate.total_score = total
            candidate.factor_breakdown = breakdown
            scored.append(candidate)

        scored.sort(key=lambda item: item.total_score, reverse=True)
        day_returns: list[float] = []

        if active_return_basis == "next_open":
            formal_threshold = params.formal_score + regime.formal_threshold_delta
            formal_limit = params.formal_max_total
            if regime.formal_limit_cap is not None:
                formal_limit = min(formal_limit, regime.formal_limit_cap)

            formal_recommendations = [
                item
                for item in scored
                if item.total_score >= formal_threshold and item.has_real_tail
            ][:formal_limit]
            formal_codes = {item.snapshot.code for item in formal_recommendations}
            watch_threshold = params.watchlist_score if regime.market_ok else max(params.watchlist_score - 2.0, 0.0)
            watchlist = _build_watchlist(scored, formal_codes, watch_threshold, params.watchlist_max_total)

            if formal_recommendations:
                days_with_formal_picks += 1

            benchmark_return = benchmark_map.get(trade_date)
            for bucket, category in ((formal_recommendations, "formal"), (watchlist, "watchlist")):
                for candidate in bucket:
                    history = histories.get(candidate.snapshot.code)
                    next_open_return, next_trade_date, scan_close = _next_open_return(
                        history if history is not None else pd.DataFrame(),
                        trade_date,
                    )
                    evaluated = next_open_return is not None and category == "formal"
                    if category == "formal" and next_open_return is None:
                        missing_next_open_count += 1
                    if evaluated and next_open_return is not None:
                        day_returns.append(next_open_return)
                        all_pick_returns.append(next_open_return)
                    candidate_results.append(
                        {
                            "trade_date": trade_date,
                            "category": category,
                            "pool": candidate.snapshot.pool,
                            "ticker": candidate.snapshot.code,
                            "name": candidate.snapshot.name,
                            "quality": candidate.quality,
                            "quick_score": round(candidate.quick_score, 1),
                            "total_score": round(candidate.total_score, 1),
                            "factor_breakdown": {
                                key: round(value, 2) for key, value in candidate.factor_breakdown.items()
                            },
                            "tail_metrics": candidate_to_dict(candidate).get("tail_metrics"),
                            "filter_reason": candidate.filter_reason,
                            "next_trade_date": next_trade_date,
                            "scan_close_price": round(scan_close, 4) if scan_close else None,
                            "next_open_return": next_open_return,
                            "benchmark_next_open_return": benchmark_return,
                            "excess_return": (
                                round(next_open_return - benchmark_return, 4)
                                if next_open_return is not None and benchmark_return is not None
                                else None
                            ),
                            "counted_in_performance": evaluated,
                        }
                    )

            daily_return = round(mean(day_returns), 4) if day_returns else None
            if daily_return is not None:
                daily_equal_weight_returns.append(daily_return)
            if benchmark_return is not None and daily_return is not None:
                daily_benchmark_returns.append(benchmark_return)

            daily_results.append(
                {
                    "trade_date": trade_date,
                    "market_regime": _regime_label(regime),
                    "formal_count": len(formal_recommendations),
                    "watchlist_count": len(watchlist),
                    "formal_tickers": [item.snapshot.code for item in formal_recommendations][:10],
                    "market_message": regime.market_message,
                    "benchmark_next_open_return": benchmark_return,
                    "equal_weight_next_open_return": daily_return,
                    "avg_excess_return": (
                        round(daily_return - benchmark_return, 4)
                        if daily_return is not None and benchmark_return is not None
                        else None
                    ),
                    "tail_quality_counts": split_quality_counts(scored),
                    "passed_filters": len(passed),
                    "failed_filters": failed_count,
                    "bias_flags": bias_flags_by_day.get(trade_date, []),
                    "universe_snapshot_date": universe_snapshot_dates.get(trade_date),
                }
            )
        else:
            selected = scored[0] if scored else None
            benchmark_payload = benchmark_map.get(trade_date, {})
            benchmark_return = benchmark_payload.get("benchmark_return")
            daily_return = None
            if selected is not None:
                history = histories.get(selected.snapshot.code)
                next_trade_date = _next_trade_date_from_history(
                    history if history is not None else pd.DataFrame(),
                    trade_date,
                )
                trade_eval = _evaluate_intraday_trade(
                    selected.snapshot.profile,
                    trade_date,
                    next_trade_date,
                    cache_root,
                )
                if not trade_eval["counted_in_performance"]:
                    missing_trade_price_count += 1

                excess_return = (
                    round(trade_eval["strategy_return"] - benchmark_return, 4)
                    if trade_eval["strategy_return"] is not None and benchmark_return is not None
                    else None
                )
                if trade_eval["counted_in_performance"] and trade_eval["strategy_return"] is not None:
                    days_with_trade += 1
                    day_returns.append(trade_eval["strategy_return"])
                    all_pick_returns.append(trade_eval["strategy_return"])
                    daily_equal_weight_returns.append(trade_eval["strategy_return"])
                    daily_return = trade_eval["strategy_return"]
                    if benchmark_return is not None:
                        daily_benchmark_returns.append(benchmark_return)

                candidate_results.append(
                    {
                        "trade_date": trade_date,
                        "category": "selected",
                        "pool": selected.snapshot.pool,
                        "ticker": selected.snapshot.code,
                        "name": selected.snapshot.name,
                        "quality": selected.quality,
                        "quick_score": round(selected.quick_score, 1),
                        "total_score": round(selected.total_score, 1),
                        "factor_breakdown": {
                            key: round(value, 2) for key, value in selected.factor_breakdown.items()
                        },
                        "tail_metrics": candidate_to_dict(selected).get("tail_metrics"),
                        "filter_reason": selected.filter_reason,
                        "entry_target_time": ENTRY_TARGET_TIME,
                        "entry_time_used": trade_eval["entry_time_used"],
                        "entry_price": trade_eval["entry_price"],
                        "exit_target_time": EXIT_TARGET_TIME,
                        "next_trade_date": next_trade_date,
                        "exit_time_used": trade_eval["exit_time_used"],
                        "exit_price": trade_eval["exit_price"],
                        "strategy_return": trade_eval["strategy_return"],
                        "benchmark_return": benchmark_return,
                        "excess_return": excess_return,
                        "counted_in_performance": trade_eval["counted_in_performance"],
                        "skipped_reason": trade_eval["skipped_reason"],
                    }
                )

            daily_results.append(
                {
                    "trade_date": trade_date,
                    "market_regime": _regime_label(regime),
                    "trade_count": 1 if daily_return is not None else 0,
                    "selected_ticker": selected.snapshot.code if selected else None,
                    "selected_name": selected.snapshot.name if selected else None,
                    "selected_pool": selected.snapshot.pool if selected else None,
                    "selected_quality": selected.quality if selected else None,
                    "selected_total_score": round(selected.total_score, 1) if selected else None,
                    "market_message": regime.market_message,
                    "entry_target_time": ENTRY_TARGET_TIME,
                    "entry_time_used": candidate_results[-1]["entry_time_used"] if selected else None,
                    "entry_price": candidate_results[-1]["entry_price"] if selected else None,
                    "exit_target_time": EXIT_TARGET_TIME,
                    "exit_trade_date": candidate_results[-1]["next_trade_date"] if selected else None,
                    "exit_time_used": candidate_results[-1]["exit_time_used"] if selected else None,
                    "exit_price": candidate_results[-1]["exit_price"] if selected else None,
                    "strategy_return": daily_return,
                    "benchmark_return": benchmark_return,
                    "excess_return": (
                        round(daily_return - benchmark_return, 4)
                        if daily_return is not None and benchmark_return is not None
                        else None
                    ),
                    "tail_quality_counts": split_quality_counts(scored),
                    "passed_filters": len(passed),
                    "failed_filters": failed_count,
                    "bias_flags": bias_flags_by_day.get(trade_date, []),
                    "universe_snapshot_date": universe_snapshot_dates.get(trade_date),
                    "counted_in_performance": daily_return is not None,
                    "formal_count": 1 if daily_return is not None else 0,
                    "watchlist_count": 0,
                    "formal_tickers": [selected.snapshot.code] if selected and daily_return is not None else [],
                    "benchmark_next_open_return": benchmark_return,
                    "equal_weight_next_open_return": daily_return,
                    "avg_excess_return": (
                        round(daily_return - benchmark_return, 4)
                        if daily_return is not None and benchmark_return is not None
                        else None
                    ),
                }
            )

    excess_returns = [
        day["excess_return"] if day.get("excess_return") is not None else day["avg_excess_return"]
        for day in daily_results
        if day.get("excess_return") is not None or day.get("avg_excess_return") is not None
    ]
    valid_daily_results = [
        day
        for day in daily_results
        if day.get("strategy_return") is not None or day.get("equal_weight_next_open_return") is not None
    ]
    best_day = None
    worst_day = None
    if valid_daily_results:
        best_day = max(
            valid_daily_results,
            key=lambda item: item.get("strategy_return", item.get("equal_weight_next_open_return")),
        )
        worst_day = min(
            valid_daily_results,
            key=lambda item: item.get("strategy_return", item.get("equal_weight_next_open_return")),
        )

    survivorship_bias = any(
        "survivorship_bias" in bias_flags
        for bias_flags in bias_flags_by_day.values()
    )
    counted_candidate_results = [
        item
        for item in candidate_results
        if item.get("counted_in_performance")
    ]
    trade_count = len(counted_candidate_results)
    days_with_trade = days_with_trade if active_return_basis != "next_open" else days_with_formal_picks
    avg_strategy_return = round(mean(all_pick_returns), 4) if all_pick_returns else None
    median_strategy_return = round(median(all_pick_returns), 4) if all_pick_returns else None
    avg_daily_strategy_return = (
        round(mean(daily_equal_weight_returns), 4) if daily_equal_weight_returns else None
    )
    avg_benchmark_return = (
        round(mean(daily_benchmark_returns), 4) if daily_benchmark_returns else None
    )
    best_day_return = (
        best_day.get("strategy_return")
        if best_day and best_day.get("strategy_return") is not None
        else (best_day.get("equal_weight_next_open_return") if best_day else None)
    )
    worst_day_return = (
        worst_day.get("strategy_return")
        if worst_day and worst_day.get("strategy_return") is not None
        else (worst_day.get("equal_weight_next_open_return") if worst_day else None)
    )
    best_day_benchmark = (
        best_day.get("benchmark_return")
        if best_day and best_day.get("benchmark_return") is not None
        else (best_day.get("benchmark_next_open_return") if best_day else None)
    )
    worst_day_benchmark = (
        worst_day.get("benchmark_return")
        if worst_day and worst_day.get("benchmark_return") is not None
        else (worst_day.get("benchmark_next_open_return") if worst_day else None)
    )
    best_day_ticker = (
        best_day.get("selected_ticker")
        if best_day and best_day.get("selected_ticker")
        else ((best_day.get("formal_tickers") or [None])[0] if best_day else None)
    )
    worst_day_ticker = (
        worst_day.get("selected_ticker")
        if worst_day and worst_day.get("selected_ticker")
        else ((worst_day.get("formal_tickers") or [None])[0] if worst_day else None)
    )
    summary = {
        "end_trade_date": end_trade_date,
        "market_region": "cn_a",
        "window_days": active_window_days,
        "mode": active_mode,
        "return_basis": active_return_basis,
        "trade_count": trade_count,
        "days_with_trade": days_with_trade,
        "avg_strategy_return": avg_strategy_return,
        "median_strategy_return": median_strategy_return,
        "avg_daily_strategy_return": avg_daily_strategy_return,
        "avg_benchmark_return": avg_benchmark_return,
        "candidate_count": trade_count,
        "days_evaluated": len(trade_dates),
        "days_with_formal_picks": days_with_trade,
        "avg_next_open_return": avg_strategy_return,
        "median_next_open_return": median_strategy_return,
        "positive_pick_rate": (
            round(sum(1 for value in all_pick_returns if value > 0) / len(all_pick_returns), 4)
            if all_pick_returns
            else None
        ),
        "avg_daily_equal_weight_return": avg_daily_strategy_return,
        "avg_benchmark_next_open_return": avg_benchmark_return,
        "avg_excess_return": round(mean(excess_returns), 4) if excess_returns else None,
        "best_day": {
            "trade_date": best_day["trade_date"],
            "strategy_return": best_day_return,
            "benchmark_return": best_day_benchmark,
            "excess_return": (
                best_day.get("excess_return")
                if best_day.get("excess_return") is not None
                else best_day.get("avg_excess_return")
            ),
            "selected_ticker": best_day_ticker,
            "equal_weight_next_open_return": best_day_return,
            "benchmark_next_open_return": best_day_benchmark,
            "avg_excess_return": (
                best_day.get("excess_return")
                if best_day.get("excess_return") is not None
                else best_day.get("avg_excess_return")
            ),
            "formal_tickers": [best_day_ticker] if best_day_ticker else [],
        }
        if best_day
        else None,
        "worst_day": {
            "trade_date": worst_day["trade_date"],
            "strategy_return": worst_day_return,
            "benchmark_return": worst_day_benchmark,
            "excess_return": (
                worst_day.get("excess_return")
                if worst_day.get("excess_return") is not None
                else worst_day.get("avg_excess_return")
            ),
            "selected_ticker": worst_day_ticker,
            "equal_weight_next_open_return": worst_day_return,
            "benchmark_next_open_return": worst_day_benchmark,
            "avg_excess_return": (
                worst_day.get("excess_return")
                if worst_day.get("excess_return") is not None
                else worst_day.get("avg_excess_return")
            ),
            "formal_tickers": [worst_day_ticker] if worst_day_ticker else [],
        }
        if worst_day
        else None,
        "has_valid_samples": bool(all_pick_returns),
        "headline_message": (
            "No valid intraday top-pick samples were produced in the selected window."
            if not all_pick_returns and active_return_basis == "buy_1455_sell_next_day_1000"
            else (
                "No valid formal recommendation samples were produced in the selected window."
                if not all_pick_returns
                else "Historical overnight review completed successfully."
            )
        ),
        "regime_breakdown": _aggregate_daily_breakdown(
            [
                item
                for item in daily_results
                if item.get("strategy_return") is not None or item.get("equal_weight_next_open_return") is not None
            ],
            "market_regime",
        ),
        "pool_breakdown": _aggregate_candidate_breakdown(
            counted_candidate_results,
            "pool",
        ),
        "tail_quality_breakdown": _aggregate_candidate_breakdown(
            counted_candidate_results,
            "quality",
        ),
        "data_quality": {
            "status": "incomplete" if survivorship_bias else "ok",
            "message": (
                "Historical review used fallback universe snapshots or current live universe for dates without an exact saved snapshot."
                if survivorship_bias
                else "Historical review used saved universe snapshots for every replay day."
            ),
        },
        "provider_route": {
            "spot": "akshare_spot",
            "index": (
                "akshare_index_minute"
                if active_return_basis == "buy_1455_sell_next_day_1000"
                else "akshare_index"
            ),
            "history": "akshare_daily",
            "tail": "akshare_minute",
            "risk": "akshare_news",
        },
        "bias_flags": ["survivorship_bias"] if survivorship_bias else [],
        "universe_snapshot_date": max(
            (snapshot_date for snapshot_date in universe_snapshot_dates.values() if snapshot_date),
            default=None,
        ),
        "survivorship_bias": survivorship_bias,
        "evaluation_config_version": evaluation_payload["version"],
        "evaluation_config_hash": evaluation_payload["short_hash"],
    }
    audit = {
        "mode": active_mode,
        "return_basis": active_return_basis,
        "window_days": active_window_days,
        "universe_count": len(universe),
        "history_loaded": len(histories),
        "history_failed": history_failures,
        "missing_real_tail_count": missing_real_tail_count,
        "missing_next_open_count": missing_next_open_count,
        "missing_trade_price_count": missing_trade_price_count,
        "survivorship_bias": survivorship_bias,
        "provider_route": summary["provider_route"],
        "universe_snapshot_dates": universe_snapshot_dates,
        "notes": [
            "historical_review_prefers_saved_universe_snapshots",
            "Survivorship bias exists for dates without exact saved universe snapshots, because those dates fall back to the nearest available saved snapshot or the currently available active A-share list."
            if survivorship_bias
            else "All replay dates used saved universe snapshots.",
            (
                "Review basis uses same-day 14:55 entry and next-trading-day 10:00 exit on the highest-scored candidate."
                if active_return_basis == "buy_1455_sell_next_day_1000"
                else "Strict mode only counts formal recommendations with real tail minute data."
            ),
            (
                "Candidates without usable minute prices near 14:55 or 10:00 are excluded from return statistics."
                if active_return_basis == "buy_1455_sell_next_day_1000"
                else "Candidates without next-trading-day open data are excluded from return statistics."
            ),
        ],
        "evaluation_config_version": evaluation_payload["version"],
        "evaluation_config_hash": evaluation_payload["short_hash"],
    }
    return {
        "summary": summary,
        "daily_results": daily_results,
        "candidate_results": candidate_results,
        "audit": audit,
    }
