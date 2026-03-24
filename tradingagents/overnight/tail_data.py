from __future__ import annotations

from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

from tradingagents.dataflows.a_share_support import get_a_share_history
from tradingagents.market_utils import (
    SecurityProfile,
    call_with_proxy_fallback,
    call_with_proxy_or_empty_fallback,
)
from tradingagents.qveris import QVerisClient, QVerisToolRegistry, QVerisUsageTracker
from tradingagents.qveris.client import QVerisClientError

from .models import OvernightMode, OvernightSnapshot, TailMetrics


logger = logging.getLogger(__name__)


def _normalize_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    return normalized


def _find_column(frame: pd.DataFrame, keywords: list[str]) -> str | None:
    lowered = [keyword.lower() for keyword in keywords]
    for column in frame.columns:
        name = str(column).strip().lower()
        if any(keyword in name for keyword in lowered):
            return str(column)
    return None


def _history_cache_path(cache_root: Path, profile: SecurityProfile, trade_date: str) -> Path:
    directory = cache_root / "history" / trade_date
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{profile.display_symbol}.csv"


def _tail_cache_path(cache_root: Path, profile: SecurityProfile, trade_date: str) -> Path:
    directory = cache_root / "minute" / trade_date
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{profile.display_symbol}.csv"


def load_history_frame(
    profile: SecurityProfile,
    trade_date: str,
    cache_root: Path,
) -> pd.DataFrame:
    cache_path = _history_cache_path(cache_root, profile, trade_date)
    if cache_path.exists():
        cached = pd.read_csv(cache_path)
        return _normalize_frame(cached)

    end_date = datetime.strptime(trade_date, "%Y-%m-%d")
    start_date = (end_date - timedelta(days=180)).strftime("%Y-%m-%d")
    inclusive_end = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
    history = get_a_share_history(profile, start_date, inclusive_end)
    if history.empty:
        return pd.DataFrame()
    history.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return _normalize_frame(history)


def _normalize_intraday_df(frame: pd.DataFrame | None) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return pd.DataFrame()

    time_col = _find_column(normalized, ["时间", "日期", "datetime", "dt", "day"])
    open_col = _find_column(normalized, ["开盘", "open"])
    close_col = _find_column(normalized, ["收盘", "close", "最新价", "price"])
    high_col = _find_column(normalized, ["最高", "high"])
    low_col = _find_column(normalized, ["最低", "low"])
    volume_col = _find_column(normalized, ["成交量", "volume"])
    amount_col = _find_column(normalized, ["成交额", "amount"])

    if not time_col or not close_col:
        return pd.DataFrame()

    working = pd.DataFrame(
        {
            "dt": pd.to_datetime(normalized[time_col], errors="coerce"),
            "open": pd.to_numeric(normalized[open_col], errors="coerce") if open_col else pd.NA,
            "price": pd.to_numeric(normalized[close_col], errors="coerce"),
            "high_p": pd.to_numeric(normalized[high_col], errors="coerce") if high_col else pd.NA,
            "low_p": pd.to_numeric(normalized[low_col], errors="coerce") if low_col else pd.NA,
            "volume": pd.to_numeric(normalized[volume_col], errors="coerce") if volume_col else 0.0,
            "amount": pd.to_numeric(normalized[amount_col], errors="coerce") if amount_col else 0.0,
        }
    ).dropna(subset=["dt", "price"])
    if working.empty:
        return pd.DataFrame()

    if working["high_p"].isna().all():
        working["high_p"] = working["price"]
    else:
        working["high_p"] = working["high_p"].fillna(working["price"])
    if working["low_p"].isna().all():
        working["low_p"] = working["price"]
    else:
        working["low_p"] = working["low_p"].fillna(working["price"])
    if "open" in working:
        working["open"] = working["open"].fillna(working["price"])
    working["amount"] = pd.to_numeric(working["amount"], errors="coerce").fillna(0.0)
    return working.sort_values("dt").reset_index(drop=True)


def _load_intraday_minute_df(
    profile: SecurityProfile,
    trade_date: str,
) -> tuple[pd.DataFrame, str]:
    prefixed_symbol = (
        f"sh{profile.akshare_symbol}" if profile.exchange == "SSE" else f"sz{profile.akshare_symbol}"
    )
    start = f"{trade_date} 09:30:00"
    end = f"{trade_date} 15:05:00"
    try:
        frame = call_with_proxy_or_empty_fallback(
            ak.stock_zh_a_hist_min_em,
            symbol=profile.akshare_symbol,
            start_date=start,
            end_date=end,
            period="1",
            adjust="",
        )
        normalized = _normalize_intraday_df(frame)
        if not normalized.empty:
            return normalized, "akshare_minute"
    except Exception as exc:
        logger.debug("A-share minute source akshare_minute failed: %s", exc)

    try:
        frame = call_with_proxy_or_empty_fallback(
            ak.stock_zh_a_minute,
            symbol=prefixed_symbol,
            period="1",
            adjust="qfq",
        )
        normalized = _normalize_intraday_df(frame)
        if not normalized.empty:
            return normalized, "akshare_minute_legacy"
    except Exception as exc:
        logger.debug("A-share minute source akshare_minute_legacy failed: %s", exc)

    today = datetime.now().strftime("%Y-%m-%d")
    if trade_date == today:
        try:
            frame = call_with_proxy_or_empty_fallback(
                ak.stock_intraday_em,
                symbol=profile.akshare_symbol,
            )
            normalized = _normalize_intraday_df(frame)
            if not normalized.empty:
                return normalized, "akshare_intraday"
        except Exception as exc:
            logger.debug("A-share minute source akshare_intraday failed: %s", exc)
        try:
            frame = call_with_proxy_or_empty_fallback(
                ak.stock_intraday_sina,
                symbol=prefixed_symbol,
            )
            normalized = _normalize_intraday_df(frame)
            if not normalized.empty:
                return normalized, "akshare_intraday_sina"
        except Exception as exc:
            logger.debug("A-share minute source akshare_intraday_sina failed: %s", exc)
    return pd.DataFrame(), "no_tail_source"


def _qveris_symbol(profile: SecurityProfile) -> str:
    suffix = "SH" if profile.exchange == "SSE" else "SZ"
    return f"{profile.akshare_symbol}.{suffix}"


def _extract_qveris_minute_frames(response: dict[str, Any]) -> dict[str, pd.DataFrame]:
    result = (response.get("result") or {}).get("data")
    grouped: dict[str, list[dict[str, Any]]] = {}
    if isinstance(result, dict):
        for key, value in result.items():
            if isinstance(value, list):
                grouped[str(key)] = [item for item in value if isinstance(item, dict)]
    elif isinstance(result, list):
        for group in result:
            if isinstance(group, list):
                rows = [item for item in group if isinstance(item, dict)]
                for row in rows:
                    symbol = str(
                        row.get("thscode")
                        or row.get("code")
                        or row.get("symbol")
                        or ""
                    ).upper()
                    if not symbol:
                        continue
                    grouped.setdefault(symbol, []).append(row)
            elif isinstance(group, dict):
                symbol = str(
                    group.get("thscode")
                    or group.get("code")
                    or group.get("symbol")
                    or ""
                ).upper()
                rows = group.get("items") or group.get("data") or []
                if symbol and isinstance(rows, list):
                    grouped[symbol] = [item for item in rows if isinstance(item, dict)]
    frames: dict[str, pd.DataFrame] = {}
    for key, rows in grouped.items():
        if not rows:
            continue
        frame = _normalize_intraday_df(pd.DataFrame(rows))
        if not frame.empty:
            frames[key] = frame
    return frames


def _load_qveris_minute_frames(
    profiles: list[SecurityProfile],
    trade_date: str,
    usage_tracker: QVerisUsageTracker | None = None,
) -> tuple[dict[str, pd.DataFrame], str]:
    tracker = usage_tracker or QVerisUsageTracker()
    registry = QVerisToolRegistry()
    client = QVerisClient()
    try:
        tool = registry.ensure_tool(
            "cn_a_intraday_minute",
            client,
            require_batch_capability=True,
        )
    except Exception:
        tracker.record_skip("intraday_minute", "batch_capability_unavailable")
        return {}, "qveris:unavailable"

    codes = [_qveris_symbol(profile) for profile in profiles]
    requested_codes, skipped_reason = tracker.plan_codes(
        "intraday_minute",
        codes,
        supports_batch=bool(tool.get("supports_batch")),
    )
    if not requested_codes:
        return {}, f"qveris:skipped_{skipped_reason or 'budget'}"

    parameters: dict[str, Any] = {
        str(tool.get("batch_parameter_name") or "codes"): ",".join(requested_codes),
        "startdate": trade_date,
        "enddate": trade_date,
        "interval": "1",
    }
    try:
        response = client.call_tool(
            str(tool["tool_id"]),
            str(tool["discovery_id"]),
            parameters=parameters,
            max_response_size=48_000,
            timeout_ms=60_000,
        )
    except QVerisClientError as exc:
        if exc.status_code is not None:
            registry.invalidate("cn_a_intraday_minute")
        tracker.record_skip("intraday_minute", "call_failed")
        return {}, "qveris:unavailable"

    frames = _extract_qveris_minute_frames(response)
    route = f"qveris:{tool['tool_id']}"
    tracker.record_success(
        "intraday_minute",
        requested_codes=len(requested_codes),
        resolved_codes=len(frames),
        route=route,
        tool_id=str(tool["tool_id"]),
    )
    return frames, route


def _market_close_reached(trade_date: str) -> bool:
    today = datetime.now().strftime("%Y-%m-%d")
    if trade_date != today:
        return True
    return datetime.now().strftime("%H:%M") >= "15:00"


def calc_proxy_tail_metrics(snapshot: OvernightSnapshot) -> TailMetrics:
    proxy_tail_return = max(snapshot.intraday_return_from_open * 0.35, 0.0)
    proxy_amount_ratio = min(max(snapshot.amount / 2.0e9, 0.06), 0.22) if snapshot.amount > 0 else 0.0
    proxy_last10 = max(proxy_tail_return * 0.28, 0.0)
    close_ratio = min(max(snapshot.position / 100.0, 0.0), 1.0)
    auction_strength = max(min((snapshot.position - 70.0) / 500.0, 0.12), 0.0)
    return TailMetrics(
        has_real_tail_data=False,
        source="snapshot_proxy",
        tail_return_pct=round(proxy_tail_return, 4),
        tail_amount_ratio=round(proxy_amount_ratio, 4),
        last10_return_pct=round(proxy_last10, 4),
        close_at_high_ratio=round(close_ratio, 4),
        auction_strength=round(auction_strength, 4),
        rows=1,
        note="proxy_fallback",
        quality="proxy",
    )


def calc_tail_metrics_from_minute_df(
    frame: pd.DataFrame,
    snapshot: OvernightSnapshot,
    trade_date: str,
    tail_start_time: str,
    tail_last_window_minutes: int,
) -> TailMetrics:
    normalized = _normalize_intraday_df(frame)
    if normalized.empty:
        return TailMetrics(has_real_tail_data=False, note="minute_df_empty", quality="missing")

    target_day = datetime.strptime(trade_date, "%Y-%m-%d").date()
    normalized = normalized[normalized["dt"].dt.date == target_day].copy()
    if normalized.empty:
        return TailMetrics(has_real_tail_data=False, note="minute_df_not_found", quality="missing")

    tail_start = pd.Timestamp(f"{trade_date} {tail_start_time}")
    tail_df = normalized[normalized["dt"] >= tail_start].copy()
    if len(tail_df) < 3:
        return TailMetrics(
            has_real_tail_data=False,
            rows=len(normalized),
            note="tail_rows_insufficient",
            quality="invalid",
        )

    max_time = tail_df["dt"].max().strftime("%H:%M")
    if _market_close_reached(trade_date) and max_time < "14:57":
        return TailMetrics(
            has_real_tail_data=False,
            rows=len(normalized),
            note=f"incomplete_tail_window:{max_time}",
            quality="invalid",
        )
    if not _market_close_reached(trade_date):
        return TailMetrics(
            has_real_tail_data=False,
            rows=len(normalized),
            note="market_not_closed",
            quality="invalid",
        )

    close_price = float(tail_df["price"].iloc[-1])
    start_price = float(tail_df["price"].iloc[0])
    high_p = float(tail_df["high_p"].max())
    low_p = float(tail_df["low_p"].min())

    full_amount = max(snapshot.amount, 0.0)
    tail_amount = float(tail_df["amount"].sum())
    tail_ratio = (tail_amount / full_amount) if full_amount > 0 else 0.0
    tail_return = (close_price - start_price) / start_price * 100 if start_price > 0 else 0.0

    last_window = tail_df.tail(max(tail_last_window_minutes + 1, 2))
    last_start = float(last_window["price"].iloc[0])
    last_close = float(last_window["price"].iloc[-1])
    last_return = (last_close - last_start) / last_start * 100 if last_start > 0 else 0.0

    close_at_high_ratio = (close_price - low_p) / (high_p - low_p) if high_p > low_p else 0.5
    prev_price = float(tail_df["price"].iloc[-2]) if len(tail_df) >= 2 else close_price
    auction_strength = (close_price - prev_price) / prev_price * 100 if prev_price > 0 else 0.0

    return TailMetrics(
        has_real_tail_data=True,
        source="akshare_minute",
        tail_return_pct=round(tail_return, 4),
        tail_amount_ratio=round(tail_ratio, 4),
        last10_return_pct=round(last_return, 4),
        close_at_high_ratio=round(close_at_high_ratio, 4),
        auction_strength=round(auction_strength, 4),
        rows=len(normalized),
        quality="real",
    )


def load_tail_metrics(
    profile: SecurityProfile,
    snapshot: OvernightSnapshot,
    trade_date: str,
    mode: OvernightMode,
    cache_root: Path,
    tail_start_time: str,
    tail_last_window_minutes: int,
) -> TailMetrics:
    cache_path = _tail_cache_path(cache_root, profile, trade_date)
    if cache_path.exists():
        cached = pd.read_csv(cache_path)
        tail = calc_tail_metrics_from_minute_df(
            cached,
            snapshot,
            trade_date,
            tail_start_time,
            tail_last_window_minutes,
        )
        if tail.has_real_tail_data:
            tail.source = f"disk_cache:{trade_date}"
            tail.provider_chain = ["disk_cache"]
            return tail

    frame, source = _load_intraday_minute_df(profile, trade_date)
    if not frame.empty:
        frame.to_csv(cache_path, index=False, encoding="utf-8-sig")
        tail = calc_tail_metrics_from_minute_df(
            frame,
            snapshot,
            trade_date,
            tail_start_time,
            tail_last_window_minutes,
        )
        tail.source = source
        tail.provider_chain = [source]
        if tail.has_real_tail_data:
            return tail

    if mode == "research_fallback":
        proxy = calc_proxy_tail_metrics(snapshot)
        proxy.note = f"proxy_after:{source}"
        proxy.provider_chain = [source, "snapshot_proxy"]
        return proxy

    return TailMetrics(
        has_real_tail_data=False,
        source=source,
        note="real_tail_required",
        quality="missing",
        provider_chain=[source],
    )


def load_tail_metrics_batch(
    snapshots: list[OvernightSnapshot],
    trade_date: str,
    mode: OvernightMode,
    cache_root: Path,
    tail_start_time: str,
    tail_last_window_minutes: int,
    usage_tracker: QVerisUsageTracker | None = None,
) -> dict[str, TailMetrics]:
    tails: dict[str, TailMetrics] = {}
    unresolved: list[OvernightSnapshot] = []

    for snapshot in snapshots:
        tail = load_tail_metrics(
            snapshot.profile,
            snapshot,
            trade_date,
            "strict",
            cache_root,
            tail_start_time,
            tail_last_window_minutes,
        )
        tails[snapshot.code] = tail
        if not tail.has_real_tail_data and tail.quality in {"missing", "invalid"}:
            unresolved.append(snapshot)

    if unresolved:
        qveris_frames, qveris_route = _load_qveris_minute_frames(
            [snapshot.profile for snapshot in unresolved],
            trade_date,
            usage_tracker=usage_tracker,
        )
        for snapshot in unresolved:
            frame = qveris_frames.get(_qveris_symbol(snapshot.profile))
            if frame is None:
                continue
            tail = calc_tail_metrics_from_minute_df(
                frame,
                snapshot,
                trade_date,
                tail_start_time,
                tail_last_window_minutes,
            )
            tail.source = qveris_route
            tail.provider_chain = [tails[snapshot.code].source, qveris_route]
            if tail.has_real_tail_data:
                cache_path = _tail_cache_path(cache_root, snapshot.profile, trade_date)
                frame.to_csv(cache_path, index=False, encoding="utf-8-sig")
                tails[snapshot.code] = tail

    if mode == "research_fallback":
        for snapshot in snapshots:
            tail = tails[snapshot.code]
            if tail.has_real_tail_data or tail.quality == "proxy":
                continue
            proxy = calc_proxy_tail_metrics(snapshot)
            proxy.note = f"proxy_after:{tail.source}"
            proxy.provider_chain = [*tail.provider_chain, "snapshot_proxy"] if tail.provider_chain else [tail.source, "snapshot_proxy"]
            tails[snapshot.code] = proxy

    return tails
