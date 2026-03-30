from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

from tradingagents.market_utils import (
    build_security_profile,
    call_with_proxy_fallback,
    call_with_proxy_or_empty_fallback,
)

from .tail_data import (
    load_intraday_minute_frame,
    normalize_intraday_minute_df,
    pick_intraday_price_near_time,
)


ENTRY_TARGET_TIME = "14:55"
EXIT_TARGET_TIME = "10:00"
HS300_INDEX_SYMBOL = "000300"
CHINA_TZ = timezone(timedelta(hours=8))


def build_cache_root(data_dir: str | Path) -> Path:
    cache_root = Path(data_dir) / "overnight_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root


def _local_trade_timestamp(trade_date: str, target_time: str) -> datetime:
    return datetime.fromisoformat(f"{trade_date}T{target_time}:00").replace(tzinfo=CHINA_TZ)


def _normalize_trade_calendar_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    date_col = None
    for candidate in ("trade_date", "日期", "date"):
        if candidate in normalized.columns:
            date_col = candidate
            break
    if not date_col:
        return pd.DataFrame()
    normalized["trade_date"] = pd.to_datetime(normalized[date_col], errors="coerce")
    normalized = normalized.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
    return normalized[["trade_date"]]


def load_trade_calendar_dates() -> list[str]:
    frame = call_with_proxy_fallback(ak.tool_trade_date_hist_sina)
    normalized = _normalize_trade_calendar_frame(frame)
    if normalized.empty:
        return []
    return normalized["trade_date"].dt.strftime("%Y-%m-%d").tolist()


def get_next_trade_date(trade_date: str) -> str | None:
    dates = load_trade_calendar_dates()
    if not dates:
        return None
    for index, current in enumerate(dates):
        if current == trade_date:
            return dates[index + 1] if index + 1 < len(dates) else None
        if current > trade_date:
            return current
    return None


def lookup_trade_price(
    ticker: str,
    trade_date: str,
    target_time: str,
    cache_root: Path,
    prefer_on_tie: str,
) -> tuple[float | None, str | None]:
    profile = build_security_profile(ticker, "cn_a")
    frame, _source = load_intraday_minute_frame(profile, trade_date, cache_root)
    return pick_intraday_price_near_time(
        frame,
        trade_date,
        target_time,
        prefer_on_tie=prefer_on_tie,  # type: ignore[arg-type]
    )


def refresh_tracked_trade(
    trade: dict[str, Any],
    *,
    data_dir: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now.astimezone(CHINA_TZ) if now else datetime.now(CHINA_TZ)
    cache_root = build_cache_root(data_dir)
    checked_at = datetime.now(timezone.utc).isoformat()

    trade_date = str(trade["trade_date"])
    ticker = str(trade["ticker"])
    entry_target = _local_trade_timestamp(trade_date, ENTRY_TARGET_TIME)
    next_trade_date = get_next_trade_date(trade_date)

    updated: dict[str, Any] = {
        "entry_target_time": ENTRY_TARGET_TIME,
        "exit_target_time": EXIT_TARGET_TIME,
        "exit_trade_date": next_trade_date,
        "last_checked_at": checked_at,
        "updated_at": checked_at,
    }

    if current_time < entry_target:
        updated["status"] = "pending_entry"
        updated["last_error"] = None
        return updated

    entry_price = trade.get("entry_price")
    entry_time_used = trade.get("entry_time_used")
    if entry_price is None or float(entry_price) <= 0:
        entry_price, entry_time_used = lookup_trade_price(
            ticker,
            trade_date,
            ENTRY_TARGET_TIME,
            cache_root,
            prefer_on_tie="before",
        )
        updated["entry_price"] = round(entry_price, 4) if entry_price else None
        updated["entry_time_used"] = entry_time_used

    if entry_price is None or float(entry_price) <= 0:
        updated["status"] = "unavailable"
        updated["last_error"] = "missing_entry_price"
        return updated

    if not next_trade_date:
        updated["status"] = "pending_exit"
        updated["last_error"] = "missing_next_trade_date"
        return updated

    exit_target = _local_trade_timestamp(next_trade_date, EXIT_TARGET_TIME)
    if current_time < exit_target:
        updated["status"] = "pending_exit"
        updated["last_error"] = None
        return updated

    exit_price = trade.get("exit_price")
    exit_time_used = trade.get("exit_time_used")
    if exit_price is None or float(exit_price) <= 0:
        exit_price, exit_time_used = lookup_trade_price(
            ticker,
            next_trade_date,
            EXIT_TARGET_TIME,
            cache_root,
            prefer_on_tie="after",
        )
        updated["exit_price"] = round(exit_price, 4) if exit_price else None
        updated["exit_time_used"] = exit_time_used

    if exit_price is None or float(exit_price) <= 0:
        updated["status"] = "unavailable"
        updated["last_error"] = "missing_exit_price"
        return updated

    updated["strategy_return"] = round((float(exit_price) - float(entry_price)) / float(entry_price) * 100, 4)
    updated["status"] = "validated"
    updated["last_error"] = None
    return updated


def build_tracked_trade_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    validated = [
        trade for trade in trades
        if trade.get("status") == "validated" and trade.get("strategy_return") is not None
    ]
    returns = [float(trade["strategy_return"]) for trade in validated]
    avg_return = round(sum(returns) / len(returns), 4) if returns else None
    win_rate = (
        round(sum(1 for value in returns if value > 0) / len(returns), 4)
        if returns
        else None
    )
    cumulative_return = None
    if returns:
        compounded = 1.0
        for value in returns:
            compounded *= 1.0 + value / 100.0
        cumulative_return = round((compounded - 1.0) * 100.0, 4)
    return {
        "total_days": len(trades),
        "validated_days": len(validated),
        "pending_count": sum(
            1 for trade in trades if trade.get("status") in {"pending_entry", "pending_exit"}
        ),
        "unavailable_count": sum(1 for trade in trades if trade.get("status") == "unavailable"),
        "avg_return": avg_return,
        "win_rate": win_rate,
        "cumulative_return": cumulative_return,
    }


def _index_minute_cache_path(cache_root: Path, trade_date: str) -> Path:
    directory = cache_root / "index_minute" / trade_date
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{HS300_INDEX_SYMBOL}.csv"


def load_hs300_minute_frame(
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
