from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import akshare as ak
import pandas as pd

from tradingagents.market_utils import call_with_proxy_fallback
from tradingagents.qveris import QVerisClient, QVerisToolRegistry, QVerisUsageTracker
from tradingagents.qveris.client import QVerisClientError

from .models import MarketRegime


INDEX_SYMBOLS = {
    "上证指数": "sh000001",
    "沪深300": "sh000300",
    "创业板指": "sz399006",
}

QVERIS_INDEX_CODES = {
    "上证指数": "000001.SH",
    "沪深300": "399300.SZ",
    "创业板指": "399006.SZ",
}


@dataclass
class IndexSnapshotResult:
    values: dict[str, float]
    provider_route: str
    bias_flags: list[str] = field(default_factory=list)
    qveris_tool_ids: list[str] = field(default_factory=list)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
    except TypeError:
        pass
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


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


def _load_index_snapshot_from_qveris(
    usage_tracker: QVerisUsageTracker | None = None,
) -> IndexSnapshotResult:
    tracker = usage_tracker or QVerisUsageTracker()
    requested_codes, _ = tracker.plan_codes(
        "index_snapshot",
        list(QVERIS_INDEX_CODES.values()),
        supports_batch=True,
    )
    if not requested_codes:
        return IndexSnapshotResult(
            values={},
            provider_route="qveris:skipped_budget",
        )
    registry = QVerisToolRegistry()
    client = QVerisClient()
    try:
        tool = registry.ensure_tool(
            "cn_a_index_snapshot",
            client,
            require_batch_capability=True,
        )
    except Exception:
        tracker.record_skip("index_snapshot", "batch_capability_unavailable")
        return IndexSnapshotResult(
            values={},
            provider_route="qveris:unavailable",
        )
    try:
        response = client.call_tool(
            str(tool["tool_id"]),
            str(tool["discovery_id"]),
            parameters={
                str(tool.get("batch_parameter_name") or "codes"): ",".join(requested_codes),
                "indicators": "common",
            },
            max_response_size=12_000,
            timeout_ms=30_000,
        )
    except QVerisClientError as exc:
        if exc.status_code is not None:
            registry.invalidate("cn_a_index_snapshot")
        tracker.record_skip("index_snapshot", "call_failed")
        raise

    rows_by_code: dict[str, dict[str, object]] = {}
    data = ((response.get("result") or {}).get("data")) or []
    if isinstance(data, list):
        for group in data:
            if not isinstance(group, list):
                continue
            for row in group:
                if not isinstance(row, dict):
                    continue
                code = str(row.get("thscode") or "").upper()
                if code:
                    rows_by_code[code] = row

    mapped: dict[str, float] = {}
    for label, symbol in QVERIS_INDEX_CODES.items():
        if symbol not in requested_codes:
            continue
        row = rows_by_code.get(symbol)
        if not row:
            continue
        change_ratio = row.get("changeRatio")
        if change_ratio is None:
            latest = _safe_float(row.get("latest"))
            previous = _safe_float(row.get("preClose"))
            pct = ((latest - previous) / previous * 100.0) if previous > 0 else 0.0
        else:
            pct = _safe_float(change_ratio)
        mapped[label] = round(pct, 4)

    provider_route = f"qveris:{tool['tool_id']}"
    if mapped:
        tracker.record_success(
            "index_snapshot",
            requested_codes=len(requested_codes),
            resolved_codes=len(mapped),
            route=provider_route,
            tool_id=str(tool["tool_id"]),
        )
    return IndexSnapshotResult(
        values=mapped,
        provider_route=provider_route,
        qveris_tool_ids=[str(tool["tool_id"])],
    )


def _load_index_snapshot_from_daily(trade_date: str) -> IndexSnapshotResult:
    result: dict[str, float] = {}
    compact_date = trade_date.replace("-", "")
    for label, symbol in INDEX_SYMBOLS.items():
        try:
            frame = call_with_proxy_fallback(ak.stock_zh_index_daily, symbol=symbol)
        except Exception:
            continue
        normalized = _normalize_frame(frame)
        date_col = _find_column(normalized, ["date", "日期"])
        open_col = _find_column(normalized, ["open", "开盘"])
        close_col = _find_column(normalized, ["close", "收盘"])
        if not date_col or not open_col or not close_col:
            continue
        normalized[date_col] = pd.to_datetime(normalized[date_col], errors="coerce")
        normalized = normalized.dropna(subset=[date_col]).sort_values(date_col)
        matched = normalized[normalized[date_col].dt.strftime("%Y%m%d") <= compact_date]
        if matched.empty:
            continue
        row = matched.iloc[-1]
        open_price = _safe_float(row[open_col])
        close_price = _safe_float(row[close_col])
        pct = ((close_price - open_price) / open_price * 100.0) if open_price > 0 else 0.0
        result[label] = round(pct, 4)
    return IndexSnapshotResult(
        values=result,
        provider_route="akshare_index_daily",
        bias_flags=["index_daily_fallback"] if result else [],
    )


def load_index_snapshot(
    trade_date: str,
    usage_tracker: QVerisUsageTracker | None = None,
) -> IndexSnapshotResult:
    today = datetime.now().strftime("%Y-%m-%d")
    if trade_date == today:
        try:
            frame = ak.stock_zh_index_spot_sina()
        except Exception:
            frame = None
        normalized = _normalize_frame(frame)
        code_col = _find_column(normalized, ["代码"])
        pct_col = _find_column(normalized, ["涨跌幅"])
        if code_col and pct_col:
            result: dict[str, float] = {}
            for label, symbol in INDEX_SYMBOLS.items():
                matched = normalized[normalized[code_col].astype(str) == symbol]
                if matched.empty:
                    continue
                result[label] = _safe_float(matched.iloc[0][pct_col])
            if result:
                return IndexSnapshotResult(
                    values=result,
                    provider_route="akshare_index_spot_sina",
                )

        try:
            qveris_result = _load_index_snapshot_from_qveris(usage_tracker)
        except Exception:
            if usage_tracker is not None:
                usage_tracker.record_skip("index_snapshot", "fallback_failed")
            qveris_result = IndexSnapshotResult(
                values={},
                provider_route="qveris:unavailable",
            )
        if qveris_result.values:
            return qveris_result

        daily_result = _load_index_snapshot_from_daily(trade_date)
        if daily_result.values:
            return daily_result
        return IndexSnapshotResult(values={}, provider_route="unavailable")

    daily_result = _load_index_snapshot_from_daily(trade_date)
    if daily_result.values:
        daily_result.bias_flags = []
    return daily_result


def evaluate_market_regime(index_values: dict[str, float], trade_date: str) -> MarketRegime:
    if not index_values:
        return MarketRegime(
            market_ok=True,
            market_message="无法获取市场指数快照，默认允许扫描。",
            benchmark_pct=0.0,
            indices={},
            notes=["market_indices_unavailable"],
        )

    sh = index_values.get("上证指数", 0.0)
    hs300 = index_values.get("沪深300", sh)
    cyb = index_values.get("创业板指", sh)
    parts = [f"上证 {sh:+.2f}%", f"沪深300 {hs300:+.2f}%", f"创业板指 {cyb:+.2f}%"]

    if sh <= -0.8 or (sh < 0 and hs300 < 0 and cyb < 0):
        return MarketRegime(
            market_ok=False,
            market_message=" | ".join(parts) + " | 市场偏弱，正式推荐阈值提高并收缩推荐数量。",
            benchmark_pct=hs300,
            indices=index_values,
            formal_threshold_delta=3.0,
            formal_limit_cap=3,
            notes=["weak_market_regime"],
        )

    if sh < -0.3:
        return MarketRegime(
            market_ok=True,
            market_message=" | ".join(parts) + " | 市场偏弱，建议缩量参与。",
            benchmark_pct=hs300,
            indices=index_values,
            formal_threshold_delta=2.0,
            formal_limit_cap=5,
            notes=["cautious_market_regime"],
        )

    return MarketRegime(
        market_ok=True,
        market_message=" | ".join(parts),
        benchmark_pct=hs300,
        indices=index_values,
    )
