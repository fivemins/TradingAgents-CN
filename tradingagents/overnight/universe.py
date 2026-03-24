from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Iterable

import akshare as ak
import pandas as pd
import requests

from tradingagents.market_utils import (
    build_security_profile,
    call_with_proxy_or_empty_fallback,
)
from tradingagents.qveris import QVerisClient, QVerisToolRegistry, QVerisUsageTracker
from tradingagents.qveris.client import QVerisClientError

from .models import OvernightSnapshot, PoolType, ScanParams


_SPOT_CACHE_TTL_SECONDS = 120.0
_SPOT_CACHE: dict[str, object] = {"timestamp": 0.0, "frame": None}

CODE_COLUMN = "代码"
NAME_COLUMN = "名称"
LATEST_COLUMN = "最新价"
PCT_COLUMN = "涨跌幅"
AMOUNT_COLUMN = "成交额"
TURNOVER_COLUMN = "换手率"
PRE_CLOSE_COLUMN = "昨收"
OPEN_COLUMN = "今开"
HIGH_COLUMN = "最高"
LOW_COLUMN = "最低"
PROVIDER_ROUTE_COLUMN = "_provider_route"

logger = logging.getLogger(__name__)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
    except TypeError:
        logger.debug("Falling back to string coercion while normalizing universe numeric values.")
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


def _find_column(frame: pd.DataFrame, keywords: Iterable[str]) -> str | None:
    lowered = [keyword.lower() for keyword in keywords]
    for column in frame.columns:
        name = str(column).strip().lower()
        if any(keyword in name for keyword in lowered):
            return str(column)
    return None


def classify_pool(code: str) -> PoolType:
    normalized = str(code).split(".", 1)[0]
    if normalized.startswith("688"):
        return "star"
    if normalized.startswith("3"):
        return "gem"
    if normalized.startswith(("0", "6", "5", "9")):
        return "main"
    return "other"


def pool_label(pool: PoolType) -> str:
    return {
        "main": "主板",
        "gem": "创业板",
        "star": "科创板",
        "other": "其他",
    }[pool]


def universe_snapshot_path(cache_root: str | Path, trade_date: str) -> Path:
    return Path(cache_root) / "universe" / f"{trade_date}.parquet"


def persist_universe_snapshot(
    frame: pd.DataFrame,
    cache_root: str | Path,
    trade_date: str,
) -> Path:
    snapshot_path = universe_snapshot_path(cache_root, trade_date)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        column
        for column in ("code", "name", "pool", "score", "latest", "pct", "amount", "turnover")
        if column in frame.columns
    ]
    frame[columns].copy().to_parquet(snapshot_path, index=False)
    return snapshot_path


def load_universe_snapshot(cache_root: str | Path, trade_date: str) -> pd.DataFrame:
    snapshot_path = universe_snapshot_path(cache_root, trade_date)
    if not snapshot_path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_parquet(snapshot_path)
    except Exception:
        return pd.DataFrame()
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame


def _qveris_symbol(code: str) -> str:
    normalized = str(code).zfill(6)
    suffix = "SH" if normalized.startswith(("5", "6", "9")) else "SZ"
    return f"{normalized}.{suffix}"


def get_spot_provider_route(frame: pd.DataFrame, default: str = "spot_unavailable") -> str:
    route = frame.attrs.get("provider_route")
    if isinstance(route, str) and route:
        return route
    if frame.empty:
        return default
    if PROVIDER_ROUTE_COLUMN in frame.columns:
        series = frame[PROVIDER_ROUTE_COLUMN].dropna()
        if not series.empty:
            return str(series.iloc[0])
    return default


def _load_code_name_table() -> pd.DataFrame:
    try:
        frame = call_with_proxy_or_empty_fallback(ak.stock_info_a_code_name)
    except Exception:
        return pd.DataFrame()

    normalized = _normalize_frame(frame)
    if normalized.empty or "code" not in normalized.columns or "name" not in normalized.columns:
        return pd.DataFrame()

    working = normalized.copy()
    working["code"] = working["code"].astype(str).str.zfill(6)
    working = working[working["code"].str.match(r"^(0|3|5|6|9)\d{5}$")]
    return working.reset_index(drop=True)


def load_market_spot_table(
    cache_root: str | Path | None = None,
    usage_tracker: QVerisUsageTracker | None = None,
) -> pd.DataFrame:
    cached_frame = _get_cached_spot_frame()
    if not cached_frame.empty:
        return cached_frame

    for loader in (
        _load_market_spot_table_from_sina,
        _load_market_spot_table_from_akshare,
    ):
        frame = loader()
        if not frame.empty:
            _set_cached_spot_frame(frame)
            return frame
    frame = _load_market_spot_table_from_qveris_limited(
        cache_root=cache_root,
        usage_tracker=usage_tracker,
    )
    if not frame.empty:
        _set_cached_spot_frame(frame)
    return frame


def _empty_frame_with_route(route: str) -> pd.DataFrame:
    frame = pd.DataFrame()
    frame.attrs["provider_route"] = route
    return frame


def _get_cached_spot_frame() -> pd.DataFrame:
    frame = _SPOT_CACHE.get("frame")
    timestamp = float(_SPOT_CACHE.get("timestamp") or 0.0)
    if not isinstance(frame, pd.DataFrame):
        return pd.DataFrame()
    if time.monotonic() - timestamp > _SPOT_CACHE_TTL_SECONDS:
        return pd.DataFrame()
    return frame.copy()


def _set_cached_spot_frame(frame: pd.DataFrame) -> None:
    _SPOT_CACHE["timestamp"] = time.monotonic()
    copied = frame.copy()
    copied.attrs = dict(getattr(frame, "attrs", {}))
    _SPOT_CACHE["frame"] = copied


def _standardize_spot_frame(frame: pd.DataFrame, route: str) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return pd.DataFrame()

    code_col = _find_column(normalized, ["代码", "股票代码", "symbol"])
    name_col = _find_column(normalized, ["名称", "股票名称", "name"])
    latest_col = _find_column(normalized, ["最新价", "现价", "close", "price", "latest"])
    pct_col = _find_column(normalized, ["涨跌幅", "涨幅", "changepercent", "changeratio"])
    amount_col = _find_column(normalized, ["成交额", "成交金额", "amount", "turnover_amount"])
    turnover_col = _find_column(normalized, ["换手率", "turnoverratio"])
    pre_close_col = _find_column(normalized, ["昨收", "preclose", "previousclose"])
    open_col = _find_column(normalized, ["今开", "open"])
    high_col = _find_column(normalized, ["最高", "high"])
    low_col = _find_column(normalized, ["最低", "low"])

    if not all([code_col, name_col, latest_col, pct_col, amount_col, pre_close_col, open_col, high_col, low_col]):
        return pd.DataFrame()

    working = pd.DataFrame(
        {
            CODE_COLUMN: normalized[code_col].astype(str).str.extract(r"(\d{6})", expand=False).fillna(""),
            NAME_COLUMN: normalized[name_col].astype(str),
            LATEST_COLUMN: pd.to_numeric(normalized[latest_col], errors="coerce"),
            PCT_COLUMN: pd.to_numeric(normalized[pct_col], errors="coerce"),
            AMOUNT_COLUMN: pd.to_numeric(normalized[amount_col], errors="coerce"),
            TURNOVER_COLUMN: (
                pd.to_numeric(normalized[turnover_col], errors="coerce")
                if turnover_col
                else 0.0
            ),
            PRE_CLOSE_COLUMN: pd.to_numeric(normalized[pre_close_col], errors="coerce"),
            OPEN_COLUMN: pd.to_numeric(normalized[open_col], errors="coerce"),
            HIGH_COLUMN: pd.to_numeric(normalized[high_col], errors="coerce"),
            LOW_COLUMN: pd.to_numeric(normalized[low_col], errors="coerce"),
        }
    )

    working[CODE_COLUMN] = working[CODE_COLUMN].astype(str).str.zfill(6)
    working[TURNOVER_COLUMN] = pd.to_numeric(working[TURNOVER_COLUMN], errors="coerce").fillna(0.0)
    working = working.dropna(
        subset=[
            LATEST_COLUMN,
            PCT_COLUMN,
            AMOUNT_COLUMN,
            PRE_CLOSE_COLUMN,
            OPEN_COLUMN,
            HIGH_COLUMN,
            LOW_COLUMN,
        ]
    )
    working = working[working[CODE_COLUMN].str.match(r"^(0|3|5|6|9)\d{5}$")]
    if working.empty:
        return pd.DataFrame()
    working[PROVIDER_ROUTE_COLUMN] = route
    working.attrs["provider_route"] = route
    return working.reset_index(drop=True)


def _load_market_spot_table_from_sina() -> pd.DataFrame:
    working = _load_code_name_table()
    if working.empty:
        return pd.DataFrame()

    symbol_map = {
        row["code"]: (f"sh{row['code']}" if row["code"].startswith(("5", "6", "9")) else f"sz{row['code']}")
        for _, row in working.iterrows()
    }
    name_map = {row["code"]: str(row["name"]) for _, row in working.iterrows()}

    session = requests.Session()
    session.trust_env = False
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0",
    }

    rows: list[dict[str, object]] = []
    symbols = list(symbol_map.items())
    chunk_size = 500
    for offset in range(0, len(symbols), chunk_size):
        chunk = symbols[offset : offset + chunk_size]
        symbol_list = ",".join(symbol for _, symbol in chunk)
        try:
            response = session.get(
                "https://hq.sinajs.cn/list=" + symbol_list,
                headers=headers,
                timeout=15,
            )
            response.raise_for_status()
        except Exception:
            continue

        for line in response.text.splitlines():
            prefix = "var hq_str_"
            if not line.startswith(prefix) or '="' not in line:
                continue
            symbol, payload = line[len(prefix) :].split('="', 1)
            values = payload.rstrip('";').split(",")
            if len(values) < 10:
                continue
            code = symbol[-6:]
            name = name_map.get(code, values[0] or code)
            open_price = _safe_float(values[1], default=-1.0)
            prev_close = _safe_float(values[2], default=-1.0)
            latest = _safe_float(values[3], default=-1.0)
            high = _safe_float(values[4], default=-1.0)
            low = _safe_float(values[5], default=-1.0)
            amount = _safe_float(values[9], default=0.0)
            if min(open_price, prev_close, latest, high, low) <= 0:
                continue
            pct = (latest - prev_close) / prev_close * 100.0 if prev_close > 0 else 0.0
            rows.append(
                {
                    CODE_COLUMN: code,
                    NAME_COLUMN: name,
                    LATEST_COLUMN: latest,
                    PCT_COLUMN: round(pct, 4),
                    AMOUNT_COLUMN: amount,
                    TURNOVER_COLUMN: 0.0,
                    PRE_CLOSE_COLUMN: prev_close,
                    OPEN_COLUMN: open_price,
                    HIGH_COLUMN: high,
                    LOW_COLUMN: low,
                    PROVIDER_ROUTE_COLUMN: "sina_hq_batch",
                }
            )

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame.attrs["provider_route"] = "sina_hq_batch"
    return frame


def _load_recent_universe_seed_frame(cache_root: str | Path | None) -> pd.DataFrame:
    if cache_root is None:
        return pd.DataFrame()
    snapshot_dir = Path(cache_root) / "universe"
    if not snapshot_dir.exists():
        return pd.DataFrame()
    for snapshot_path in sorted(snapshot_dir.glob("*.parquet"), reverse=True)[:3]:
        try:
            frame = pd.read_parquet(snapshot_path)
        except Exception:
            continue
        normalized = _normalize_frame(frame)
        if normalized.empty or "code" not in normalized.columns:
            continue
        normalized["code"] = normalized["code"].astype(str).str.zfill(6)
        return normalized
    return pd.DataFrame()


def _load_recent_candidate_seed_frame() -> pd.DataFrame:
    dashboard_data_dir = Path(
        os.getenv(
            "TRADINGAGENTS_DASHBOARD_DATA_DIR",
            Path(__file__).resolve().parents[2] / "dashboard_data",
        )
    )
    scans_dir = dashboard_data_dir / "overnight_scans"
    if not scans_dir.exists():
        return pd.DataFrame()
    for recommendations_path in sorted(scans_dir.glob("*/recommendations.json"), reverse=True)[:5]:
        try:
            payload = json.loads(recommendations_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows: list[dict[str, object]] = []
        for bucket in ("formal_recommendations", "watchlist", "preliminary_candidates"):
            for item in payload.get(bucket, []) or []:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("ticker") or item.get("code") or "").split(".", 1)[0]
                if not code:
                    continue
                rows.append(
                    {
                        "code": code.zfill(6),
                        "name": str(item.get("name") or code),
                        "score": _safe_float(item.get("total_score"), default=_safe_float(item.get("quick_score"), 0.0)),
                        "amount": _safe_float(item.get("amount"), 0.0),
                    }
                )
        if rows:
            frame = pd.DataFrame(rows).drop_duplicates("code")
            if not frame.empty:
                return frame.reset_index(drop=True)
    return pd.DataFrame()


def _load_market_spot_table_from_qveris_limited(
    *,
    cache_root: str | Path | None,
    usage_tracker: QVerisUsageTracker | None = None,
) -> pd.DataFrame:
    seed_frame = _load_recent_universe_seed_frame(cache_root)
    if seed_frame.empty:
        seed_frame = _load_recent_candidate_seed_frame()
    tracker = usage_tracker or QVerisUsageTracker()
    if seed_frame.empty:
        tracker.record_skip("realtime_spot", "no_seed_universe")
        return _empty_frame_with_route("qveris:skipped_no_seed_universe")

    registry = QVerisToolRegistry()
    client = QVerisClient()
    try:
        tool = registry.ensure_tool(
            "cn_a_realtime_spot",
            client,
            require_batch_capability=True,
        )
    except Exception:
        tracker.record_skip("realtime_spot", "batch_capability_unavailable")
        return _empty_frame_with_route("qveris:unavailable")
    base_route = f"qveris:{tool['tool_id']}"
    name_map = {
        str(row["code"]).zfill(6): str(row.get("name") or row["code"])
        for _, row in seed_frame.iterrows()
    }
    ordered_codes = (
        seed_frame.sort_values(["score", "amount"], ascending=False)["code"].astype(str).str.zfill(6).tolist()
        if {"score", "amount"}.issubset(seed_frame.columns)
        else list(name_map.keys())
    )
    requested_codes, skipped_reason = tracker.plan_codes(
        "realtime_spot",
        ordered_codes,
        supports_batch=bool(tool.get("supports_batch")),
    )
    if not requested_codes:
        return _empty_frame_with_route(f"qveris:skipped_{skipped_reason or 'budget'}")

    rows: list[dict[str, object]] = []
    try:
        response = client.call_tool(
            str(tool["tool_id"]),
            str(tool["discovery_id"]),
            parameters={
                str(tool.get("batch_parameter_name") or "codes"): ",".join(
                    _qveris_symbol(code) for code in requested_codes
                ),
                "indicators": "common",
            },
            max_response_size=24_000,
            timeout_ms=60_000,
        )
    except QVerisClientError as exc:
        if exc.status_code is not None:
            registry.invalidate("cn_a_realtime_spot")
        logger.warning("QVeris limited spot fallback failed.")
        tracker.record_skip("realtime_spot", "call_failed")
        return _empty_frame_with_route("qveris:unavailable")

    data = ((response.get("result") or {}).get("data")) or []
    if not isinstance(data, list):
        tracker.record_skip("realtime_spot", "invalid_response_shape")
        return _empty_frame_with_route("qveris:invalid_response")
    for group in data:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            thscode = str(item.get("thscode") or "")
            code = thscode.split(".", 1)[0].zfill(6)
            latest = _safe_float(item.get("latest"), default=-1.0)
            pre_close = _safe_float(item.get("preClose"), default=-1.0)
            open_price = _safe_float(item.get("open"), default=-1.0)
            high = _safe_float(item.get("high"), default=-1.0)
            low = _safe_float(item.get("low"), default=-1.0)
            amount = _safe_float(item.get("amount"), default=0.0)
            if min(latest, pre_close, open_price, high, low) <= 0:
                continue
            change_ratio = item.get("changeRatio")
            pct = _safe_float(change_ratio) if change_ratio is not None else (
                (latest - pre_close) / pre_close * 100.0
            )
            turnover_ratio = item.get("turnoverRatio")
            turnover = _safe_float(turnover_ratio, default=0.0)
            provider_route = base_route if turnover_ratio is not None else f"{base_route}:missing_turnover"
            rows.append(
                {
                    CODE_COLUMN: code,
                    NAME_COLUMN: name_map.get(code, code),
                    LATEST_COLUMN: latest,
                    PCT_COLUMN: round(pct, 4),
                    AMOUNT_COLUMN: amount,
                    TURNOVER_COLUMN: turnover,
                    PRE_CLOSE_COLUMN: pre_close,
                    OPEN_COLUMN: open_price,
                    HIGH_COLUMN: high,
                    LOW_COLUMN: low,
                    PROVIDER_ROUTE_COLUMN: provider_route,
                }
            )

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame.attrs["provider_route"] = base_route
        frame.attrs["qveris_tool_id"] = str(tool["tool_id"])
        tracker.record_success(
            "realtime_spot",
            requested_codes=len(requested_codes),
            resolved_codes=len(frame),
            route=base_route,
            tool_id=str(tool["tool_id"]),
        )
    return frame


def _load_market_spot_table_from_qveris(
    *,
    cache_root: str | Path | None = None,
    usage_tracker: QVerisUsageTracker | None = None,
) -> pd.DataFrame:
    return _load_market_spot_table_from_qveris_limited(
        cache_root=cache_root,
        usage_tracker=usage_tracker,
    )


def _load_market_spot_table_from_akshare() -> pd.DataFrame:
    for loader in (ak.stock_zh_a_spot_em, ak.stock_zh_a_spot):
        for _ in range(3):
            try:
                frame = call_with_proxy_or_empty_fallback(loader)
            except Exception:
                time.sleep(1.0)
                continue
            normalized = _standardize_spot_frame(frame, f"akshare:{loader.__name__}")
            if not normalized.empty:
                return normalized
            time.sleep(0.5)
    return pd.DataFrame()


def build_dynamic_pool_from_frame(
    frame: pd.DataFrame,
    params: ScanParams,
) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return pd.DataFrame()

    code_col = _find_column(normalized, [CODE_COLUMN, "股票代码"])
    name_col = _find_column(normalized, [NAME_COLUMN, "股票名称"])
    latest_col = _find_column(normalized, [LATEST_COLUMN])
    pct_col = _find_column(normalized, [PCT_COLUMN, "涨幅"])
    amount_col = _find_column(normalized, [AMOUNT_COLUMN, "成交金额"])
    turnover_col = _find_column(normalized, [TURNOVER_COLUMN])

    if not all([code_col, name_col, latest_col, pct_col, amount_col]):
        return pd.DataFrame()

    working = pd.DataFrame(
        {
            "code": normalized[code_col].astype(str).str.extract(r"(\d{6})", expand=False).fillna(""),
            "name": normalized[name_col].astype(str),
            "latest": pd.to_numeric(normalized[latest_col], errors="coerce"),
            "pct": pd.to_numeric(normalized[pct_col], errors="coerce"),
            "amount": pd.to_numeric(normalized[amount_col], errors="coerce"),
            "turnover": (
                pd.to_numeric(normalized[turnover_col], errors="coerce")
                if turnover_col
                else 0.0
            ),
        }
    ).dropna(subset=["latest", "pct", "amount"])

    working["code"] = working["code"].astype(str).str.zfill(6)
    working["turnover"] = pd.to_numeric(working["turnover"], errors="coerce").fillna(0.0)
    working = working[working["code"].str.match(r"^(0|3|5|6|9)\d{5}$")]
    working = working[~working["name"].str.contains("ST", case=False, na=False)]
    working = working[
        working["latest"].between(params.dynamic_pool_min_price, params.dynamic_pool_max_price)
    ]
    working = working[working["pct"].between(-2.0, params.dynamic_pool_max_pct)]
    working = working[working["amount"] >= params.dynamic_pool_min_amount_yi * 1e8]

    if working.empty:
        return pd.DataFrame()

    working["pool"] = working["code"].apply(classify_pool)
    working["score"] = (
        working["amount"].rank(pct=True) * 0.55
        + working["turnover"].rank(pct=True) * 0.20
        + working["pct"].rank(pct=True) * 0.25
    )

    limits = {
        "main": params.dynamic_pool_main_limit,
        "gem": params.dynamic_pool_gem_limit,
        "star": params.dynamic_pool_star_limit,
    }
    selected_frames: list[pd.DataFrame] = []
    for pool, limit in limits.items():
        pool_frame = (
            working[working["pool"] == pool]
            .sort_values(["score", "amount"], ascending=False)
            .head(limit)
        )
        if not pool_frame.empty:
            selected_frames.append(pool_frame)

    if working[working["pool"] == "other"].shape[0]:
        selected_frames.append(
            working[working["pool"] == "other"]
            .sort_values(["score", "amount"], ascending=False)
            .head(max(10, params.dynamic_pool_max_size // 10))
        )

    if not selected_frames:
        return pd.DataFrame()

    selected = pd.concat(selected_frames, ignore_index=True).drop_duplicates("code")
    selected = selected.sort_values(["score", "amount"], ascending=False).head(params.dynamic_pool_max_size)
    return selected.head(params.dynamic_pool_realtime_limit).reset_index(drop=True)


def build_dynamic_pool(params: ScanParams) -> tuple[pd.DataFrame, str]:
    frame = load_market_spot_table()
    if frame.empty:
        return pd.DataFrame(), "spot_unavailable"
    selected = build_dynamic_pool_from_frame(frame, params)
    if selected.empty:
        return pd.DataFrame(), "empty_dynamic_pool"
    return selected, get_spot_provider_route(frame)


def build_snapshots_from_pool_frame(
    frame: pd.DataFrame,
    raw_spot_frame: pd.DataFrame,
) -> list[OvernightSnapshot]:
    normalized_raw = _normalize_frame(raw_spot_frame)
    code_col = _find_column(normalized_raw, [CODE_COLUMN, "股票代码"])
    latest_col = _find_column(normalized_raw, [LATEST_COLUMN])
    pre_close_col = _find_column(normalized_raw, [PRE_CLOSE_COLUMN])
    open_col = _find_column(normalized_raw, [OPEN_COLUMN])
    high_col = _find_column(normalized_raw, [HIGH_COLUMN])
    low_col = _find_column(normalized_raw, [LOW_COLUMN])
    amount_col = _find_column(normalized_raw, [AMOUNT_COLUMN, "成交金额"])
    turnover_col = _find_column(normalized_raw, [TURNOVER_COLUMN])
    name_col = _find_column(normalized_raw, [NAME_COLUMN, "股票名称"])

    if not all([code_col, latest_col, pre_close_col, open_col, high_col, low_col, amount_col, name_col]):
        return []

    raw_map = {
        str(row[code_col]).split(".", 1)[0].zfill(6): row.to_dict()
        for _, row in normalized_raw.iterrows()
    }
    snapshots: list[OvernightSnapshot] = []
    for _, row in frame.iterrows():
        code = str(row["code"]).zfill(6)
        raw = raw_map.get(code)
        if not raw:
            continue
        latest = _safe_float(raw.get(latest_col))
        pre_close = _safe_float(raw.get(pre_close_col))
        open_price = _safe_float(raw.get(open_col))
        high = _safe_float(raw.get(high_col))
        low = _safe_float(raw.get(low_col))
        amount = _safe_float(raw.get(amount_col))
        turnover = _safe_float(raw.get(turnover_col)) if turnover_col else 0.0
        if latest <= 0 or pre_close <= 0 or high <= 0 or low <= 0:
            continue

        pct = (latest - pre_close) / pre_close * 100 if pre_close > 0 else 0.0
        intraday_return = (latest - open_price) / open_price * 100 if open_price > 0 else 0.0
        position = (latest - low) / (high - low) * 100 if high > low else 50.0
        dist_to_high = (high - latest) / high * 100 if high > 0 else 999.0
        amplitude = (high - low) / low * 100 if low > 0 else 999.0

        profile = build_security_profile(code, "cn_a")
        snapshots.append(
            OvernightSnapshot(
                profile=profile,
                name=str(raw.get(name_col) or code),
                latest=latest,
                pre_close=pre_close,
                open_price=open_price,
                high=high,
                low=low,
                amount=amount,
                turnover=turnover,
                raw=raw,
                pct=round(pct, 4),
                intraday_return_from_open=round(intraday_return, 4),
                position=round(position, 4),
                dist_to_high=round(dist_to_high, 4),
                amplitude=round(amplitude, 4),
                pool=classify_pool(code),
            )
        )
    return snapshots
