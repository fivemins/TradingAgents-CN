from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

from tradingagents.market_utils import call_with_proxy_fallback

from .models import OvernightSnapshot, ScanParams


RISK_KEYWORDS = [
    "减持",
    "解禁",
    "业绩预亏",
    "业绩下降",
    "预警",
    "风险提示",
    "退市",
    "ST",
    "亏损",
    "诉讼",
    "处罚",
    "问询",
]


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


def load_risk_stocks(
    trade_date: str,
    cache_dir: Path,
    look_back_days: int = 7,
) -> tuple[set[str], dict[str, int]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"risk_{trade_date}.json"
    if cache_path.exists():
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return set(payload.get("codes", [])), payload.get("summary", {})

    target_date = datetime.strptime(trade_date, "%Y-%m-%d")
    risk_codes: set[str] = set()
    matched_rows = 0
    scanned_days = 0
    for offset in range(look_back_days):
        day = target_date - timedelta(days=offset)
        try:
            frame = call_with_proxy_fallback(ak.stock_gsrl_gsdt_em, date=day.strftime("%Y%m%d"))
        except Exception:
            continue
        normalized = _normalize_frame(frame)
        if normalized.empty:
            continue
        scanned_days += 1
        code_col = _find_column(normalized, ["代码", "证券代码", "股票代码"])
        title_col = _find_column(normalized, ["标题", "内容", "事件", "摘要"])
        if not code_col or not title_col:
            continue
        mask = normalized[title_col].astype(str).str.contains(
            "|".join(RISK_KEYWORDS),
            na=False,
            case=False,
        )
        filtered = normalized[mask]
        matched_rows += int(len(filtered))
        for code in filtered[code_col].astype(str):
            code6 = code.split(".", 1)[0].zfill(6)
            if code6.startswith(("0", "3")):
                risk_codes.add(f"{code6}.SZ")
            else:
                risk_codes.add(f"{code6}.SS")

    summary = {
        "matched_events": matched_rows,
        "risk_codes": len(risk_codes),
        "scanned_days": scanned_days,
    }
    cache_path.write_text(
        json.dumps({"codes": sorted(risk_codes), "summary": summary}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return risk_codes, summary


def check_buy_filters(
    snapshot: OvernightSnapshot,
    risk_stocks: set[str],
    params: ScanParams,
) -> tuple[bool, str]:
    if snapshot.code in risk_stocks:
        return False, "存在风险事件（减持/解禁/业绩/监管）"
    if snapshot.pct < params.min_tail_return:
        return False, f"涨幅 {snapshot.pct:.2f}% 低于最小阈值"
    if snapshot.pct > params.max_rise_4h:
        return False, f"涨幅 {snapshot.pct:.2f}% 过热"
    if snapshot.position < 40:
        return False, f"收盘位置 {snapshot.position:.1f}% 过低"
    if snapshot.dist_to_high > params.max_distance_high:
        return False, f"离日内高点 {snapshot.dist_to_high:.2f}% 过远"
    max_amplitude = (
        params.max_amplitude_main if snapshot.is_main else params.max_amplitude_gem
    )
    if snapshot.amplitude > max_amplitude:
        return False, f"振幅 {snapshot.amplitude:.2f}% 过大"
    if snapshot.amount < params.min_amount:
        return False, f"成交额 {snapshot.amount / 1e8:.2f} 亿不足"
    if snapshot.dist_to_limit is not None and snapshot.dist_to_limit < 1.5:
        return False, f"距涨停 {snapshot.dist_to_limit:.2f}% 过近"
    if snapshot.is_main and snapshot.turnover > params.turnover_overheat_main:
        return False, f"换手率 {snapshot.turnover:.2f}% 过热（主板）"
    if snapshot.is_gem_or_star and snapshot.turnover > params.turnover_overheat_gem:
        return False, f"换手率 {snapshot.turnover:.2f}% 过热（创业板/科创板）"
    return True, "通过"
