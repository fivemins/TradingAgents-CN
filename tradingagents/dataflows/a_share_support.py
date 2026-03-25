from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Iterable

import akshare as ak
import pandas as pd
import yfinance as yf

from .config import get_config
from tradingagents.market_utils import SecurityProfile, call_with_proxy_fallback


def _safe_akshare_call(func, *args, **kwargs):
    try:
        result = call_with_proxy_fallback(func, *args, **kwargs)
    except Exception:
        return None
    if isinstance(result, pd.DataFrame) and result.empty:
        return None
    return result


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact_date(date_str: str) -> str:
    return date_str.replace("-", "")


def _normalize_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = [
            "_".join(str(part) for part in column if part not in ("", None)).strip("_")
            for column in normalized.columns
        ]
    else:
        normalized.columns = [str(column).strip() for column in normalized.columns]
    normalized = normalized.loc[:, ~normalized.columns.duplicated()]
    return normalized


def _get_series(frame: pd.DataFrame, column: str) -> pd.Series:
    value = frame[column]
    if isinstance(value, pd.DataFrame):
        return value.iloc[:, 0]
    return value


def _normalize_column_key(value: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value).strip().lower())


def _find_column(frame: pd.DataFrame, keywords: Iterable[str]) -> str | None:
    lowered = [keyword.lower() for keyword in keywords]
    normalized_keywords = {_normalize_column_key(keyword) for keyword in keywords}
    substring_matches: list[str] = []
    for column in frame.columns:
        normalized = str(column).strip().lower()
        normalized_key = _normalize_column_key(column)
        if normalized_key in normalized_keywords:
            return str(column)
        if any(keyword in normalized for keyword in lowered):
            substring_matches.append(str(column))
            continue
        if any(keyword and keyword in normalized_key for keyword in normalized_keywords):
            substring_matches.append(str(column))
    if substring_matches:
        return substring_matches[0]
    return None


def _filter_by_code(frame: pd.DataFrame | None, profile: SecurityProfile) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return normalized

    code_column = _find_column(normalized, ["代码", "证券代码", "股票代码", "security_code"])
    if not code_column:
        return normalized

    series = normalized[code_column].astype(str)
    filtered = normalized[
        series.str.endswith(profile.akshare_symbol)
        | (series == profile.akshare_symbol)
        | (series == profile.eastmoney_symbol)
    ]
    return filtered.reset_index(drop=True)


def _trim_frame(frame: pd.DataFrame, rows: int = 8, columns: list[str] | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame
    trimmed = frame.iloc[:rows].copy()
    if columns:
        keep = [column for column in columns if column in trimmed.columns]
        if keep:
            trimmed = trimmed[keep]
    return trimmed


def _frame_to_markdown(
    title: str,
    frame: pd.DataFrame,
    rows: int = 8,
    columns: list[str] | None = None,
) -> str:
    if frame.empty:
        return ""
    trimmed = _trim_frame(frame, rows=rows, columns=columns)
    return f"## {title}\n{trimmed.to_markdown(index=False)}\n"


def _standardize_history(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return normalized

    adjusted_close_columns = [
        column
        for column in normalized.columns
        if _normalize_column_key(column) in {"adjclose", "adjustedclose"}
    ]
    close_columns = [
        column
        for column in normalized.columns
        if _normalize_column_key(column) == "close"
    ]
    if adjusted_close_columns and close_columns:
        normalized = normalized.drop(columns=adjusted_close_columns)

    mapping_candidates = {
        "Date": ["日期", "交易日期", "date"],
        "Open": ["开盘", "open"],
        "High": ["最高", "high"],
        "Low": ["最低", "low"],
        "Close": ["收盘", "close"],
        "Volume": ["成交量", "volume"],
        "Turnover": ["成交额", "amount", "turnover"],
        "Amplitude": ["振幅", "amplitude"],
        "TurnoverRate": ["换手率", "turnoverrate"],
    }

    rename_map: dict[str, str] = {}
    for canonical, keywords in mapping_candidates.items():
        column = _find_column(normalized, keywords)
        if column:
            rename_map[column] = canonical

    normalized = normalized.rename(columns=rename_map)
    normalized = normalized.loc[:, ~normalized.columns.duplicated()].copy()
    if "Date" in normalized.columns:
        normalized["Date"] = pd.to_datetime(_get_series(normalized, "Date"), errors="coerce").dt.strftime("%Y-%m-%d")
    for column in ["Open", "High", "Low", "Close", "Volume", "Turnover", "Amplitude", "TurnoverRate"]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(_get_series(normalized, column), errors="coerce")
    if "Date" in normalized.columns:
        normalized = normalized.dropna(subset=["Date"]).drop_duplicates(subset=["Date"], keep="last")
    return normalized.reset_index(drop=True)


def _a_share_history_cache_path(profile: SecurityProfile, start_date: str, end_date: str) -> Path:
    config = get_config()
    cache_root = Path(config["data_cache_dir"]) / "a_share_history"
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root / f"{profile.normalized_ticker}-{start_date}-{end_date}.csv"


def _load_cached_history(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame()
    try:
        return _standardize_history(pd.read_csv(cache_path))
    except Exception:
        try:
            cache_path.unlink()
        except FileNotFoundError:
            pass
        return pd.DataFrame()


def _save_cached_history(cache_path: Path, frame: pd.DataFrame) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(cache_path, index=False, encoding="utf-8-sig")


def _download_yfinance_history(
    profile: SecurityProfile,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    attempts: list[pd.DataFrame] = []

    def _try_download() -> pd.DataFrame:
        frame = call_with_proxy_fallback(
            yf.download,
            profile.yfinance_symbol,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            multi_level_index=False,
            threads=False,
        )
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return frame.reset_index()
        return pd.DataFrame()

    def _try_history() -> pd.DataFrame:
        ticker = yf.Ticker(profile.yfinance_symbol)
        frame = call_with_proxy_fallback(
            ticker.history,
            start=start_date,
            end=end_date,
            auto_adjust=False,
        )
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return frame.reset_index()
        return pd.DataFrame()

    for loader in (_try_download, _try_download, _try_history, _try_history):
        try:
            frame = loader()
        except Exception:
            continue
        if not frame.empty:
            attempts.append(frame)
            break

    if not attempts:
        return pd.DataFrame()

    return _standardize_history(attempts[0])


def _load_history_with_fallback(
    profile: SecurityProfile,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    cache_path = _a_share_history_cache_path(profile, start_date, end_date)
    cached = _load_cached_history(cache_path)
    if not cached.empty:
        return cached

    yfinance_history = _download_yfinance_history(profile, start_date, end_date)
    if not yfinance_history.empty:
        _save_cached_history(cache_path, yfinance_history)
        return yfinance_history

    return pd.DataFrame()


def get_a_share_history(
    profile: SecurityProfile,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    history = _safe_akshare_call(
        ak.stock_zh_a_hist,
        symbol=profile.akshare_symbol,
        period="daily",
        start_date=_compact_date(start_date),
        end_date=_compact_date(end_date),
        adjust="qfq",
    )
    if isinstance(history, pd.DataFrame) and not history.empty:
        standardized = _standardize_history(history)
        if not standardized.empty:
            _save_cached_history(_a_share_history_cache_path(profile, start_date, end_date), standardized)
            return standardized

    return _load_history_with_fallback(profile, start_date, end_date)


def get_a_share_individual_info(profile: SecurityProfile) -> pd.DataFrame:
    info = _safe_akshare_call(ak.stock_individual_info_em, symbol=profile.akshare_symbol)
    return _normalize_frame(info)


def get_a_share_research_reports(profile: SecurityProfile) -> pd.DataFrame:
    reports = _safe_akshare_call(ak.stock_research_report_em, symbol=profile.akshare_symbol)
    return _normalize_frame(reports)


def get_a_share_holder_snapshot(profile: SecurityProfile) -> pd.DataFrame:
    holders = _safe_akshare_call(ak.stock_zh_a_gdhs_detail_em, symbol=profile.akshare_symbol)
    return _normalize_frame(holders)


def get_a_share_financial_indicators(profile: SecurityProfile) -> pd.DataFrame:
    indicators = _safe_akshare_call(
        ak.stock_financial_analysis_indicator_em,
        symbol=profile.eastmoney_symbol,
    )
    return _normalize_frame(indicators)


def get_a_share_hot_rank(profile: SecurityProfile) -> pd.DataFrame:
    hot_rank = _safe_akshare_call(ak.stock_hot_rank_em)
    if not isinstance(hot_rank, pd.DataFrame):
        return pd.DataFrame()
    return _filter_by_code(hot_rank, profile)


def get_a_share_company_events(
    profile: SecurityProfile,
    curr_date: str,
    look_back_days: int = 7,
) -> pd.DataFrame:
    target_date = datetime.strptime(curr_date, "%Y-%m-%d")
    frames: list[pd.DataFrame] = []
    for offset in range(look_back_days):
        day = target_date - timedelta(days=offset)
        daily = _safe_akshare_call(ak.stock_gsrl_gsdt_em, date=day.strftime("%Y%m%d"))
        filtered = _filter_by_code(daily, profile)
        if not filtered.empty:
            frames.append(filtered)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True).drop_duplicates()
    return combined.reset_index(drop=True)


def build_a_share_market_report(
    profile: SecurityProfile,
    start_date: str,
    end_date: str,
) -> str:
    history = get_a_share_history(profile, start_date, end_date)
    info = get_a_share_individual_info(profile)
    hot_rank = get_a_share_hot_rank(profile)

    report_parts = [
        f"# {profile.normalized_ticker} 的 A 股市场背景",
        f"- 市场区域：cn_a",
        f"- 交易所：{profile.exchange}",
        f"- YFinance 代码：{profile.yfinance_symbol}",
        f"- Akshare 代码：{profile.akshare_symbol}",
    ]
    report_parts.append(_frame_to_markdown("公司基础快照", info, rows=8))
    report_parts.append(_frame_to_markdown("热度排行快照", hot_rank, rows=3))
    report_parts.append(
        _frame_to_markdown(
            f"近期价格历史（{start_date} 到 {end_date}）",
            history,
            rows=20,
            columns=["Date", "Open", "High", "Low", "Close", "Volume", "TurnoverRate"],
        )
    )
    return "\n\n".join(part for part in report_parts if part)


def build_a_share_sentiment_report(profile: SecurityProfile, curr_date: str) -> str:
    hot_rank = get_a_share_hot_rank(profile)
    holders = get_a_share_holder_snapshot(profile)
    events = get_a_share_company_events(profile, curr_date, look_back_days=5)

    report_parts = [
        f"# {profile.normalized_ticker} 的 A 股关注度与情绪快照",
        "本报告优先使用公开热度、股东户数变化以及短期公司事件信号，而不是 Reddit 风格的海外社交数据。",
    ]
    report_parts.append(_frame_to_markdown("热度排行信号", hot_rank, rows=3))
    report_parts.append(_frame_to_markdown("股东户数趋势", holders, rows=5))
    report_parts.append(_frame_to_markdown("近期公司事件", events, rows=5))
    return "\n\n".join(part for part in report_parts if part)


def build_a_share_news_report(profile: SecurityProfile, curr_date: str) -> str:
    events = get_a_share_company_events(profile, curr_date, look_back_days=7)
    reports = get_a_share_research_reports(profile)

    report_parts = [
        f"# {profile.normalized_ticker} 的 A 股公司与政策敏感新闻快照",
        "对于 A 股，上市公司公告、公司事件和券商研报通常比泛海外宏观新闻更具决策价值。",
    ]
    report_parts.append(_frame_to_markdown("近期公司事件", events, rows=8))
    report_parts.append(_frame_to_markdown("近期券商研报标题", reports, rows=5))
    return "\n\n".join(part for part in report_parts if part)


def build_a_share_fundamentals_report(profile: SecurityProfile, curr_date: str) -> str:
    info = get_a_share_individual_info(profile)
    indicators = get_a_share_financial_indicators(profile)
    reports = get_a_share_research_reports(profile)
    holders = get_a_share_holder_snapshot(profile)

    report_parts = [
        f"# {profile.normalized_ticker} 的 A 股基本面快照",
        f"- 分析日期：{curr_date}",
    ]
    report_parts.append(_frame_to_markdown("公司基础画像", info, rows=10))
    report_parts.append(_frame_to_markdown("财务分析指标", indicators, rows=4))
    report_parts.append(_frame_to_markdown("券商一致预期", reports, rows=5))
    report_parts.append(_frame_to_markdown("股东集中度", holders, rows=5))
    return "\n\n".join(part for part in report_parts if part)


def get_a_share_factor_inputs(
    profile: SecurityProfile,
    curr_date: str,
) -> dict[str, Any]:
    end_date = datetime.strptime(curr_date, "%Y-%m-%d")
    start_date = (end_date - timedelta(days=160)).strftime("%Y-%m-%d")
    end_inclusive = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
    return {
        "history": get_a_share_history(profile, start_date, end_inclusive),
        "info": get_a_share_individual_info(profile),
        "research_reports": get_a_share_research_reports(profile),
        "holders": get_a_share_holder_snapshot(profile),
        "events": get_a_share_company_events(profile, curr_date, look_back_days=7),
        "financial_indicators": get_a_share_financial_indicators(profile),
        "hot_rank": get_a_share_hot_rank(profile),
    }


def extract_basic_company_info(info: pd.DataFrame) -> dict[str, Any]:
    if info.empty:
        return {}
    item_column = _find_column(info, ["item"])
    value_column = _find_column(info, ["value"])
    if not item_column or not value_column:
        return {}
    return dict(zip(info[item_column], info[value_column], strict=False))


def latest_holder_delta(holders: pd.DataFrame) -> dict[str, float | None]:
    normalized = _normalize_frame(holders)
    if normalized.empty:
        return {
            "holder_change_pct": None,
            "holder_market_value": None,
        }

    latest = normalized.iloc[0]
    change_column = _find_column(normalized, ["增减比例", "change"])
    market_value_column = _find_column(normalized, ["户均持股市值", "market value"])
    return {
        "holder_change_pct": _coerce_float(latest.get(change_column)) if change_column else None,
        "holder_market_value": _coerce_float(latest.get(market_value_column)) if market_value_column else None,
    }


def latest_financial_row(indicators: pd.DataFrame) -> dict[str, Any]:
    normalized = _normalize_frame(indicators)
    if normalized.empty:
        return {}
    return normalized.iloc[0].to_dict()
