from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import math
from typing import Any

import pandas as pd
import yfinance as yf

from tradingagents.dataflows.a_share_support import (
    extract_basic_company_info,
    get_a_share_factor_inputs,
    latest_financial_row,
    latest_holder_delta,
)
from tradingagents.market_utils import SecurityProfile, build_security_profile


FACTOR_WEIGHTS = {
    "technical": 0.30,
    "sentiment": 0.15,
    "news": 0.25,
    "fundamentals": 0.30,
}

OVERNIGHT_FACTOR_WEIGHTS = {
    "technical": 0.25,
    "sentiment": 0.15,
    "news": 0.20,
    "fundamentals": 0.25,
    "overnight_tail": 0.15,
}

THRESHOLD_POLICY = {
    "style": "balanced",
    "buy_at_or_above": 65.0,
    "sell_at_or_below": 45.0,
    "min_confidence_for_directional_call": 0.50,
}

POSITIVE_TONE_KEYWORDS = (
    "beat", "bullish", "buy", "growth", "improving", "outperform", "strong", "upside",
    "surprise", "expand", "订单", "中标", "增长", "改善", "超预期", "回购", "增持", "看好",
    "上调", "分红", "预增", "扭亏",
)
NEGATIVE_TONE_KEYWORDS = (
    "bearish", "decline", "deteriorating", "downgrade", "investigation", "lawsuit",
    "miss", "sell", "warning", "weak", "亏损", "下滑", "减持", "诉讼", "问询", "处罚",
    "风险", "停牌", "下调", "预亏", "预减",
)
POSITIVE_EVENT_KEYWORDS = (
    "订单", "中标", "签订", "合作", "回购", "增持", "分红", "预增", "扭亏", "获批", "扩产",
    "提价", "buyback", "dividend", "contract", "order", "guidance raise",
)
NEGATIVE_EVENT_KEYWORDS = (
    "减持", "诉讼", "亏损", "下滑", "预亏", "预减", "停牌", "违约", "冻结", "质押", "立案",
    "lawsuit", "loss", "warning",
)
REGULATORY_EVENT_KEYWORDS = (
    "问询", "处罚", "监管", "立案", "调查", "停牌", "风险提示", "问询函", "处罚决定",
    "inquiry", "penalty", "regulator", "investigation",
)


@dataclass
class EvidenceItem:
    signal: str
    value: Any
    impact: str
    source: str
    weight: float

    def to_dict(self) -> dict[str, Any]:
        value = round(self.value, 4) if isinstance(self.value, float) else self.value
        return {
            "signal": self.signal,
            "value": value,
            "impact": self.impact,
            "source": self.source,
            "weight": round(self.weight, 4),
        }


@dataclass
class Subscore:
    score: float
    value: Any
    summary: str
    weight: float

    def to_dict(self) -> dict[str, Any]:
        value = self.value
        if isinstance(value, float):
            value = round(value, 4)
        elif isinstance(value, dict):
            value = {
                key: round(item, 4) if isinstance(item, float) else item
                for key, item in value.items()
            }
        return {
            "score": round(self.score, 2),
            "value": value,
            "summary": self.summary,
            "weight": round(self.weight, 4),
        }


@dataclass
class ScoreBlock:
    score: float
    confidence: float
    summary: str
    subscores: dict[str, Subscore]
    strengths: list[EvidenceItem] = field(default_factory=list)
    risks: list[EvidenceItem] = field(default_factory=list)
    raw_metrics: list[EvidenceItem] = field(default_factory=list)
    confidence_drivers: list[str] = field(default_factory=list)

    def to_factor_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 2),
            "confidence": round(self.confidence, 2),
            "summary": self.summary,
            "subscores": {name: item.to_dict() for name, item in self.subscores.items()},
            "top_positive_signals": [item.to_dict() for item in _trim_items(self.strengths, 3)],
            "top_negative_signals": [item.to_dict() for item in _trim_items(self.risks, 3)],
            "confidence_drivers": self.confidence_drivers[:4],
        }

    def to_evidence_dict(self) -> dict[str, Any]:
        return {
            "strengths": [item.to_dict() for item in _trim_items(self.strengths, 5)],
            "risks": [item.to_dict() for item in _trim_items(self.risks, 5)],
            "raw_metrics": [item.to_dict() for item in self.raw_metrics[:5]],
        }


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, str):
        normalized = value.strip().replace(",", "").replace("%", "")
        if normalized in {"", "--", "nan", "None"}:
            return None
        value = normalized
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clip(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def _scale_to_score(
    value: float | None,
    lower: float,
    upper: float,
    *,
    reverse: bool = False,
    missing_score: float = 50.0,
) -> float:
    if value is None or lower == upper:
        return missing_score
    normalized = _clip((value - lower) / (upper - lower), 0.0, 1.0)
    if reverse:
        normalized = 1.0 - normalized
    return round(15.0 + normalized * 70.0, 2)


def _weighted_average(items: list[tuple[float, float]]) -> float:
    valid = [(score, weight) for score, weight in items if weight > 0]
    if not valid:
        return 50.0
    total_weight = sum(weight for _, weight in valid)
    return sum(score * weight for score, weight in valid) / total_weight


def _signal_weight(score: float, base_weight: float) -> float:
    return round(abs(score - 50.0) / 50.0 * base_weight, 4)


def _keyword_hits(text: str, keywords: tuple[str, ...]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


def _keyword_balance(text: str) -> tuple[int, int]:
    return len(_keyword_hits(text, POSITIVE_TONE_KEYWORDS)), len(_keyword_hits(text, NEGATIVE_TONE_KEYWORDS))


def _tone_score(positive_hits: int, negative_hits: int, *, scale: float = 4.5, cap: float = 18.0) -> float:
    return _clip(50.0 + max(-cap, min(cap, (positive_hits - negative_hits) * scale)))


def _normalize_columns(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    return normalized


def _find_column(frame: pd.DataFrame, keywords: tuple[str, ...] | list[str]) -> str | None:
    lowered = [keyword.lower() for keyword in keywords]
    for column in frame.columns:
        normalized = str(column).strip().lower()
        if any(keyword in normalized for keyword in lowered):
            return str(column)
    return None


def _get_first_series(frame: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
    for candidate in candidates:
        if candidate in frame.columns:
            return frame[candidate]
    fallback = _find_column(frame, candidates)
    return frame[fallback] if fallback else None


def _row_text(row: pd.Series) -> str:
    parts: list[str] = []
    for value in row.tolist():
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "--"}:
            parts.append(text)
    return " | ".join(parts)


def _trim_items(items: list[EvidenceItem], limit: int) -> list[EvidenceItem]:
    return sorted(items, key=lambda item: item.weight, reverse=True)[:limit]


def _lookup_mapping_value(mapping: dict[str, Any], keywords: tuple[str, ...]) -> Any:
    for key, value in mapping.items():
        key_text = str(key).lower()
        if any(keyword.lower() in key_text for keyword in keywords):
            return value
    return None


def _broker_coverage_score(report_count: int) -> float:
    if report_count <= 0:
        return 50.0
    capped = min(report_count, 200)
    normalized = math.log1p(capped) / math.log1p(200)
    return round(50.0 + normalized * 18.0, 2)


def _score_overnight_tail(overnight_context: dict[str, Any] | None) -> ScoreBlock | None:
    context = dict(overnight_context or {})
    if not context:
        return None

    quality = str(context.get("quality") or "missing").strip().lower()
    if quality == "missing":
        return None

    factor_breakdown = context.get("factor_breakdown") or {}
    tail_metrics = context.get("tail_metrics") or {}
    provider_route = context.get("provider_route") or {}

    tail_strength = _safe_float(factor_breakdown.get("tail_strength"))
    total_score = _safe_float(context.get("total_score"))
    quick_score = _safe_float(context.get("quick_score"))
    tail_return = _safe_float(tail_metrics.get("tail_return_pct"))
    tail_amount_ratio = _safe_float(tail_metrics.get("tail_amount_ratio"))
    last10_return = _safe_float(tail_metrics.get("last10_return_pct"))
    close_at_high = _safe_float(tail_metrics.get("close_at_high_ratio"))
    auction_strength = _safe_float(tail_metrics.get("auction_strength"))

    tail_signal_score = _weighted_average(
        [
            (_clip((tail_strength or 0.0) / 22.0 * 100.0), 0.35),
            (_scale_to_score(tail_return, -0.2, 2.2), 0.20),
            (_scale_to_score(last10_return, -0.1, 1.2), 0.15),
            (_scale_to_score(close_at_high, 0.3, 1.0), 0.15),
            (_scale_to_score(tail_amount_ratio, 0.04, 0.24), 0.10),
            (_scale_to_score(auction_strength, -0.1, 0.8), 0.05),
        ]
    )
    scan_alignment_score = _weighted_average(
        [
            (_clip(total_score or 50.0), 0.65),
            (_clip(quick_score or 50.0), 0.35),
        ]
    )
    quality_score = {
        "real": 82.0,
        "proxy": 55.0,
        "invalid": 45.0,
    }.get(quality, 50.0)

    subscore_weights = {
        "tail_signal_strength": 0.55,
        "scan_alignment": 0.30,
        "tail_quality": 0.15,
    }
    subscores = {
        "tail_signal_strength": Subscore(
            round(tail_signal_score, 2),
            {
                "tail_strength": round(tail_strength or 0.0, 2),
                "tail_return_pct": round(tail_return or 0.0, 4),
                "last10_return_pct": round(last10_return or 0.0, 4),
                "close_at_high_ratio": round(close_at_high or 0.0, 4),
                "tail_amount_ratio": round(tail_amount_ratio or 0.0, 4),
            },
            "隔夜扫描中的尾盘强度与收盘结构，作为短期延续性的辅助信号。",
            subscore_weights["tail_signal_strength"],
        ),
        "scan_alignment": Subscore(
            round(scan_alignment_score, 2),
            {
                "quick_score": round(quick_score or 0.0, 2),
                "total_score": round(total_score or 0.0, 2),
            },
            "扫描阶段的候选总分和预筛分，反映其在隔夜规则引擎中的相对位置。",
            subscore_weights["scan_alignment"],
        ),
        "tail_quality": Subscore(
            round(quality_score, 2),
            {
                "quality": quality,
                "provider_route": provider_route,
                "source": tail_metrics.get("source"),
            },
            "真实尾盘分钟数据优先，代理尾盘只做轻量参考。",
            subscore_weights["tail_quality"],
        ),
    }

    block_score = _weighted_average([(item.score, item.weight) for item in subscores.values()])
    available_components = sum(
        value is not None
        for value in (tail_strength, total_score, quick_score, tail_return, last10_return, close_at_high)
    )
    confidence = _confidence_from_coverage(
        available_components,
        6,
        floor=0.22,
        ceiling=0.80,
        bonus=0.04 if quality == "real" else 0.0,
    )
    if quality == "proxy":
        block_score = min(block_score, 60.0)
        confidence = max(0.2, round(confidence - 0.15, 2))
    elif quality == "invalid":
        block_score = min(block_score, 50.0)
        confidence = max(0.18, round(confidence - 0.20, 2))

    quality_label = {"real": "真实尾盘", "proxy": "代理尾盘", "invalid": "无效尾盘"}.get(quality, quality)
    strengths: list[EvidenceItem] = []
    risks: list[EvidenceItem] = []
    if quality == "real" and block_score >= 60:
        strengths.append(
            EvidenceItem(
                "尾盘来源",
                f"{quality_label} / total_score {total_score or 0.0:.1f}",
                "positive",
                "overnight_scan",
                _signal_weight(block_score, 0.12),
            )
        )
        if tail_return is not None:
            strengths.append(
                EvidenceItem(
                    "尾盘收益",
                    f"14:30 后涨幅 {tail_return:.2f}%",
                    "positive",
                    "overnight_scan",
                    _signal_weight(tail_signal_score, 0.10),
                )
            )
    if quality == "proxy":
        risks.append(
            EvidenceItem(
                "尾盘质量",
                "当前仅有 proxy tail，信号只做轻量参考",
                "negative",
                "overnight_scan",
                0.12,
            )
        )
    if block_score <= 45:
        risks.append(
            EvidenceItem(
                "尾盘延续性",
                f"尾盘综合分 {block_score:.1f}，对隔夜延续的支持有限",
                "negative",
                "overnight_scan",
                _signal_weight(block_score, 0.10),
            )
        )

    raw_metrics = [
        EvidenceItem("候选总分", round(total_score or 0.0, 2), "neutral", "overnight_scan", 0.0),
        EvidenceItem("预筛分", round(quick_score or 0.0, 2), "neutral", "overnight_scan", 0.0),
        EvidenceItem("尾盘质量", quality_label, "neutral", "overnight_scan", 0.0),
    ]
    if tail_return is not None:
        raw_metrics.append(EvidenceItem("14:30后涨幅(%)", round(tail_return, 4), "neutral", "overnight_scan", 0.0))
    if last10_return is not None:
        raw_metrics.append(EvidenceItem("最后10分钟涨幅(%)", round(last10_return, 4), "neutral", "overnight_scan", 0.0))
    if close_at_high is not None:
        raw_metrics.append(EvidenceItem("收盘靠近尾盘高点", round(close_at_high, 4), "neutral", "overnight_scan", 0.0))

    confidence_drivers = [
        f"候选分层: {context.get('bucket') or '--'}",
        f"尾盘质量: {quality_label}",
    ]
    if tail_metrics.get("source"):
        confidence_drivers.append(f"尾盘来源: {tail_metrics.get('source')}")

    return ScoreBlock(
        score=round(block_score, 2),
        confidence=confidence,
        summary=f"隔夜尾盘因子 {block_score:.1f} 分，{quality_label}作为单票深度分析的辅助证据。",
        subscores=subscores,
        strengths=strengths,
        risks=risks,
        raw_metrics=raw_metrics,
        confidence_drivers=confidence_drivers[:4],
    )


def _confidence_from_coverage(
    available_components: int,
    expected_components: int,
    *,
    floor: float = 0.28,
    ceiling: float = 0.88,
    bonus: float = 0.0,
) -> float:
    if expected_components <= 0:
        return floor
    coverage = available_components / expected_components
    confidence = floor + coverage * (ceiling - floor) + bonus
    return round(min(ceiling, confidence), 2)


def _score_label(score: float) -> str:
    if score >= 65:
        return "偏强"
    if score <= 35:
        return "偏弱"
    return "中性"


def _build_us_factor_inputs(profile: SecurityProfile, trade_date: str) -> dict[str, Any]:
    end_date = datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)
    start_date = end_date - timedelta(days=180)
    ticker = yf.Ticker(profile.yfinance_symbol)
    history = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
    )
    if history is not None and not history.empty:
        history = history.reset_index()
        history["Date"] = pd.to_datetime(history["Date"]).dt.strftime("%Y-%m-%d")
    try:
        info = ticker.info or {}
    except Exception:
        info = {}
    return {
        "history": history if isinstance(history, pd.DataFrame) else pd.DataFrame(),
        "info": info,
        "research_reports": pd.DataFrame(),
        "holders": pd.DataFrame(),
        "events": pd.DataFrame(),
        "financial_indicators": pd.DataFrame(),
        "hot_rank": pd.DataFrame(),
    }


def _classify_event_text(text: str) -> dict[str, Any]:
    positive_hits = _keyword_hits(text, POSITIVE_EVENT_KEYWORDS)
    negative_hits = _keyword_hits(text, NEGATIVE_EVENT_KEYWORDS)
    regulatory_hits = _keyword_hits(text, REGULATORY_EVENT_KEYWORDS)
    if regulatory_hits:
        return {"category": "regulatory", "keywords": regulatory_hits, "weight": -1.0}
    if len(negative_hits) > len(positive_hits):
        return {"category": "negative", "keywords": negative_hits, "weight": -0.8}
    if positive_hits:
        return {"category": "positive", "keywords": positive_hits, "weight": 0.8}
    if negative_hits:
        return {"category": "negative", "keywords": negative_hits, "weight": -0.8}
    return {"category": "neutral", "keywords": [], "weight": 0.0}


def _summarize_events(events: pd.DataFrame) -> dict[str, Any]:
    normalized = _normalize_columns(events)
    summary = {
        "total": int(len(normalized)),
        "positive": 0,
        "negative": 0,
        "regulatory": 0,
        "neutral": 0,
        "positive_examples": [],
        "negative_examples": [],
        "regulatory_examples": [],
    }
    if normalized.empty:
        return summary
    for _, row in normalized.head(20).iterrows():
        text = _row_text(row)
        if not text:
            continue
        classified = _classify_event_text(text)
        category = classified["category"]
        summary[category] += 1
        example_key = f"{category}_examples"
        if example_key in summary and len(summary[example_key]) < 3:
            summary[example_key].append(text[:80])
    return summary


def _score_technical(history: pd.DataFrame) -> ScoreBlock:
    history = _normalize_columns(history)
    close_series = _get_first_series(history, ["Close", "close", "收盘"])
    volume_series = _get_first_series(history, ["Volume", "volume", "成交量"])

    if close_series is None:
        return ScoreBlock(
            score=50.0,
            confidence=0.2,
            summary="缺少可识别的收盘价序列，技术面暂时保持中性。",
            subscores={},
            raw_metrics=[EvidenceItem("收盘价序列", "缺失", "neutral", "price_history", 0.0)],
            confidence_drivers=["行情字段不完整"],
        )

    close = pd.to_numeric(close_series, errors="coerce").dropna()
    volume = (
        pd.to_numeric(volume_series, errors="coerce").dropna()
        if volume_series is not None
        else pd.Series(dtype="float64")
    )
    if len(close) < 20:
        return ScoreBlock(
            score=50.0,
            confidence=0.22,
            summary="行情样本少于 20 个交易日，技术面不具备稳定判断条件。",
            subscores={},
            raw_metrics=[EvidenceItem("可用交易日", float(len(close)), "neutral", "price_history", 0.0)],
            confidence_drivers=["可用历史样本过短"],
        )

    last_close = float(close.iloc[-1])
    sma20 = float(close.tail(20).mean())
    sma60 = float(close.tail(min(60, len(close))).mean())
    return20 = float((close.iloc[-1] / close.iloc[-20] - 1) * 100) if len(close) >= 20 else None
    return60 = float((close.iloc[-1] / close.iloc[-60] - 1) * 100) if len(close) >= 60 else None
    volatility20 = float(close.pct_change().dropna().tail(20).std() * 100) if len(close) >= 21 else None
    volume_ratio = None
    if len(volume) >= 20:
        avg_volume_20 = float(volume.tail(20).mean())
        if avg_volume_20:
            volume_ratio = float(volume.tail(5).mean() / avg_volume_20)

    trend_signal = 0.0
    trend_signal += 0.7 if last_close >= sma20 else -0.7
    trend_signal += 0.9 if last_close >= sma60 else -0.9
    trend_signal += 0.6 if sma20 >= sma60 else -0.6
    trend_score = _clip(50.0 + trend_signal * 16.0)

    momentum_parts = [
        _scale_to_score(return20, -12.0, 18.0),
        _scale_to_score(return60, -18.0, 28.0),
    ]
    momentum_score = round(sum(momentum_parts) / len(momentum_parts), 2)
    volatility_score = _scale_to_score(volatility20, 1.5, 7.5, reverse=True)

    volume_score = 50.0
    if volume_ratio is not None:
        if (return20 or 0.0) >= 0:
            volume_score = _clip(50.0 + (volume_ratio - 1.0) * 55.0)
        else:
            volume_score = _clip(50.0 - (volume_ratio - 1.0) * 55.0)

    subscore_weights = {
        "trend_strength": 0.35,
        "momentum": 0.30,
        "volatility_state": 0.20,
        "volume_confirmation": 0.15,
    }
    subscores = {
        "trend_strength": Subscore(
            round(trend_score, 2),
            {"last_close": round(last_close, 2), "sma20": round(sma20, 2), "sma60": round(sma60, 2)},
            f"收盘价相对均线的结构{_score_label(trend_score)}。",
            subscore_weights["trend_strength"],
        ),
        "momentum": Subscore(
            round(momentum_score, 2),
            {"return20_pct": round(return20 or 0.0, 2), "return60_pct": round(return60 or 0.0, 2)},
            f"20 日与 60 日动量整体{_score_label(momentum_score)}。",
            subscore_weights["momentum"],
        ),
        "volatility_state": Subscore(
            round(volatility_score, 2),
            {"volatility20_pct": round(volatility20 or 0.0, 2)},
            f"20 日波动率处于{_score_label(volatility_score)}区间。",
            subscore_weights["volatility_state"],
        ),
        "volume_confirmation": Subscore(
            round(volume_score, 2),
            {"volume_ratio_5_20": round(volume_ratio or 0.0, 2)},
            "量价配合用于确认短期趋势是否有成交支持。",
            subscore_weights["volume_confirmation"],
        ),
    }
    block_score = _weighted_average([(item.score, item.weight) for item in subscores.values()])

    strengths: list[EvidenceItem] = []
    risks: list[EvidenceItem] = []
    labels = {
        "trend_strength": "趋势强弱",
        "momentum": "近期动量",
        "volatility_state": "波动状态",
        "volume_confirmation": "量价配合",
    }
    for key, item in subscores.items():
        if item.score >= 60:
            value = {
                "trend_strength": f"最新收盘 {last_close:.2f}，位于 SMA20 {sma20:.2f} 与 SMA60 {sma60:.2f} 上方" if last_close >= sma20 and last_close >= sma60 else f"均线结构改善，最新收盘 {last_close:.2f}",
                "momentum": f"20 日收益率 {return20 or 0.0:.2f}%，60 日收益率 {return60 or 0.0:.2f}%",
                "volatility_state": f"20 日波动率 {volatility20 or 0.0:.2f}%，处于可控区间",
                "volume_confirmation": f"5/20 日量比 {volume_ratio or 0.0:.2f}，趋势具备成交支撑",
            }[key]
            strengths.append(
                EvidenceItem(labels[key], value, "positive", "price_history", _signal_weight(item.score, item.weight))
            )
        elif item.score <= 40:
            value = {
                "trend_strength": f"最新收盘 {last_close:.2f}，相对 SMA20 {sma20:.2f} / SMA60 {sma60:.2f} 偏弱",
                "momentum": f"20 日收益率 {return20 or 0.0:.2f}%，60 日收益率 {return60 or 0.0:.2f}%",
                "volatility_state": f"20 日波动率 {volatility20 or 0.0:.2f}%，短线波动偏大",
                "volume_confirmation": f"5/20 日量比 {volume_ratio or 0.0:.2f}，量价配合不足",
            }[key]
            risks.append(
                EvidenceItem(labels[key], value, "negative", "price_history", _signal_weight(item.score, item.weight))
            )

    raw_metrics = [
        EvidenceItem("最新收盘价", round(last_close, 2), "neutral", "price_history", 0.0),
        EvidenceItem("SMA20", round(sma20, 2), "neutral", "price_history", 0.0),
        EvidenceItem("SMA60", round(sma60, 2), "neutral", "price_history", 0.0),
        EvidenceItem("20日收益率(%)", round(return20 or 0.0, 2), "neutral", "price_history", 0.0),
        EvidenceItem("60日收益率(%)", round(return60 or 0.0, 2), "neutral", "price_history", 0.0),
        EvidenceItem("20日波动率(%)", round(volatility20 or 0.0, 2), "neutral", "price_history", 0.0),
    ]
    if volume_ratio is not None:
        raw_metrics.append(EvidenceItem("5日/20日量比", round(volume_ratio, 2), "neutral", "price_history", 0.0))

    available_components = 2 + int(volatility20 is not None) + int(volume_ratio is not None)
    bonus = 0.12 if len(close) >= 120 else 0.06 if len(close) >= 60 else 0.0
    confidence = _confidence_from_coverage(available_components, 4, bonus=bonus)
    confidence_drivers = []
    if len(close) >= 120:
        confidence_drivers.append("近 120 个交易日行情完整")
    elif len(close) >= 60:
        confidence_drivers.append("近 60 个交易日行情可用")
    if volume_ratio is not None:
        confidence_drivers.append("量能数据可用于验证趋势")
    if volatility20 is not None:
        confidence_drivers.append("短期波动率可稳定计算")

    summary = (
        f"技术面综合 {block_score:.1f} 分，趋势 {_score_label(trend_score)}，"
        f"动量 {_score_label(momentum_score)}，20 日波动率约 {volatility20 or 0.0:.2f}% 。"
    )
    return ScoreBlock(
        score=round(block_score, 2),
        confidence=confidence,
        summary=summary,
        subscores=subscores,
        strengths=strengths,
        risks=risks,
        raw_metrics=raw_metrics,
        confidence_drivers=confidence_drivers or ["技术面可用信息有限"],
    )


def _score_sentiment(
    profile: SecurityProfile,
    sentiment_report: str,
    hot_rank: pd.DataFrame,
    holders: pd.DataFrame,
) -> ScoreBlock:
    hot_rank = _normalize_columns(hot_rank)
    rank_value = None
    if not hot_rank.empty:
        rank_column = _find_column(hot_rank, ("排名", "rank"))
        if rank_column:
            rank_value = _safe_float(hot_rank.iloc[0].get(rank_column))

    holder_metrics = latest_holder_delta(holders)
    holder_change_pct = _safe_float(holder_metrics.get("holder_change_pct"))
    positive_hits, negative_hits = _keyword_balance(sentiment_report)

    attention_score = _scale_to_score(rank_value, 5.0, 120.0, reverse=True)
    concentration_score = _scale_to_score(holder_change_pct, -12.0, 15.0, reverse=True)
    tone_score = _tone_score(positive_hits, negative_hits, scale=3.8, cap=16.0)

    subscore_weights = {
        "attention_heat": 0.40,
        "holder_concentration": 0.35,
        "sentiment_balance": 0.25,
    }
    subscores = {
        "attention_heat": Subscore(
            round(attention_score, 2),
            {"hot_rank": rank_value},
            "个股热度排名越靠前，短期关注度越强。",
            subscore_weights["attention_heat"],
        ),
        "holder_concentration": Subscore(
            round(concentration_score, 2),
            {"holder_change_pct": round(holder_change_pct or 0.0, 2)},
            "股东户数下降通常意味着筹码更集中，上升则反映交易拥挤风险。",
            subscore_weights["holder_concentration"],
        ),
        "sentiment_balance": Subscore(
            round(tone_score, 2),
            {"positive_hits": positive_hits, "negative_hits": negative_hits},
            "情绪报告关键词仅作辅助，用于修正公众讨论方向。",
            subscore_weights["sentiment_balance"],
        ),
    }
    block_score = _weighted_average([(item.score, item.weight) for item in subscores.values()])

    strengths: list[EvidenceItem] = []
    risks: list[EvidenceItem] = []
    if rank_value is not None:
        if attention_score >= 60:
            strengths.append(
                EvidenceItem("个股热度", f"热度排名约 {int(rank_value)}", "positive", "eastmoney_hot_rank", _signal_weight(attention_score, subscore_weights["attention_heat"]))
            )
        elif attention_score <= 40:
            risks.append(
                EvidenceItem("个股热度", f"热度排名约 {int(rank_value)}，市场关注度偏低", "negative", "eastmoney_hot_rank", _signal_weight(attention_score, subscore_weights["attention_heat"]))
            )
    if holder_change_pct is not None:
        if concentration_score >= 60:
            strengths.append(
                EvidenceItem("股东户数变化", f"股东户数变动 {holder_change_pct:.2f}%", "positive", "eastmoney_shareholders", _signal_weight(concentration_score, subscore_weights["holder_concentration"]))
            )
        elif concentration_score <= 40:
            risks.append(
                EvidenceItem("股东户数变化", f"股东户数变动 {holder_change_pct:.2f}%", "negative", "eastmoney_shareholders", _signal_weight(concentration_score, subscore_weights["holder_concentration"]))
            )
    if positive_hits or negative_hits:
        target_list = strengths if tone_score >= 55 else risks if tone_score <= 45 else None
        if target_list is not None:
            target_list.append(
                EvidenceItem(
                    "情绪语义",
                    f"正面关键词 {positive_hits}，负面关键词 {negative_hits}",
                    "positive" if target_list is strengths else "negative",
                    "sentiment_report",
                    _signal_weight(tone_score, subscore_weights["sentiment_balance"]),
                )
            )

    raw_metrics = []
    if rank_value is not None:
        raw_metrics.append(EvidenceItem("个股热度排名", float(rank_value), "neutral", "eastmoney_hot_rank", 0.0))
    if holder_change_pct is not None:
        raw_metrics.append(EvidenceItem("股东户数变化(%)", round(holder_change_pct, 2), "neutral", "eastmoney_shareholders", 0.0))
    raw_metrics.append(
        EvidenceItem("情绪关键词平衡", {"positive": positive_hits, "negative": negative_hits}, "neutral", "sentiment_report", 0.0)
    )

    available_components = int(rank_value is not None) + int(holder_change_pct is not None) + int(bool(sentiment_report.strip()))
    confidence = _confidence_from_coverage(
        available_components,
        3,
        floor=0.24,
        ceiling=0.82,
        bonus=0.05 if rank_value is not None and holder_change_pct is not None else 0.0,
    )
    confidence_drivers = []
    if rank_value is not None:
        confidence_drivers.append("存在东财热度排名数据")
    if holder_change_pct is not None:
        confidence_drivers.append("存在股东户数变化数据")
    if sentiment_report.strip():
        confidence_drivers.append("情绪报告可用于关键词平衡修正")

    summary = f"{profile.display_symbol} 的情绪面综合 {block_score:.1f} 分，热度 {_score_label(attention_score)}，筹码结构 {_score_label(concentration_score)}。"
    return ScoreBlock(
        score=round(block_score, 2),
        confidence=confidence,
        summary=summary,
        subscores=subscores,
        strengths=strengths,
        risks=risks,
        raw_metrics=raw_metrics,
        confidence_drivers=confidence_drivers or ["情绪面数据覆盖有限"],
    )


def _score_news(news_report: str, events: pd.DataFrame, research_reports: pd.DataFrame) -> ScoreBlock:
    event_summary = _summarize_events(events)
    report_count = int(len(research_reports)) if isinstance(research_reports, pd.DataFrame) else 0
    positive_hits, negative_hits = _keyword_balance(news_report)

    event_signal_score = _clip(
        50.0
        + event_summary["positive"] * 7.0
        - event_summary["negative"] * 8.0
        - event_summary["regulatory"] * 10.0
        + min(event_summary["neutral"], 2) * 1.0
    )
    coverage_score = _broker_coverage_score(report_count)
    tone_score = _tone_score(positive_hits, negative_hits, scale=2.5, cap=12.0)

    subscore_weights = {
        "company_events": 0.50,
        "broker_coverage": 0.25,
        "news_tone": 0.25,
    }
    subscores = {
        "company_events": Subscore(
            round(event_signal_score, 2),
            {
                "positive_events": event_summary["positive"],
                "negative_events": event_summary["negative"],
                "regulatory_events": event_summary["regulatory"],
            },
            "公司事件与公告优先级高于泛宏观新闻。",
            subscore_weights["company_events"],
        ),
        "broker_coverage": Subscore(
            round(coverage_score, 2),
            {"broker_report_count": report_count},
            "研报覆盖采用对数缩放，只反映研究关注度，不直接主导结论。",
            subscore_weights["broker_coverage"],
        ),
        "news_tone": Subscore(
            round(tone_score, 2),
            {"positive_hits": positive_hits, "negative_hits": negative_hits},
            "新闻文本语义只做小权重修正。",
            subscore_weights["news_tone"],
        ),
    }
    block_score = _weighted_average([(item.score, item.weight) for item in subscores.values()])

    strengths: list[EvidenceItem] = []
    risks: list[EvidenceItem] = []
    if event_summary["positive"] > 0:
        strengths.append(
            EvidenceItem(
                "公司事件",
                f"近 7 日利好事件 {event_summary['positive']} 条",
                "positive",
                "company_events",
                max(0.08, _signal_weight(event_signal_score, subscore_weights["company_events"])),
            )
        )
    if event_summary["negative"] > 0:
        risks.append(
            EvidenceItem(
                "公司事件",
                f"近 7 日利空事件 {event_summary['negative']} 条",
                "negative",
                "company_events",
                max(0.08, _signal_weight(event_signal_score, subscore_weights["company_events"])),
            )
        )
    if event_summary["regulatory"] > 0:
        risks.append(
            EvidenceItem(
                "监管事项",
                f"近 7 日监管类事件 {event_summary['regulatory']} 条",
                "negative",
                "company_events",
                max(0.12, _signal_weight(event_signal_score, subscore_weights["company_events"])),
            )
        )
    if report_count > 0 and coverage_score >= 58:
        strengths.append(
            EvidenceItem(
                "券商覆盖",
                f"近期研报覆盖 {report_count} 条（已对数缩放）",
                "positive",
                "broker_research",
                _signal_weight(coverage_score, subscore_weights["broker_coverage"]),
            )
        )
    if positive_hits or negative_hits:
        target_list = strengths if tone_score >= 55 else risks if tone_score <= 45 else None
        if target_list is not None:
            target_list.append(
                EvidenceItem(
                    "新闻语义",
                    f"正面关键词 {positive_hits}，负面关键词 {negative_hits}",
                    "positive" if target_list is strengths else "negative",
                    "news_report",
                    _signal_weight(tone_score, subscore_weights["news_tone"]),
                )
            )

    raw_metrics = [
        EvidenceItem("公司事件总数", float(event_summary["total"]), "neutral", "company_events", 0.0),
        EvidenceItem("利好事件数", float(event_summary["positive"]), "neutral", "company_events", 0.0),
        EvidenceItem("利空事件数", float(event_summary["negative"]), "neutral", "company_events", 0.0),
        EvidenceItem("监管事件数", float(event_summary["regulatory"]), "neutral", "company_events", 0.0),
        EvidenceItem("券商研报条数", float(report_count), "neutral", "broker_research", 0.0),
        EvidenceItem("新闻关键词平衡", {"positive": positive_hits, "negative": negative_hits}, "neutral", "news_report", 0.0),
    ]

    available_components = int(event_summary["total"] > 0) + int(report_count > 0) + int(bool(news_report.strip()))
    confidence = _confidence_from_coverage(
        available_components,
        3,
        floor=0.22,
        ceiling=0.84,
        bonus=0.04 if event_summary["total"] > 0 and report_count > 0 else 0.0,
    )
    confidence_drivers = []
    if event_summary["total"] > 0:
        confidence_drivers.append("存在近期公司事件数据")
    if report_count > 0:
        confidence_drivers.append("存在券商研报覆盖数据")
    if news_report.strip():
        confidence_drivers.append("新闻分析报告可用于语义修正")

    summary = f"新闻/公告维度 {block_score:.1f} 分，公司事件 {_score_label(event_signal_score)}，研报覆盖 {_score_label(coverage_score)}。"
    return ScoreBlock(
        score=round(block_score, 2),
        confidence=confidence,
        summary=summary,
        subscores=subscores,
        strengths=strengths,
        risks=risks,
        raw_metrics=raw_metrics,
        confidence_drivers=confidence_drivers or ["新闻证据覆盖不足"],
    )


def _score_fundamentals(
    profile: SecurityProfile,
    info: Any,
    financial_indicators: pd.DataFrame,
) -> ScoreBlock:
    company_info = extract_basic_company_info(info) if isinstance(info, pd.DataFrame) else dict(info or {})
    metrics = latest_financial_row(financial_indicators)

    roe = _safe_float(metrics.get("ROEJQ"))
    revenue_growth = _safe_float(metrics.get("YYZSRGDHBZC"))
    profit_growth = _safe_float(metrics.get("PARENTNETPROFITTZ"))
    gross_margin = _safe_float(metrics.get("XSMLL"))
    debt_ratio = _safe_float(metrics.get("ZCFZL"))
    trailing_pe = _safe_float(_lookup_mapping_value(company_info, ("市盈率", "trailing p/e", "pe")))

    roe_score = _scale_to_score(roe, 2.0, 24.0)
    margin_score = _scale_to_score(gross_margin, 12.0, 65.0)
    profitability_score = _weighted_average([(roe_score, 0.55), (margin_score, 0.45)])

    revenue_score = _scale_to_score(revenue_growth, -10.0, 20.0)
    profit_score = _scale_to_score(profit_growth, -15.0, 25.0)
    growth_score = _weighted_average([(revenue_score, 0.45), (profit_score, 0.55)])

    balance_score = _scale_to_score(debt_ratio, 20.0, 80.0, reverse=True)
    valuation_score = _scale_to_score(trailing_pe, 8.0, 60.0, reverse=True)

    subscore_weights = {
        "profitability": 0.35,
        "growth": 0.30,
        "balance_sheet": 0.20,
        "valuation": 0.15,
    }
    subscores = {
        "profitability": Subscore(
            round(profitability_score, 2),
            {"roe_pct": roe, "gross_margin_pct": gross_margin},
            "盈利质量由 ROE 与毛利率共同决定。",
            subscore_weights["profitability"],
        ),
        "growth": Subscore(
            round(growth_score, 2),
            {"revenue_growth_pct": revenue_growth, "profit_growth_pct": profit_growth},
            "增长项关注营收与利润的同比增速。",
            subscore_weights["growth"],
        ),
        "balance_sheet": Subscore(
            round(balance_score, 2),
            {"debt_ratio_pct": debt_ratio},
            "资产负债率越低，资产负债表韧性通常越强。",
            subscore_weights["balance_sheet"],
        ),
        "valuation": Subscore(
            round(valuation_score, 2),
            {"trailing_pe": trailing_pe},
            "估值仅作轻权重修正，不直接主导结论。",
            subscore_weights["valuation"],
        ),
    }
    block_score = _weighted_average([(item.score, item.weight) for item in subscores.values()])

    strengths: list[EvidenceItem] = []
    risks: list[EvidenceItem] = []
    labels = {
        "profitability": "盈利能力",
        "growth": "成长性",
        "balance_sheet": "资产负债表",
        "valuation": "估值水平",
    }
    for key, item in subscores.items():
        source = "financial_indicators" if key != "valuation" else "company_profile"
        if item.score >= 60:
            value = {
                "profitability": f"ROE {roe or 0.0:.2f}%，毛利率 {gross_margin or 0.0:.2f}%",
                "growth": f"营收增速 {revenue_growth or 0.0:.2f}%，利润增速 {profit_growth or 0.0:.2f}%",
                "balance_sheet": f"资产负债率 {debt_ratio or 0.0:.2f}%，财务结构稳健",
                "valuation": f"动态市盈率 {trailing_pe or 0.0:.2f}",
            }[key]
            strengths.append(EvidenceItem(labels[key], value, "positive", source, _signal_weight(item.score, item.weight)))
        elif item.score <= 40:
            value = {
                "profitability": f"ROE {roe or 0.0:.2f}%，毛利率 {gross_margin or 0.0:.2f}%，盈利质量偏弱",
                "growth": f"营收增速 {revenue_growth or 0.0:.2f}%，利润增速 {profit_growth or 0.0:.2f}%",
                "balance_sheet": f"资产负债率 {debt_ratio or 0.0:.2f}%，杠杆压力偏高",
                "valuation": f"动态市盈率 {trailing_pe or 0.0:.2f}，估值约束较强",
            }[key]
            risks.append(EvidenceItem(labels[key], value, "negative", source, _signal_weight(item.score, item.weight)))

    raw_metrics = []
    if roe is not None:
        raw_metrics.append(EvidenceItem("ROE(%)", round(roe, 2), "neutral", "financial_indicators", 0.0))
    if revenue_growth is not None:
        raw_metrics.append(EvidenceItem("营收增速(%)", round(revenue_growth, 2), "neutral", "financial_indicators", 0.0))
    if profit_growth is not None:
        raw_metrics.append(EvidenceItem("利润增速(%)", round(profit_growth, 2), "neutral", "financial_indicators", 0.0))
    if gross_margin is not None:
        raw_metrics.append(EvidenceItem("毛利率(%)", round(gross_margin, 2), "neutral", "financial_indicators", 0.0))
    if debt_ratio is not None:
        raw_metrics.append(EvidenceItem("资产负债率(%)", round(debt_ratio, 2), "neutral", "financial_indicators", 0.0))
    if trailing_pe is not None:
        raw_metrics.append(EvidenceItem("动态市盈率", round(trailing_pe, 2), "neutral", "company_profile", 0.0))

    metric_count = sum(value is not None for value in (roe, revenue_growth, profit_growth, gross_margin, debt_ratio, trailing_pe))
    confidence = _confidence_from_coverage(
        metric_count,
        6,
        floor=0.24,
        ceiling=0.88,
        bonus=0.06 if metric_count >= 5 else 0.0,
    )
    confidence_drivers = []
    if roe is not None and gross_margin is not None:
        confidence_drivers.append("盈利能力指标齐全")
    if revenue_growth is not None and profit_growth is not None:
        confidence_drivers.append("营收与利润增速可用")
    if debt_ratio is not None:
        confidence_drivers.append("资产负债率可用于稳健性判断")
    if trailing_pe is not None:
        confidence_drivers.append("估值数据可用于轻权重修正")

    summary = f"{profile.display_symbol} 的基本面综合 {block_score:.1f} 分，盈利能力 {_score_label(profitability_score)}，成长性 {_score_label(growth_score)}。"
    return ScoreBlock(
        score=round(block_score, 2),
        confidence=confidence,
        summary=summary,
        subscores=subscores,
        strengths=strengths,
        risks=risks,
        raw_metrics=raw_metrics,
        confidence_drivers=confidence_drivers or ["财务指标覆盖不足"],
    )


def _composite_decision(
    blocks: dict[str, ScoreBlock],
    factor_weights: dict[str, float] | None = None,
) -> tuple[float, float, str, dict[str, Any]]:
    active_weights = factor_weights or FACTOR_WEIGHTS
    composite = sum(
        blocks[name].score * weight
        for name, weight in active_weights.items()
        if name in blocks
    )
    confidence = sum(
        blocks[name].confidence * weight
        for name, weight in active_weights.items()
        if name in blocks
    )

    all_strengths: list[tuple[str, EvidenceItem]] = []
    all_risks: list[tuple[str, EvidenceItem]] = []
    for factor_name, block in blocks.items():
        all_strengths.extend((factor_name, item) for item in _trim_items(block.strengths, 2))
        all_risks.extend((factor_name, item) for item in _trim_items(block.risks, 2))

    primary_drivers = [
        f"{factor_name}: {item.signal} - {item.value}"
        for factor_name, item in sorted(all_strengths, key=lambda pair: pair[1].weight, reverse=True)[:3]
    ]
    risk_flags = [
        f"{factor_name}: {item.signal} - {item.value}"
        for factor_name, item in sorted(all_risks, key=lambda pair: pair[1].weight, reverse=True)[:3]
    ]

    if confidence < THRESHOLD_POLICY["min_confidence_for_directional_call"]:
        decision = "HOLD"
        summary = (
            f"综合得分 {composite:.2f}，但整体置信度只有 {confidence:.2f}，"
            f"低于 {THRESHOLD_POLICY['min_confidence_for_directional_call']:.2f} 的方向性阈值，因此降级为 HOLD。"
        )
    elif composite >= THRESHOLD_POLICY["buy_at_or_above"]:
        decision = "BUY"
        summary = f"综合得分 {composite:.2f}，高于买入阈值 {THRESHOLD_POLICY['buy_at_or_above']:.0f}，更偏向积极配置。"
    elif composite <= THRESHOLD_POLICY["sell_at_or_below"]:
        decision = "SELL"
        summary = f"综合得分 {composite:.2f}，低于卖出阈值 {THRESHOLD_POLICY['sell_at_or_below']:.0f}，当前应以风险控制为主。"
    else:
        decision = "HOLD"
        summary = (
            f"综合得分 {composite:.2f}，介于 {THRESHOLD_POLICY['sell_at_or_below']:.0f} 和 "
            f"{THRESHOLD_POLICY['buy_at_or_above']:.0f} 之间，信号仍偏混合。"
        )

    return composite, confidence, decision, {
        "summary": summary,
        "primary_drivers": primary_drivers,
        "risk_flags": risk_flags,
    }


def build_structured_analysis(
    final_state: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    market_region = final_state.get("market_region") or config.get("market_region") or "cn_a"
    trade_date = final_state.get("trade_date", "")
    profile_payload = final_state.get("security_profile")
    if profile_payload:
        profile = SecurityProfile(**profile_payload)
    else:
        profile = build_security_profile(final_state["company_of_interest"], market_region)

    factor_inputs = (
        get_a_share_factor_inputs(profile, trade_date)
        if profile.market_region == "cn_a"
        else _build_us_factor_inputs(profile, trade_date)
    )

    technical = _score_technical(factor_inputs.get("history", pd.DataFrame()))
    sentiment = _score_sentiment(
        profile,
        final_state.get("sentiment_report", ""),
        factor_inputs.get("hot_rank", pd.DataFrame()),
        factor_inputs.get("holders", pd.DataFrame()),
    )
    news = _score_news(
        final_state.get("news_report", ""),
        factor_inputs.get("events", pd.DataFrame()),
        factor_inputs.get("research_reports", pd.DataFrame()),
    )
    fundamentals = _score_fundamentals(
        profile,
        factor_inputs.get("info", {}),
        factor_inputs.get("financial_indicators", pd.DataFrame()),
    )
    overnight_context = final_state.get("overnight_context") or config.get("overnight_context") or {}
    overnight_tail = _score_overnight_tail(overnight_context)

    blocks = {
        "technical": technical,
        "sentiment": sentiment,
        "news": news,
        "fundamentals": fundamentals,
    }
    if overnight_tail is not None:
        blocks["overnight_tail"] = overnight_tail
    active_weights = OVERNIGHT_FACTOR_WEIGHTS if "overnight_tail" in blocks else FACTOR_WEIGHTS
    composite_score, confidence, decision, rationale = _composite_decision(blocks, active_weights)

    strategy = (
        "a_share_balanced_v3_overnight"
        if profile.market_region == "cn_a" and "overnight_tail" in blocks
        else "a_share_balanced_v2"
        if profile.market_region == "cn_a"
        else "global_balanced_v2"
    )
    factor_snapshot = {
        "strategy": strategy,
        "market_region": profile.market_region,
        "symbol": profile.normalized_ticker,
        "display_symbol": profile.display_symbol,
        "trade_date": trade_date,
        "composite_score": round(composite_score, 2),
        "confidence": round(confidence, 2),
        "recommended_action": decision,
        "scores": {name: block.to_factor_dict() for name, block in blocks.items()},
    }
    evidence_snapshot = {
        "strategy": strategy,
        "security_profile": profile.to_dict(),
        "technical": technical.to_evidence_dict(),
        "sentiment": sentiment.to_evidence_dict(),
        "news": news.to_evidence_dict(),
        "fundamentals": fundamentals.to_evidence_dict(),
    }
    if overnight_tail is not None:
        evidence_snapshot["overnight_tail"] = overnight_tail.to_evidence_dict()
    structured_decision = {
        "decision": decision,
        "score": round(composite_score, 2),
        "confidence": round(confidence, 2),
        "summary": rationale["summary"],
        "source": "balanced_factor_snapshot_v3" if "overnight_tail" in blocks else "balanced_factor_snapshot_v2",
        "threshold_policy": THRESHOLD_POLICY,
        "primary_drivers": rationale["primary_drivers"],
        "risk_flags": rationale["risk_flags"],
    }
    return {
        "security_profile": profile.to_dict(),
        "factor_snapshot": factor_snapshot,
        "evidence_snapshot": evidence_snapshot,
        "structured_decision": structured_decision,
    }
