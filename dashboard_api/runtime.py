from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from tradingagents.text_cleaning import clean_structure, clean_text


REPORT_FILE_MAP = {
    "market_report": "market.md",
    "sentiment_report": "sentiment.md",
    "news_report": "news.md",
    "fundamentals_report": "fundamentals.md",
    "trader_investment_plan": "trader_plan.md",
    "investment_plan": "investment_plan.md",
    "final_trade_decision": "final_decision.md",
}

STAGE_BY_KEY = {
    "market_report": "market",
    "sentiment_report": "social",
    "news_report": "news",
    "fundamentals_report": "fundamentals",
    "investment_debate_state": "research",
    "trader_investment_plan": "trader",
    "risk_debate_state": "risk",
    # Keep the task in the risk stage until the runner finishes
    # post-processing and marks it succeeded/completed in one write.
    "final_trade_decision": "risk",
}

TRACKED_KEYS = [
    "final_trade_decision",
    "risk_debate_state",
    "trader_investment_plan",
    "investment_debate_state",
    "fundamentals_report",
    "news_report",
    "sentiment_report",
    "market_report",
]


def extract_content_string(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
                elif item.get("type") == "tool_use":
                    text_parts.append(f"[Tool: {item.get('name', 'unknown')}]")
            else:
                text_parts.append(str(item))
        return " ".join(text_parts)
    return str(content)


def append_event(log_path: Path, event_type: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp} [{event_type}] {message.strip()}\n")


def normalize_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def write_report_files(
    report_dir: Path,
    chunk: dict[str, Any],
    written_values: dict[str, str],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    for key, filename in REPORT_FILE_MAP.items():
        value = chunk.get(key)
        if not value:
            continue
        serialized = normalize_value(value)
        if serialized == written_values.get(key):
            continue
        written_values[key] = serialized
        target_dir = report_dir if filename != "final_decision.md" else report_dir.parent
        (target_dir / filename).write_text(str(value), encoding="utf-8")


def detect_stage_update(
    chunk: dict[str, Any],
    seen_values: dict[str, str],
) -> tuple[str | None, str | None]:
    for key in TRACKED_KEYS:
        value = chunk.get(key)
        if not value:
            continue
        serialized = normalize_value(value)
        if serialized == seen_values.get(key):
            continue
        seen_values[key] = serialized
        return STAGE_BY_KEY[key], stage_message(key, value)
    return None, None


def stage_message(key: str, value: Any) -> str:
    if key == "investment_debate_state":
        decision = value.get("judge_decision", "")
        current = value.get("current_response", "")
        if decision:
            return "Research manager finalized the investment debate."
        if current:
            return "Bull and bear researchers are debating the thesis."
        return "Research debate started."
    if key == "risk_debate_state":
        if value.get("judge_decision"):
            return "Portfolio manager completed the risk review."
        for field, label in (
            ("current_risky_response", "Risky analyst"),
            ("current_safe_response", "Safe analyst"),
            ("current_neutral_response", "Neutral analyst"),
        ):
            if value.get(field):
                return f"{label} updated the risk discussion."
        return "Risk review started."
    if key == "trader_investment_plan":
        return "Trader synthesized an investment plan."
    if key == "final_trade_decision":
        return "Final trade decision is ready."
    label_map = {
        "market_report": "Market analysis report generated.",
        "sentiment_report": "Sentiment analysis report generated.",
        "news_report": "News analysis report generated.",
        "fundamentals_report": "Fundamentals report generated.",
    }
    return label_map[key]


def summarize_message(chunk: dict[str, Any]) -> str | None:
    messages = chunk.get("messages") or []
    if not messages:
        return None
    last_message = messages[-1]
    content = extract_content_string(getattr(last_message, "content", str(last_message)))
    content = " ".join(content.split())
    return content[:500] if content else None


def serialize_final_state(final_state: dict[str, Any]) -> dict[str, Any]:
    investment_state = final_state.get("investment_debate_state") or {}
    risk_state = final_state.get("risk_debate_state") or {}
    return {
        "company_of_interest": final_state.get("company_of_interest", ""),
        "trade_date": final_state.get("trade_date", ""),
        "market_region": final_state.get("market_region", ""),
        "security_profile": final_state.get("security_profile", {}),
        "source_context": clean_structure(final_state.get("source_context", {})),
        "overnight_context": clean_structure(final_state.get("overnight_context", {})),
        "market_report": final_state.get("market_report", ""),
        "sentiment_report": final_state.get("sentiment_report", ""),
        "news_report": final_state.get("news_report", ""),
        "fundamentals_report": final_state.get("fundamentals_report", ""),
        "factor_snapshot": final_state.get("factor_snapshot", {}),
        "evidence_snapshot": final_state.get("evidence_snapshot", {}),
        "structured_decision": final_state.get("structured_decision", {}),
        "investment_debate_state": {
            "bull_history": investment_state.get("bull_history", ""),
            "bear_history": investment_state.get("bear_history", ""),
            "history": investment_state.get("history", ""),
            "current_response": investment_state.get("current_response", ""),
            "judge_decision": investment_state.get("judge_decision", ""),
        },
        "trader_investment_decision": final_state.get("trader_investment_plan", ""),
        "risk_debate_state": {
            "risky_history": risk_state.get("risky_history", ""),
            "safe_history": risk_state.get("safe_history", ""),
            "neutral_history": risk_state.get("neutral_history", ""),
            "history": risk_state.get("history", ""),
            "judge_decision": risk_state.get("judge_decision", ""),
        },
        "investment_plan": final_state.get("investment_plan", ""),
        "final_trade_decision": final_state.get("final_trade_decision", ""),
    }


def _format_signal_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value[:3])
    if isinstance(value, dict):
        parts = [f"{key}: {_format_signal_value(item)}" for key, item in list(value.items())[:3]]
        return ", ".join(parts)
    return str(value)


def _signal_to_summary_text(signal: dict[str, Any] | None) -> str | None:
    if not signal:
        return None
    name = str(signal.get("signal", "")).strip()
    value = _format_signal_value(signal.get("value"))
    if name and value:
        return f"{name} - {value}"
    if name:
        return name
    return clean_text(value or None)


def build_structured_summary(
    factor_snapshot: dict[str, Any] | None,
    structured_decision: dict[str, Any] | None,
    final_action: str | None = None,
) -> dict[str, Any] | None:
    factor_snapshot = clean_structure(factor_snapshot or {})
    structured_decision = clean_structure(structured_decision or {})

    composite_score = factor_snapshot.get("composite_score", structured_decision.get("score"))
    confidence = structured_decision.get("confidence", factor_snapshot.get("confidence"))
    recommended_action = final_action or structured_decision.get(
        "decision", factor_snapshot.get("recommended_action")
    )

    primary_driver = None
    driver_candidates = structured_decision.get("primary_drivers") or []
    if driver_candidates:
        primary_driver = clean_text(str(driver_candidates[0]).strip())

    primary_risk = None
    risk_candidates = structured_decision.get("risk_flags") or []
    if risk_candidates:
        primary_risk = clean_text(str(risk_candidates[0]).strip())

    if not primary_driver or not primary_risk:
        for block in (factor_snapshot.get("scores") or {}).values():
            if not primary_driver:
                signals = block.get("top_positive_signals") or []
                if signals:
                    primary_driver = _signal_to_summary_text(signals[0])
            if not primary_risk:
                signals = block.get("top_negative_signals") or []
                if signals:
                    primary_risk = _signal_to_summary_text(signals[0])
            if primary_driver and primary_risk:
                break

    if (
        composite_score is None
        and confidence is None
        and recommended_action is None
        and not primary_driver
        and not primary_risk
    ):
        return None

    return {
        "composite_score": composite_score,
        "confidence": confidence,
        "recommended_action": recommended_action,
        "primary_driver": primary_driver,
        "primary_risk": primary_risk,
    }
