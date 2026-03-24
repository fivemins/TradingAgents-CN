import re

from langchain_openai import ChatOpenAI


ACTION_PATTERN = re.compile(
    r"(?:FINAL ACTION|FINAL RECOMMENDATION|FINAL TRANSACTION PROPOSAL|最终动作|最终建议|结论动作)"
    r"[^A-Z]{0,32}\**\b(BUY|SELL|HOLD)\b",
    re.IGNORECASE,
)

FALLBACK_ACTION_PATTERN = re.compile(r"\b(BUY|SELL|HOLD)\b", re.IGNORECASE)


class SignalProcessor:
    """Processes trading signals to extract actionable decisions."""

    def __init__(self, quick_thinking_llm: ChatOpenAI):
        self.quick_thinking_llm = quick_thinking_llm

    @staticmethod
    def extract_action(full_signal: str | None) -> str | None:
        text = str(full_signal or "").strip()
        if not text:
            return None

        match = ACTION_PATTERN.search(text)
        if match:
            return match.group(1).upper()

        tail = text[-240:]
        fallback_matches = FALLBACK_ACTION_PATTERN.findall(tail)
        if len(fallback_matches) == 1:
            return fallback_matches[0].upper()
        return None

    def process_signal(
        self,
        full_signal: str,
        structured_decision: dict | None = None,
    ) -> str:
        """Extract the final BUY/SELL/HOLD action.

        Medium constraint behavior:
        - Prefer an explicit action in the final note.
        - Fall back to an LLM extraction if the final note is ambiguous.
        - Use the structured decision only as the last fallback.
        """
        direct_action = self.extract_action(full_signal)
        if direct_action:
            return direct_action

        messages = [
            (
                "system",
                "You extract the final investment action from a decision note. "
                "Return only one token: BUY, SELL, or HOLD.",
            ),
            ("human", full_signal),
        ]
        llm_action = str(self.quick_thinking_llm.invoke(messages).content).strip().upper()
        if llm_action in {"BUY", "SELL", "HOLD"}:
            return llm_action

        if structured_decision and structured_decision.get("decision"):
            return str(structured_decision["decision"]).strip().upper()

        return "HOLD"

    def rewrite_signal(
        self,
        full_signal: str,
        structured_decision: dict | None = None,
    ) -> str:
        """Normalize the final note into a stable Markdown structure.

        Medium constraint means the structured conclusion is a strong reference,
        but the final action may differ if the final note explicitly overrides it.
        """
        structured_decision = structured_decision or {}
        structured_action = (
            str(structured_decision.get("decision", "")).strip().upper() or None
        )
        final_action = self.extract_action(full_signal) or structured_action or "HOLD"
        summary = str(structured_decision.get("summary", "")).strip()
        drivers = [str(item).strip() for item in structured_decision.get("primary_drivers") or [] if str(item).strip()]
        risks = [str(item).strip() for item in structured_decision.get("risk_flags") or [] if str(item).strip()]
        confidence = structured_decision.get("confidence")
        score = structured_decision.get("score")
        threshold_policy = structured_decision.get("threshold_policy") or {}
        trimmed_signal = " ".join(str(full_signal or "").split())

        policy_bits: list[str] = []
        if threshold_policy.get("buy_at_or_above") is not None:
            policy_bits.append(f"BUY >= {threshold_policy['buy_at_or_above']}")
        if threshold_policy.get("sell_at_or_below") is not None:
            policy_bits.append(f"SELL <= {threshold_policy['sell_at_or_below']}")
        if threshold_policy.get("min_confidence_for_directional_call") is not None:
            policy_bits.append(
                "Directional confidence >= "
                f"{threshold_policy['min_confidence_for_directional_call']}"
            )
        policy_line = " | ".join(policy_bits) if policy_bits else "No threshold policy available."

        summary_lines: list[str] = []
        if summary:
            summary_lines.append(summary)
        if structured_action and structured_action != final_action:
            summary_lines.append(
                f"结构化因子原本倾向 {structured_action}，但综合研究、交易与风控讨论后，"
                f"本次终稿动作调整为 {final_action}。"
            )
        elif structured_action:
            summary_lines.append(f"结构化因子结论与终稿动作保持一致，均为 {final_action}。")
        else:
            summary_lines.append(f"本次终稿动作为 {final_action}。")

        lines = [
            "# 最终交易决策",
            "",
            "## 结论摘要",
            " ".join(summary_lines).strip(),
        ]

        score_bits: list[str] = []
        if score is not None:
            score_bits.append(f"综合评分：{score}")
        if confidence is not None:
            score_bits.append(f"置信度：{confidence}")
        if score_bits:
            lines.append("；".join(score_bits))

        lines.extend(["", "## 主要利多"])
        if drivers:
            lines.extend([f"- {item}" for item in drivers[:3]])
        else:
            lines.append("- 当前未提取到明确的主要利多。")

        lines.extend(["", "## 主要风险"])
        if risks:
            lines.extend([f"- {item}" for item in risks[:3]])
        else:
            lines.append("- 当前未提取到显著风险标记。")

        execution_lines = [
            f"- 结构化参考阈值：{policy_line}",
            "- 最终动作以风控终稿为准；结构化因子用于提供分数、证据与边界条件。",
        ]
        if trimmed_signal:
            execution_lines.append(f"- 原始终稿摘要：{trimmed_signal[:600]}")

        lines.extend(["", "## 执行建议", *execution_lines, "", "## 最终动作", f"**{final_action}**"])
        return "\n".join(lines)
