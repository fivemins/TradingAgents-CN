from __future__ import annotations

import unittest

from dashboard_api.runtime import build_structured_summary
from tradingagents.graph.signal_processing import SignalProcessor


class DummyLLM:
    def __init__(self, response: str = "HOLD"):
        self.response = response

    def invoke(self, messages):  # pragma: no cover - simple stub
        class Result:
            def __init__(self, content: str):
                self.content = content

        return Result(self.response)


class SignalProcessingTests(unittest.TestCase):
    def test_rewrite_signal_allows_override_when_final_note_is_explicit(self) -> None:
        processor = SignalProcessor(DummyLLM())
        markdown = processor.rewrite_signal(
            "Risk review sees elevated downside. FINAL ACTION: SELL",
            {
                "decision": "BUY",
                "score": 67.2,
                "confidence": 0.71,
                "summary": "The weighted factors remain above the buy threshold.",
                "threshold_policy": {
                    "style": "balanced",
                    "buy_at_or_above": 65,
                    "sell_at_or_below": 45,
                    "min_confidence_for_directional_call": 0.5,
                },
                "primary_drivers": ["fundamentals: 盈利能力 - ROE 18.00%，毛利率 35.00%"],
                "risk_flags": ["news: 监管事项 - 近 7 日监管类事件 1 条"],
            },
        )

        self.assertIn("## 最终动作", markdown)
        self.assertIn("**SELL**", markdown)
        self.assertIn("结构化因子原本倾向 BUY", markdown)
        self.assertIn("主要风险", markdown)

    def test_process_signal_prefers_explicit_final_action(self) -> None:
        processor = SignalProcessor(DummyLLM("BUY"))
        decision = processor.process_signal(
            "Mixed note.\nFINAL ACTION: HOLD",
            {"decision": "BUY"},
        )
        self.assertEqual(decision, "HOLD")

    def test_build_structured_summary_uses_final_action_when_provided(self) -> None:
        summary = build_structured_summary(
            {
                "composite_score": 61.3,
                "confidence": 0.66,
                "recommended_action": "HOLD",
                "scores": {
                    "technical": {
                        "top_positive_signals": [
                            {
                                "signal": "趋势强弱",
                                "value": "站上SMA20",
                                "impact": "positive",
                                "source": "price_history",
                                "weight": 0.15,
                            }
                        ],
                        "top_negative_signals": [
                            {
                                "signal": "近期动量",
                                "value": "20日收益率 -3.50%",
                                "impact": "negative",
                                "source": "price_history",
                                "weight": 0.16,
                            }
                        ],
                    }
                },
            },
            {
                "decision": "HOLD",
                "primary_drivers": ["technical: 趋势强弱 - 站上SMA20"],
                "risk_flags": ["technical: 近期动量 - 20日收益率 -3.50%"],
            },
            final_action="SELL",
        )

        self.assertEqual(summary["recommended_action"], "SELL")
        self.assertIn("站上SMA20", summary["primary_driver"])
        self.assertIn("-3.50%", summary["primary_risk"])


if __name__ == "__main__":
    unittest.main()
