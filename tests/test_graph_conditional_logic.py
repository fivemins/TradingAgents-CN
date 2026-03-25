from __future__ import annotations

import unittest

from tradingagents.graph.conditional_logic import ConditionalLogic


class ConditionalLogicTests(unittest.TestCase):
    def test_should_continue_debate_routes_to_bear_after_bull(self) -> None:
        logic = ConditionalLogic(max_debate_rounds=2)
        state = {
            "investment_debate_state": {
                "count": 1,
                "latest_speaker": "Bull",
                "current_response": "多头研究员：继续看多。",
            }
        }

        self.assertEqual(logic.should_continue_debate(state), "Bear Researcher")

    def test_should_continue_debate_routes_to_bull_after_bear(self) -> None:
        logic = ConditionalLogic(max_debate_rounds=2)
        state = {
            "investment_debate_state": {
                "count": 1,
                "latest_speaker": "Bear",
                "current_response": "空头研究员：风险更大。",
            }
        }

        self.assertEqual(logic.should_continue_debate(state), "Bull Researcher")

    def test_should_continue_debate_uses_chinese_prefix_as_legacy_fallback(self) -> None:
        logic = ConditionalLogic(max_debate_rounds=2)
        state = {
            "investment_debate_state": {
                "count": 1,
                "current_response": "多头研究员：继续看多。",
            }
        }

        self.assertEqual(logic.should_continue_debate(state), "Bear Researcher")

    def test_should_continue_debate_routes_to_manager_after_limit(self) -> None:
        logic = ConditionalLogic(max_debate_rounds=1)
        state = {
            "investment_debate_state": {
                "count": 2,
                "latest_speaker": "Bear",
                "current_response": "空头研究员：风险更大。",
            }
        }

        self.assertEqual(logic.should_continue_debate(state), "Research Manager")


if __name__ == "__main__":
    unittest.main()
