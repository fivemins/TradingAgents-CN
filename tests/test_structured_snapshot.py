from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from tradingagents.structured_snapshot import (
    _broker_coverage_score,
    _classify_event_text,
    _composite_decision,
    build_structured_analysis,
)


class StructuredSnapshotTests(unittest.TestCase):
    def test_event_classifier_flags_positive_and_regulatory_signals(self) -> None:
        positive = _classify_event_text("公司发布回购方案并公告业绩预增")
        regulatory = _classify_event_text("公司收到监管问询函并涉及处罚")

        self.assertEqual(positive["category"], "positive")
        self.assertEqual(regulatory["category"], "regulatory")

    def test_broker_coverage_score_uses_log_scaling(self) -> None:
        moderate = _broker_coverage_score(12)
        extreme = _broker_coverage_score(743)

        self.assertGreater(extreme, moderate)
        self.assertLess(extreme, 70.0)

    def test_low_confidence_is_downgraded_to_hold(self) -> None:
        fake_block = {"score": 80.0, "confidence": 0.2, "strengths": [], "risks": []}
        blocks = {
            "technical": type("Block", (), fake_block)(),
            "sentiment": type("Block", (), fake_block)(),
            "news": type("Block", (), fake_block)(),
            "fundamentals": type("Block", (), fake_block)(),
        }

        composite, confidence, decision, rationale = _composite_decision(blocks)  # type: ignore[arg-type]

        self.assertGreater(composite, 65.0)
        self.assertLess(confidence, 0.5)
        self.assertEqual(decision, "HOLD")
        self.assertIn("HOLD", rationale["summary"])

    @patch("tradingagents.structured_snapshot.get_a_share_factor_inputs")
    def test_build_structured_analysis_returns_v2_shapes(self, mocked_inputs) -> None:
        history = pd.DataFrame(
            {
                "Date": pd.date_range("2025-01-01", periods=80, freq="B").strftime("%Y-%m-%d"),
                "Close": [100 + index * 0.3 for index in range(80)],
                "Volume": [1000 + index * 5 for index in range(80)],
            }
        )
        research_reports = pd.DataFrame({"title": [f"研报 {index}" for index in range(743)]})
        mocked_inputs.return_value = {
            "history": history,
            "info": pd.DataFrame({"item": ["市盈率动态"], "value": ["18.5"]}),
            "research_reports": research_reports,
            "holders": pd.DataFrame({"change": [-6.2], "market value": [1200000]}),
            "events": pd.DataFrame({"title": ["公司发布回购计划并公告业绩预增", "公司收到监管问询函"]}),
            "financial_indicators": pd.DataFrame([{"ROEJQ": 18.0, "YYZSRGDHBZC": 12.0, "PARENTNETPROFITTZ": 15.0, "XSMLL": 35.0, "ZCFZL": 48.0}]),
            "hot_rank": pd.DataFrame({"rank": [8]}),
        }

        structured = build_structured_analysis(
            {
                "company_of_interest": "600519",
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "sentiment_report": "市场热度提升，情绪偏正面。",
                "news_report": "公司回购带来正面预期，但问询函需要继续跟踪。",
                "fundamentals_report": "盈利能力稳健，增长保持韧性。",
            },
            {"market_region": "cn_a"},
        )

        factor_snapshot = structured["factor_snapshot"]
        evidence_snapshot = structured["evidence_snapshot"]
        structured_decision = structured["structured_decision"]

        self.assertIn("subscores", factor_snapshot["scores"]["technical"])
        self.assertIn("top_positive_signals", factor_snapshot["scores"]["news"])
        self.assertIn("strengths", evidence_snapshot["fundamentals"])
        self.assertIn("raw_metrics", evidence_snapshot["technical"])
        self.assertIn("threshold_policy", structured_decision)
        self.assertLess(factor_snapshot["scores"]["news"]["subscores"]["broker_coverage"]["score"], 70.0)

    @patch("tradingagents.structured_snapshot.get_a_share_factor_inputs")
    def test_overnight_tail_factor_is_added_for_overnight_sources(self, mocked_inputs) -> None:
        mocked_inputs.return_value = {
            "history": pd.DataFrame(
                {
                    "Date": pd.date_range("2025-01-01", periods=80, freq="B").strftime("%Y-%m-%d"),
                    "Close": [100 + index * 0.2 for index in range(80)],
                    "Volume": [1000 + index * 3 for index in range(80)],
                }
            ),
            "info": pd.DataFrame({"item": ["市盈率动态"], "value": ["16.5"]}),
            "research_reports": pd.DataFrame({"title": ["研报 1", "研报 2"]}),
            "holders": pd.DataFrame({"change": [-2.5], "market value": [1200000]}),
            "events": pd.DataFrame({"title": ["公司发布订单公告"]}),
            "financial_indicators": pd.DataFrame([{"ROEJQ": 16.0, "YYZSRGDHBZC": 10.0, "PARENTNETPROFITTZ": 12.0, "XSMLL": 30.0, "ZCFZL": 40.0}]),
            "hot_rank": pd.DataFrame({"rank": [12]}),
        }

        structured = build_structured_analysis(
            {
                "company_of_interest": "600519",
                "trade_date": "2025-03-20",
                "market_region": "cn_a",
                "source_context": {"type": "overnight_scan"},
                "overnight_context": {
                    "quality": "proxy",
                    "quick_score": 72.0,
                    "total_score": 84.0,
                    "factor_breakdown": {"tail_strength": 18.0},
                    "tail_metrics": {
                        "source": "frozen-minute",
                        "quality": "proxy",
                        "tail_return_pct": 0.75,
                        "tail_amount_ratio": 0.18,
                        "last10_return_pct": 0.32,
                        "close_at_high_ratio": 0.92,
                    },
                },
            },
            {"market_region": "cn_a"},
        )

        overnight_block = structured["factor_snapshot"]["scores"]["overnight_tail"]
        self.assertIn("overnight_tail", structured["evidence_snapshot"])
        self.assertEqual(structured["factor_snapshot"]["strategy"], "a_share_balanced_v3_overnight")
        self.assertLessEqual(overnight_block["score"], 60.0)
        self.assertLess(overnight_block["confidence"], 0.8)


if __name__ == "__main__":
    unittest.main()
