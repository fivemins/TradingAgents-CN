from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from dashboard_api.runtime import (
    append_event,
    compact_message_text,
    detect_stage_update,
    reset_event_log,
    summarize_message,
)


class RuntimeTests(unittest.TestCase):
    def test_final_trade_decision_does_not_mark_task_completed_early(self) -> None:
        stage, message = detect_stage_update(
            {"final_trade_decision": "FINAL ACTION: HOLD"},
            {},
        )

        self.assertEqual(stage, "risk")
        self.assertEqual(message, "Final trade decision is ready.")

    def test_summarize_message_skips_continue_and_tool_noise(self) -> None:
        self.assertIsNone(summarize_message({"messages": [SimpleNamespace(content="Continue")]}))
        self.assertIsNone(
            summarize_message(
                {
                    "messages": [
                        SimpleNamespace(
                            content="# Stock data for 002466.SZ from 2024-01-01 to 2026-03-25 Date,Open,High"
                        )
                    ]
                }
            )
        )
        self.assertIsNone(
            summarize_message(
                {
                    "messages": [
                        SimpleNamespace(content="## close_10_ema values from 2025-07-18 to 2026-03-25: ...")
                    ]
                }
            )
        )

    def test_summarize_message_compacts_report_headings(self) -> None:
        summary = summarize_message(
            {
                "messages": [
                    SimpleNamespace(
                        content="# 天齐锂业（002466.SZ）基本面分析报告 **分析日期：2026年3月25日** ## 一、公司概况 天齐锂业是..."
                    )
                ]
            }
        )

        self.assertEqual(summary, "天齐锂业（002466.SZ）基本面分析报告 **分析日期：2026年3月25日**")

    def test_summarize_message_keeps_short_plain_text(self) -> None:
        summary = summarize_message({"messages": [SimpleNamespace(content="多头研究员：我认为趋势已经改善。")]})

        self.assertEqual(summary, "多头研究员：我认为趋势已经改善。")

    def test_compact_message_text_keeps_report_heading_only(self) -> None:
        summary = compact_message_text(
            "# 最终交易决策 ## 结论摘要 综合得分 52.81，介于 45 和 65 之间，信号仍偏混合。"
        )

        self.assertEqual(summary, "最终交易决策")

    def test_reset_event_log_truncates_previous_run_content(self) -> None:
        with TemporaryDirectory() as tempdir:
            log_path = Path(tempdir) / "events.log"
            log_path.write_text("old traceback\n", encoding="utf-8")

            reset_event_log(log_path)
            append_event(log_path, "SYSTEM", "Runner started.")

            content = log_path.read_text(encoding="utf-8")

        self.assertNotIn("old traceback", content)
        self.assertIn("[SYSTEM] Runner started.", content)


if __name__ == "__main__":
    unittest.main()
