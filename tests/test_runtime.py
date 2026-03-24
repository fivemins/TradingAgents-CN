from __future__ import annotations

import unittest

from dashboard_api.runtime import detect_stage_update


class RuntimeTests(unittest.TestCase):
    def test_final_trade_decision_does_not_mark_task_completed_early(self) -> None:
        stage, message = detect_stage_update(
            {"final_trade_decision": "FINAL ACTION: HOLD"},
            {},
        )

        self.assertEqual(stage, "risk")
        self.assertEqual(message, "Final trade decision is ready.")


if __name__ == "__main__":
    unittest.main()
