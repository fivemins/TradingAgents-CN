from __future__ import annotations

import unittest

from tradingagents.overnight.config import (
    build_evaluation_config_payload,
    compute_evaluation_config_hash,
    get_default_evaluation_config,
)


class OvernightConfigTests(unittest.TestCase):
    def test_default_config_hash_is_reproducible(self) -> None:
        left = get_default_evaluation_config()
        right = get_default_evaluation_config()

        self.assertEqual(compute_evaluation_config_hash(left), compute_evaluation_config_hash(right))

    def test_payload_contains_full_and_short_hash(self) -> None:
        payload = build_evaluation_config_payload(get_default_evaluation_config())

        self.assertIn("hash", payload)
        self.assertIn("short_hash", payload)
        self.assertTrue(payload["hash"].startswith(payload["short_hash"]))
        self.assertEqual(payload["version"], "overnight_phase2_v1")


if __name__ == "__main__":
    unittest.main()
