from __future__ import annotations

import unittest

from tradingagents.qveris.auth import build_qveris_auth_summary, get_qveris_api_keys
from tradingagents.qveris.client import QVerisClient, QVerisConfigurationError


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        return self._payload


class _DummySession:
    def __init__(self, responses: list[_DummyResponse]) -> None:
        self.responses = responses
        self.calls: list[dict] = []

    def request(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class QVerisAuthTests(unittest.TestCase):
    def test_multi_key_env_is_split_and_deduped(self) -> None:
        keys = get_qveris_api_keys(
            {
                "QVERIS_API_KEYS": " key-a , key-b,key-a , key-c ",
                "QVERIS_API_KEY": "fallback",
            }
        )
        self.assertEqual(keys, ["key-a", "key-b", "key-c"])

    def test_single_key_fallback_is_supported(self) -> None:
        keys = get_qveris_api_keys({"QVERIS_API_KEY": "single-key"})
        summary = build_qveris_auth_summary({"QVERIS_API_KEY": "single-key"})
        self.assertEqual(keys, ["single-key"])
        self.assertTrue(summary["configured"])
        self.assertEqual(summary["active_keys"], 1)
        self.assertFalse(summary["rotation_enabled"])

    def test_client_rotates_to_next_key_on_429(self) -> None:
        session = _DummySession(
            [
                _DummyResponse(429, text="quota exceeded"),
                _DummyResponse(200, payload={"results": [{"tool_id": "tool-1"}]}),
            ]
        )
        client = QVerisClient(api_keys=["key-a", "key-b"], session=session)
        payload = client.discover_tools("China stock market index real-time snapshot API")
        self.assertEqual(payload["results"][0]["tool_id"], "tool-1")
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(client.last_meta.attempts, 2)
        self.assertEqual(client.last_meta.active_key_index, 1)

    def test_client_requires_at_least_one_key(self) -> None:
        client = QVerisClient(api_keys=[])
        with self.assertRaises(QVerisConfigurationError):
            client.discover_tools("query")


if __name__ == "__main__":
    unittest.main()
