from __future__ import annotations

import tempfile
import unittest

from tradingagents.qveris.registry import QVerisToolRegistry


class _FakeClient:
    def __init__(self) -> None:
        self.discover_calls = 0
        self.inspect_calls = 0

    def discover_tools(self, query: str, *, limit: int = 8):
        self.discover_calls += 1
        return {
            "search_id": "search-1",
            "results": [
                {
                    "tool_id": "ths_ifind.real_time_quotation.v1",
                    "name": "THS Real Time Quote",
                    "params": [{"name": "codes"}, {"name": "indicators"}],
                    "stats": {"success_rate": 0.98},
                },
                {
                    "tool_id": "other.tool.v1",
                    "name": "Other Tool",
                    "params": [{"name": "codes"}, {"name": "fields"}, {"name": "region"}],
                    "stats": {"success_rate": 0.95},
                },
            ],
        }

    def inspect_tools(self, tool_ids, *, discovery_id=None, timeout_ms=30_000):
        self.inspect_calls += 1
        return {
            "results": [
                {
                    "tool_id": tool_ids[0],
                    "name": "THS Real Time Quote",
                }
            ]
        }


class QVerisRegistryTests(unittest.TestCase):
    def test_discover_and_cache_writes_registry_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            registry = QVerisToolRegistry(tempdir)
            client = _FakeClient()
            record = registry.ensure_tool("cn_a_index_snapshot", client)

            self.assertEqual(record["tool_id"], "ths_ifind.real_time_quotation.v1")
            self.assertEqual(client.discover_calls, 1)
            self.assertTrue(registry.path.exists())

    def test_cached_tool_is_reused_without_rediscovery(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            registry = QVerisToolRegistry(tempdir)
            client = _FakeClient()
            first = registry.ensure_tool("cn_a_realtime_spot", client)
            second = registry.ensure_tool("cn_a_realtime_spot", client)

            self.assertEqual(first["tool_id"], second["tool_id"])
            self.assertEqual(client.discover_calls, 1)
            self.assertEqual(client.inspect_calls, 0)

    def test_validate_cached_tool_uses_inspect(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            registry = QVerisToolRegistry(tempdir)
            client = _FakeClient()
            registry.ensure_tool("cn_a_index_snapshot", client)
            validated = registry.ensure_tool("cn_a_index_snapshot", client, validate_cached=True)

            self.assertEqual(validated["tool_id"], "ths_ifind.real_time_quotation.v1")
            self.assertEqual(client.inspect_calls, 1)


if __name__ == "__main__":
    unittest.main()
