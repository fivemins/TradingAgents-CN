from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import QVerisClient, QVerisToolUnavailableError


CAPABILITY_QUERIES = {
    "cn_a_index_snapshot": "China stock market index real-time snapshot API",
    "cn_a_realtime_spot": "China A-share real-time stock market data API",
    "cn_a_intraday_minute": "China A-share 1 minute intraday bars API",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_default_registry_path(base_dir: str | Path | None = None) -> Path:
    if base_dir is not None:
        root = Path(base_dir)
    else:
        root = Path(
            os.getenv(
                "TRADINGAGENTS_DASHBOARD_DATA_DIR",
                Path(__file__).resolve().parents[2] / "dashboard_data",
            )
        )
    return root / "qveris" / "tool_registry.json"


def _tool_priority(capability: str, tool: dict[str, Any]) -> tuple[float, float, int, int]:
    tool_id = str(tool.get("tool_id") or "")
    success_rate = float(((tool.get("stats") or {}).get("success_rate")) or 0.0)
    params = tool.get("params") or []
    param_count = len(params) if isinstance(params, list) else 0

    capability_score = 0
    if tool_id == "ths_ifind.real_time_quotation.v1":
        capability_score = 100
    elif tool_id == "ths_ifind.quotation.v1":
        capability_score = 90
    elif tool_id.startswith("ths_ifind"):
        capability_score = 70
    elif tool_id.startswith("fast_fin"):
        capability_score = 40

    preferred_success = 1 if success_rate >= 0.9 else 0
    return (capability_score, success_rate, preferred_success, -param_count)


@dataclass
class QVerisToolRecord:
    capability: str
    discovery_id: str
    tool_id: str
    tool_name: str
    success_rate: float
    discovered_at: str
    query: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "discovery_id": self.discovery_id,
            "tool_id": self.tool_id,
            "tool_name": self.tool_name,
            "success_rate": self.success_rate,
            "discovered_at": self.discovered_at,
            "query": self.query,
        }


class QVerisToolRegistry:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = get_default_registry_path(path)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def save(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def get(self, capability: str) -> dict[str, Any] | None:
        return self.load().get(capability)

    def set(self, capability: str, record: dict[str, Any]) -> dict[str, Any]:
        payload = self.load()
        payload[capability] = record
        self.save(payload)
        return record

    def ensure_tool(
        self,
        capability: str,
        client: QVerisClient,
        *,
        validate_cached: bool = False,
        require_batch_capability: bool = False,
    ) -> dict[str, Any]:
        if capability not in CAPABILITY_QUERIES:
            raise KeyError(f"Unsupported QVeris capability: {capability}")

        cached = self.get(capability)
        if cached:
            if not validate_cached and (
                not require_batch_capability or bool(cached.get("supports_batch"))
            ):
                return cached
            try:
                inspected = client.inspect_tools(
                    [str(cached.get("tool_id") or "")],
                    discovery_id=str(cached.get("discovery_id") or "") or None,
                )
                results = inspected.get("results") or []
                for item in results:
                    if str(item.get("tool_id") or "") != cached.get("tool_id"):
                        continue
                    record = self._build_record(capability, item, cached.get("discovery_id"), CAPABILITY_QUERIES[capability])
                    if require_batch_capability and not record.get("supports_batch"):
                        break
                    return self.set(capability, record)
            except QVerisToolUnavailableError:
                pass
            except Exception:
                pass

        discovered = self.discover_and_cache(
            capability,
            client,
            require_batch_capability=require_batch_capability,
        )
        return discovered

    def invalidate(self, capability: str) -> None:
        payload = self.load()
        if capability in payload:
            payload.pop(capability, None)
            self.save(payload)

    def discover_and_cache(
        self,
        capability: str,
        client: QVerisClient,
        *,
        require_batch_capability: bool = False,
    ) -> dict[str, Any]:
        query = CAPABILITY_QUERIES[capability]
        response = client.discover_tools(query, limit=8)
        results = response.get("results") or []
        if not isinstance(results, list) or not results:
            raise RuntimeError(f"QVeris returned no tools for {capability}.")

        candidates = sorted(
            (tool for tool in results if isinstance(tool, dict)),
            key=lambda tool: _tool_priority(capability, tool),
            reverse=True,
        )
        discovery_id = str(response.get("search_id") or response.get("discovery_id") or "")
        selected_record: dict[str, Any] | None = None
        for selected in candidates:
            record = self._build_record(capability, selected, discovery_id, query)
            if require_batch_capability and not record.get("supports_batch"):
                continue
            selected_record = record
            break
        if selected_record is None:
            raise RuntimeError(f"QVeris returned no batch-capable tool for {capability}.")
        record = selected_record
        return self.set(capability, record)

    def _build_record(
        self,
        capability: str,
        tool: dict[str, Any],
        discovery_id: str | None,
        query: str,
    ) -> dict[str, Any]:
        params = tool.get("params") or []
        supports_batch, batch_parameter_name, max_codes_hint = _detect_batch_capability(params)
        return {
            **QVerisToolRecord(
                capability=capability,
                discovery_id=str(discovery_id or ""),
                tool_id=str(tool.get("tool_id") or ""),
                tool_name=str(tool.get("name") or tool.get("tool_id") or ""),
                success_rate=float(((tool.get("stats") or {}).get("success_rate")) or 0.0),
                discovered_at=_utc_now(),
                query=query,
            ).to_dict(),
            "supports_batch": supports_batch,
            "batch_parameter_name": batch_parameter_name,
            "max_codes_hint": max_codes_hint,
            "capability_validated_at": _utc_now(),
        }


def _detect_batch_capability(params: Any) -> tuple[bool, str | None, int | None]:
    if not isinstance(params, list):
        return False, None, None
    for item in params:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        lowered_name = name.lower()
        description = str(item.get("description") or "").lower()
        if lowered_name not in {"codes", "symbols", "tickers"}:
            continue
        supports_batch = any(
            marker in description
            for marker in ("comma-separated", "multiple codes", "multiple", "max 50", "逗号")
        )
        max_hint = None
        for token in description.replace(")", " ").replace("(", " ").split():
            if token.isdigit():
                max_hint = int(token)
        if "max 50" in description:
            max_hint = 50
        return supports_batch, name, max_hint
    return False, None, None
