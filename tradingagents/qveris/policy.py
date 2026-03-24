from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class QVerisCapabilityPolicy:
    enabled: bool = True
    batch_only: bool = True
    max_calls_per_scan: int = 1
    max_codes_per_call: int = 50
    max_total_codes_per_scan: int = 50
    require_batch_capability: bool = True


@dataclass(frozen=True)
class QVerisUsagePolicy:
    index_snapshot: QVerisCapabilityPolicy = field(
        default_factory=lambda: QVerisCapabilityPolicy(
            max_calls_per_scan=1,
            max_codes_per_call=3,
            max_total_codes_per_scan=3,
        )
    )
    realtime_spot: QVerisCapabilityPolicy = field(
        default_factory=lambda: QVerisCapabilityPolicy(
            max_calls_per_scan=1,
            max_codes_per_call=300,
            max_total_codes_per_scan=300,
        )
    )
    intraday_minute: QVerisCapabilityPolicy = field(
        default_factory=lambda: QVerisCapabilityPolicy(
            max_calls_per_scan=1,
            max_codes_per_call=10,
            max_total_codes_per_scan=10,
        )
    )

    def get(self, capability: str) -> QVerisCapabilityPolicy:
        return {
            "index_snapshot": self.index_snapshot,
            "realtime_spot": self.realtime_spot,
            "intraday_minute": self.intraday_minute,
        }[capability]


def get_default_qveris_usage_policy() -> QVerisUsagePolicy:
    return QVerisUsagePolicy()


@dataclass
class QVerisUsageTracker:
    policy: QVerisUsagePolicy = field(default_factory=get_default_qveris_usage_policy)
    calls: dict[str, int] = field(default_factory=dict)
    requested_codes: dict[str, int] = field(default_factory=dict)
    resolved_codes: dict[str, int] = field(default_factory=dict)
    enabled_capabilities: set[str] = field(default_factory=set)
    skipped_capabilities: dict[str, list[str]] = field(default_factory=dict)
    routes: set[str] = field(default_factory=set)
    tool_ids: set[str] = field(default_factory=set)

    def plan_codes(
        self,
        capability: str,
        codes: list[str],
        *,
        supports_batch: bool = True,
    ) -> tuple[list[str], str | None]:
        policy = self.policy.get(capability)
        unique_codes = list(dict.fromkeys(code for code in codes if code))
        if not policy.enabled:
            self.record_skip(capability, "disabled")
            return [], "disabled"
        if policy.require_batch_capability and not supports_batch:
            self.record_skip(capability, "batch_capability_missing")
            return [], "batch_capability_missing"
        if policy.batch_only and len(unique_codes) > 1 and not supports_batch:
            self.record_skip(capability, "batch_only")
            return [], "batch_only"
        calls_used = self.calls.get(capability, 0)
        if calls_used >= policy.max_calls_per_scan:
            self.record_skip(capability, "call_budget_exhausted")
            return [], "call_budget_exhausted"
        remaining_total = policy.max_total_codes_per_scan - self.requested_codes.get(capability, 0)
        if remaining_total <= 0:
            self.record_skip(capability, "code_budget_exhausted")
            return [], "code_budget_exhausted"
        allowed = unique_codes[: min(policy.max_codes_per_call, remaining_total)]
        skipped_reason = None
        if not allowed:
            self.record_skip(capability, "no_codes")
            return [], "no_codes"
        if len(allowed) < len(unique_codes):
            skipped_reason = "budget_trimmed"
            self.record_skip(capability, skipped_reason)
        return allowed, skipped_reason

    def record_success(
        self,
        capability: str,
        *,
        requested_codes: int,
        resolved_codes: int,
        route: str | None = None,
        tool_id: str | None = None,
    ) -> None:
        self.calls[capability] = self.calls.get(capability, 0) + 1
        self.requested_codes[capability] = self.requested_codes.get(capability, 0) + requested_codes
        self.resolved_codes[capability] = self.resolved_codes.get(capability, 0) + resolved_codes
        self.enabled_capabilities.add(capability)
        if route:
            self.routes.add(route)
        if tool_id:
            self.tool_ids.add(tool_id)

    def record_skip(self, capability: str, reason: str) -> None:
        bucket = self.skipped_capabilities.setdefault(capability, [])
        if reason not in bucket:
            bucket.append(reason)

    def to_audit_dict(self) -> dict[str, Any]:
        policy_payload = {
            key: {
                "enabled": value.enabled,
                "batch_only": value.batch_only,
                "max_calls_per_scan": value.max_calls_per_scan,
                "max_codes_per_call": value.max_codes_per_call,
                "max_total_codes_per_scan": value.max_total_codes_per_scan,
                "require_batch_capability": value.require_batch_capability,
            }
            for key, value in {
                "index_snapshot": self.policy.index_snapshot,
                "realtime_spot": self.policy.realtime_spot,
                "intraday_minute": self.policy.intraday_minute,
            }.items()
        }
        return {
            "qveris_enabled_capabilities": sorted(self.enabled_capabilities),
            "qveris_skipped_capabilities": self.skipped_capabilities,
            "qveris_batch_calls": self.calls,
            "qveris_requested_codes": self.requested_codes,
            "qveris_resolved_codes": self.resolved_codes,
            "qveris_budget_policy": policy_payload,
            "qveris_routes": sorted(self.routes),
            "qveris_tool_ids": sorted(self.tool_ids),
            "qveris_fallback_count": len(self.routes),
            "qveris_skip_reasons": sorted(
                {reason for reasons in self.skipped_capabilities.values() for reason in reasons}
            ),
        }
