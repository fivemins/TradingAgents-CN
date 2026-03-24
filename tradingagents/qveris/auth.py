from __future__ import annotations

import os
from collections.abc import Mapping


def _split_keys(raw: str) -> list[str]:
    keys: list[str] = []
    for item in raw.split(","):
        value = item.strip()
        if value and value not in keys:
            keys.append(value)
    return keys


def get_qveris_api_keys(env: Mapping[str, str] | None = None) -> list[str]:
    active_env = env or os.environ
    multi = active_env.get("QVERIS_API_KEYS", "")
    if multi.strip():
        return _split_keys(multi)
    single = active_env.get("QVERIS_API_KEY", "").strip()
    return [single] if single else []


def build_qveris_auth_summary(env: Mapping[str, str] | None = None) -> dict[str, object]:
    keys = get_qveris_api_keys(env)
    return {
        "configured": bool(keys),
        "active_keys": len(keys),
        "rotation_enabled": len(keys) > 1,
    }
