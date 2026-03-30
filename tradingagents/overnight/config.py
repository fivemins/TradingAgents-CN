from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
import hashlib
import json
from typing import Any

from .models import OvernightMode, ReviewReturnBasis, ScanParams


DEFAULT_EVALUATION_CONFIG_VERSION = "overnight_phase2_v1"


@dataclass(frozen=True)
class OvernightTailPolicy:
    strict_requires_real_tail: bool = True
    proxy_watchlist_only: bool = True
    proxy_factor_score_cap: float = 60.0
    proxy_confidence_penalty: float = 0.15


@dataclass(frozen=True)
class OvernightEvaluationConfig:
    version: str = DEFAULT_EVALUATION_CONFIG_VERSION
    live_scan_params: ScanParams = field(default_factory=ScanParams)
    review_scan_params: ScanParams = field(
        default_factory=lambda: replace(
            ScanParams(),
            dynamic_pool_max_size=260,
            dynamic_pool_realtime_limit=220,
            dynamic_pool_min_amount_yi=5.0,
            dynamic_pool_main_limit=120,
            dynamic_pool_gem_limit=70,
            dynamic_pool_star_limit=50,
        )
    )
    review_window_days: int = 60
    review_mode: OvernightMode = "strict"
    review_return_basis: ReviewReturnBasis = "buy_1455_sell_next_day_1000"
    tail_policy: OvernightTailPolicy = field(default_factory=OvernightTailPolicy)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_default_evaluation_config() -> OvernightEvaluationConfig:
    return OvernightEvaluationConfig()


def build_evaluation_config_payload(
    config: OvernightEvaluationConfig | None = None,
) -> dict[str, Any]:
    active = config or get_default_evaluation_config()
    payload = active.to_dict()
    full_hash = compute_evaluation_config_hash(active)
    return {
        "version": active.version,
        "hash": full_hash,
        "short_hash": full_hash[:12],
        "config": payload,
    }


def compute_evaluation_config_hash(
    config: OvernightEvaluationConfig | dict[str, Any],
) -> str:
    payload = config if isinstance(config, dict) else config.to_dict()
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
