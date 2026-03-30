from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from tradingagents.market_utils import SecurityProfile


OvernightMode = Literal["strict", "intraday_preview"]
ReviewReturnBasis = Literal["next_open", "buy_1455_sell_next_day_1000"]
TailQuality = Literal["real", "partial", "proxy", "missing", "invalid"]
PoolType = Literal["main", "gem", "star", "other"]
SelectionStage = Literal["preliminary", "scored", "formal", "watchlist", "rejected"]


@dataclass(frozen=True)
class ScanParams:
    min_amount: float = 1e8
    quick_score_floor: float = 55.0
    formal_score: float = 75.0
    watchlist_score: float = 60.0
    formal_max_total: int = 10
    watchlist_max_total: int = 12
    history_fetch_limit: int = 12
    tail_fetch_limit: int = 10
    dynamic_pool_max_size: int = 160
    dynamic_pool_realtime_limit: int = 100
    dynamic_pool_min_amount_yi: float = 8.0
    dynamic_pool_min_price: float = 3.0
    dynamic_pool_max_price: float = 120.0
    dynamic_pool_max_pct: float = 7.5
    dynamic_pool_main_limit: int = 80
    dynamic_pool_gem_limit: int = 50
    dynamic_pool_star_limit: int = 30
    min_tail_return: float = 0.5
    max_tail_return: float = 2.5
    max_rise_4h: float = 4.0
    max_distance_high: float = 1.2
    max_amplitude_main: float = 8.0
    max_amplitude_gem: float = 12.0
    turnover_overheat_main: float = 25.0
    turnover_overheat_gem: float = 35.0
    tail_start_time: str = "14:30"
    tail_last_window_minutes: int = 10


@dataclass
class MarketRegime:
    market_ok: bool
    market_message: str
    benchmark_pct: float
    indices: dict[str, float] = field(default_factory=dict)
    formal_threshold_delta: float = 0.0
    formal_limit_cap: int | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class TailMetrics:
    has_real_tail_data: bool
    source: str = ""
    tail_return_pct: float = 0.0
    tail_amount_ratio: float = 0.0
    last10_return_pct: float = 0.0
    close_at_high_ratio: float = 0.0
    auction_strength: float = 0.0
    rows: int = 0
    note: str = ""
    quality: TailQuality = "missing"
    provider_chain: list[str] = field(default_factory=list)


@dataclass
class OvernightSnapshot:
    profile: SecurityProfile
    name: str
    latest: float
    pre_close: float
    open_price: float
    high: float
    low: float
    amount: float
    turnover: float
    upper_limit: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    pct: float = 0.0
    intraday_return_from_open: float = 0.0
    position: float = 0.0
    dist_to_high: float = 999.0
    amplitude: float = 999.0
    dist_to_limit: float | None = None
    pool: PoolType = "other"

    @property
    def code(self) -> str:
        return self.profile.normalized_ticker

    @property
    def is_main(self) -> bool:
        return self.pool == "main"

    @property
    def is_gem_or_star(self) -> bool:
        return self.pool in {"gem", "star"}


@dataclass
class Candidate:
    snapshot: OvernightSnapshot
    passed: bool
    filter_reason: str
    quick_score: float = 0.0
    total_score: float = 0.0
    factor_breakdown: dict[str, float] = field(default_factory=dict)
    has_history: bool = False
    tail_metrics: TailMetrics | None = None
    excluded_from_final: str = ""
    selection_stage: SelectionStage = "preliminary"
    rejected_reason: str | None = None

    @property
    def has_real_tail(self) -> bool:
        return bool(self.tail_metrics and self.tail_metrics.quality == "real")

    @property
    def quality(self) -> TailQuality:
        return self.tail_metrics.quality if self.tail_metrics else "missing"


def normalize_overnight_mode(value: str | None, default: OvernightMode = "strict") -> OvernightMode:
    if value == "research_fallback":
        return "intraday_preview"
    if value in {"strict", "intraday_preview"}:
        return value
    return default


def normalize_review_return_basis(
    value: str | None,
    default: ReviewReturnBasis = "buy_1455_sell_next_day_1000",
) -> ReviewReturnBasis:
    if value in {"next_open", "buy_1455_sell_next_day_1000"}:
        return value
    return default


def normalize_tail_quality(value: str | None, default: TailQuality = "missing") -> TailQuality:
    if value in {"real", "partial", "proxy", "missing", "invalid"}:
        return value
    return default
