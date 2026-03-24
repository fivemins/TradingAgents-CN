from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


TaskStatus = Literal["queued", "running", "succeeded", "failed"]
TaskStage = Literal[
    "initializing",
    "market",
    "social",
    "news",
    "fundamentals",
    "research",
    "trader",
    "risk",
    "completed",
]
AnalystValue = Literal["market", "social", "news", "fundamentals"]
MarketRegion = Literal["cn_a", "us"]
OvernightMode = Literal["strict", "research_fallback"]
OvernightQuality = Literal["real", "proxy", "missing", "invalid"]


class OptionItem(BaseModel):
    value: str
    label: str


class MarketRegionOption(BaseModel):
    value: MarketRegion
    label: str
    description: str
    example: str


class DepthOption(BaseModel):
    value: int
    label: str
    description: str


class ProviderOption(BaseModel):
    value: str
    label: str
    base_url: str


class ModelsByProvider(BaseModel):
    quick: list[OptionItem]
    deep: list[OptionItem]


class TaskDefaults(BaseModel):
    ticker: str
    analysis_date: str
    market_region: MarketRegion
    analysts: list[AnalystValue]
    research_depth: int
    llm_provider: str
    quick_think_llm: str
    deep_think_llm: str
    online_tools: bool


class TaskCreateRequest(BaseModel):
    ticker: str = Field(min_length=1, max_length=16)
    analysis_date: str
    market_region: MarketRegion = "cn_a"
    analysts: list[AnalystValue] = Field(min_length=1)
    research_depth: int
    llm_provider: str = Field(min_length=1)
    quick_think_llm: str = Field(min_length=1)
    deep_think_llm: str = Field(min_length=1)
    online_tools: bool = True

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("analysis_date")
    @classmethod
    def validate_analysis_date(cls, value: str) -> str:
        date.fromisoformat(value)
        return value

    @field_validator("llm_provider", "quick_think_llm", "deep_think_llm")
    @classmethod
    def strip_values(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def validate_research_depth(self) -> "TaskCreateRequest":
        if self.research_depth not in {1, 3, 5}:
            raise ValueError("research_depth must be one of 1, 3, or 5.")
        return self


class TaskSourceContext(BaseModel):
    type: str
    scan_id: str | None = None
    trade_date: str | None = None
    mode: str | None = None
    ticker: str | None = None
    name: str | None = None


class StructuredSummary(BaseModel):
    composite_score: float | None = None
    confidence: float | None = None
    recommended_action: str | None = None
    primary_driver: str | None = None
    primary_risk: str | None = None


class TaskSummary(BaseModel):
    structured_summary: StructuredSummary | None = None
    source_context: TaskSourceContext | None = None
    task_id: str
    ticker: str
    analysis_date: str
    market_region: MarketRegion
    analysts: list[AnalystValue]
    research_depth: int
    llm_provider: str
    quick_think_llm: str
    deep_think_llm: str
    online_tools: bool
    status: TaskStatus
    stage: TaskStage
    progress_message: str
    decision: str | None = None
    error_message: str | None = None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None


class TaskDetail(TaskSummary):
    config_snapshot: dict[str, Any]
    overnight_context: dict[str, Any] | None = None
    artifact_dir: str
    download_urls: dict[str, str]
    report_status: dict[str, bool]


class TaskListStats(BaseModel):
    total: int
    queued: int
    running: int
    succeeded: int
    failed: int


class TaskListResponse(BaseModel):
    items: list[TaskSummary]
    stats: TaskListStats


class TaskArtifactsResponse(BaseModel):
    task_id: str
    reports: dict[str, str]
    downloads: dict[str, str]
    structured: dict[str, Any]


class TaskOptionsResponse(BaseModel):
    analysts: list[OptionItem]
    market_regions: list[MarketRegionOption]
    research_depths: list[DepthOption]
    providers: list[ProviderOption]
    model_options: dict[str, ModelsByProvider]
    defaults: TaskDefaults


class OvernightScanCreateRequest(BaseModel):
    trade_date: str
    market_region: Literal["cn_a"] = "cn_a"
    mode: OvernightMode = "strict"

    @field_validator("trade_date")
    @classmethod
    def validate_trade_date(cls, value: str) -> str:
        date.fromisoformat(value)
        return value


class OvernightCandidate(BaseModel):
    ticker: str
    name: str
    bucket: str | None = None
    pool: str
    quality: OvernightQuality
    latest: float
    pct: float
    amount: float
    turnover: float
    quick_score: float
    total_score: float
    factor_breakdown: dict[str, float]
    selection_stage: str | None = None
    rejected_reason: str | None = None
    tail_metrics: dict[str, Any] | None = None
    filter_reason: str | None = None
    excluded_from_final: str | None = None
    linked_task_id: str | None = None
    linked_task_status: TaskStatus | None = None
    linked_task_decision: str | None = None
    validation_status: str | None = None
    next_open_return: float | None = None
    next_open_date: str | None = None
    scan_close_price: float | None = None


class StratifiedBreakdownBucket(BaseModel):
    group: str
    days_with_formal_picks: int
    candidate_count: int
    avg_next_open_return: float | None = None
    avg_excess_return: float | None = None
    positive_pick_rate: float | None = None


class OvernightScanSummary(BaseModel):
    scan_id: str
    trade_date: str
    market_region: Literal["cn_a"]
    mode: OvernightMode
    status: TaskStatus
    progress_message: str
    market_message: str
    formal_count: int
    watchlist_count: int
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    summary_snapshot: dict[str, Any] | None = None
    top_formal_tickers: list[str] = Field(default_factory=list)
    scored_count: int = 0
    rejected_count: int = 0
    validated_formal_count: int = 0
    avg_next_open_return: float | None = None
    best_candidate: dict[str, Any] | None = None
    worst_candidate: dict[str, Any] | None = None
    validation_status: str | None = None
    data_quality: dict[str, Any] | None = None
    provider_route: dict[str, str] | None = None
    bias_flags: list[str] = Field(default_factory=list)
    universe_snapshot_date: str | None = None
    evaluation_config_version: str | None = None
    evaluation_config_hash: str | None = None


class OvernightScanDetail(OvernightScanSummary):
    artifact_dir: str
    download_urls: dict[str, str]
    preliminary_candidates: list[OvernightCandidate]
    total_score_candidates: list[OvernightCandidate]
    formal_recommendations: list[OvernightCandidate]
    watchlist: list[OvernightCandidate]
    rejected_candidates: list[OvernightCandidate]
    excluded_examples: list[OvernightCandidate]
    audit: dict[str, Any]


class OvernightScanListResponse(BaseModel):
    items: list[OvernightScanSummary]


class OvernightScanArtifactsResponse(BaseModel):
    scan_id: str
    summary: dict[str, Any]
    preliminary_candidates: list[OvernightCandidate]
    total_score_candidates: list[OvernightCandidate]
    formal_recommendations: list[OvernightCandidate]
    watchlist: list[OvernightCandidate]
    rejected_candidates: list[OvernightCandidate]
    excluded_examples: list[OvernightCandidate]
    audit: dict[str, Any]
    downloads: dict[str, str]


class OvernightReviewCreateRequest(BaseModel):
    end_trade_date: str
    market_region: Literal["cn_a"] = "cn_a"

    @field_validator("end_trade_date")
    @classmethod
    def validate_end_trade_date(cls, value: str) -> str:
        date.fromisoformat(value)
        return value


class OvernightReviewSummary(BaseModel):
    review_id: str
    end_trade_date: str
    market_region: Literal["cn_a"]
    window_days: int
    mode: Literal["strict"]
    return_basis: Literal["next_open"]
    status: TaskStatus
    progress_message: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    summary_snapshot: dict[str, Any] | None = None
    data_quality: dict[str, Any] | None = None
    provider_route: dict[str, str] | None = None
    bias_flags: list[str] = Field(default_factory=list)
    universe_snapshot_date: str | None = None
    survivorship_bias: bool = False
    evaluation_config_version: str | None = None
    evaluation_config_hash: str | None = None
    regime_breakdown: list[StratifiedBreakdownBucket] = Field(default_factory=list)
    pool_breakdown: list[StratifiedBreakdownBucket] = Field(default_factory=list)
    tail_quality_breakdown: list[StratifiedBreakdownBucket] = Field(default_factory=list)


class OvernightReviewDetail(OvernightReviewSummary):
    artifact_dir: str
    download_urls: dict[str, str]
    audit: dict[str, Any]


class OvernightReviewListResponse(BaseModel):
    items: list[OvernightReviewSummary]


class OvernightReviewArtifactsResponse(BaseModel):
    review_id: str
    summary: dict[str, Any]
    daily_results: list[dict[str, Any]]
    candidate_results: list[dict[str, Any]]
    audit: dict[str, Any]
    downloads: dict[str, str]


class ReadinessComponent(BaseModel):
    name: str
    ok: bool
    status: str
    message: str
    configured: bool | None = None
    active_keys: int | None = None
    rotation_enabled: bool | None = None
    probe_detail: str | None = None
    path: str | None = None
    dist: str | None = None
    base_url: str | None = None
    provider: str | None = None
    model: str | None = None
    rows: int | None = None
    dist_exists: bool | None = None
    index_exists: bool | None = None
    checks: dict[str, Any] | None = None


class SystemReadinessResponse(BaseModel):
    checked_at: str
    ready: bool
    components: dict[str, ReadinessComponent]
