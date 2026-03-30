import type {
  EvidenceGroup,
  FactorSnapshot,
  OvernightCandidate,
  OvernightMode,
  OvernightQuality,
  OvernightTrackedTrade,
  OvernightTrackedTradeListResponse,
  OvernightTrackedTradeStats,
  OvernightReviewExtrema,
  OvernightReviewArtifactsResponse,
  OvernightReviewDetail,
  OvernightReviewListResponse,
  ReviewReturnBasis,
  OvernightReviewSummary,
  OvernightReviewSummarySnapshot,
  OvernightScanArtifactsResponse,
  OvernightScanDetail,
  OvernightScanListResponse,
  OvernightScanSummary,
  OvernightStratifiedBreakdownBucket,
  OvernightTaskContext,
  OvernightValidatedCandidateSummary,
  ReadinessComponent,
  StructuredDecision,
  StructuredSummary,
  SystemReadinessResponse,
  TailMetricsSummary,
  TaskArtifactsResponse,
  TaskDetail,
  TaskListResponse,
  TaskSourceContext,
  TaskSummary
} from "../types";

type LooseRecord = Record<string, unknown>;

function asRecord(value: unknown): LooseRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as LooseRecord) : {};
}

function asArray<T = unknown>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asInteger(value: unknown, fallback = 0): number {
  const normalized = asNumber(value);
  return normalized === null ? fallback : Math.trunc(normalized);
}

function normalizeOvernightModeValue(value: unknown, fallback: OvernightMode = "strict"): OvernightMode {
  if (value === "research_fallback") {
    return "intraday_preview";
  }
  return value === "intraday_preview" || value === "strict" ? value : fallback;
}

function normalizeOvernightQualityValue(
  value: unknown,
  fallback: OvernightQuality = "missing",
): OvernightQuality {
  return value === "real" ||
    value === "partial" ||
    value === "proxy" ||
    value === "missing" ||
    value === "invalid"
    ? value
    : fallback;
}

function normalizeReviewReturnBasisValue(
  value: unknown,
  fallback: ReviewReturnBasis = "buy_1455_sell_next_day_1000",
): ReviewReturnBasis {
  return value === "next_open" || value === "buy_1455_sell_next_day_1000" ? value : fallback;
}

function normalizeTrackedTradeStatusValue(
  value: unknown,
  fallback: OvernightTrackedTrade["status"] = "pending_entry",
): OvernightTrackedTrade["status"] {
  return value === "pending_entry" ||
    value === "pending_exit" ||
    value === "validated" ||
    value === "unavailable"
    ? value
    : fallback;
}

function normalizeTrackedTradeSourceBucketValue(
  value: unknown,
  fallback: OvernightTrackedTrade["source_bucket"] = "formal",
): OvernightTrackedTrade["source_bucket"] {
  return value === "formal" || value === "watchlist" || value === "total_score" ? value : fallback;
}

function readableName(value: unknown, fallback?: string | null): string | null {
  const text = asNullableString(value)?.trim() ?? "";
  if (text && !/^\?+$/.test(text) && text !== "????") {
    return text;
  }
  return fallback ?? null;
}

function normalizeStructuredSummary(value: unknown): StructuredSummary | null {
  const payload = asRecord(value);
  const normalized: StructuredSummary = {
    composite_score: asNumber(payload.composite_score),
    confidence: asNumber(payload.confidence),
    recommended_action: asNullableString(payload.recommended_action),
    primary_driver: asNullableString(payload.primary_driver),
    primary_risk: asNullableString(payload.primary_risk)
  };
  return Object.values(normalized).some((item) => item !== null) ? normalized : null;
}

function normalizeSourceContext(value: unknown, fallbackTicker?: string | null): TaskSourceContext | null {
  const payload = asRecord(value);
  if (!Object.keys(payload).length) {
    return null;
  }
  const ticker = asNullableString(payload.ticker) ?? fallbackTicker ?? null;
  return {
    type: asString(payload.type),
    scan_id: asNullableString(payload.scan_id),
    trade_date: asNullableString(payload.trade_date),
    mode: payload.mode ? normalizeOvernightModeValue(payload.mode) : null,
    ticker,
    name: readableName(payload.name, ticker)
  };
}

function normalizeTailMetrics(value: unknown): TailMetricsSummary | null {
  const payload = asRecord(value);
  if (!Object.keys(payload).length) {
    return null;
  }
  return {
    source: asNullableString(payload.source) ?? undefined,
    quality: normalizeOvernightQualityValue(payload.quality) as TailMetricsSummary["quality"],
    tail_return_pct: asNumber(payload.tail_return_pct) ?? undefined,
    tail_amount_ratio: asNumber(payload.tail_amount_ratio) ?? undefined,
    last10_return_pct: asNumber(payload.last10_return_pct) ?? undefined,
    close_at_high_ratio: asNumber(payload.close_at_high_ratio) ?? undefined,
    auction_strength: asNumber(payload.auction_strength) ?? undefined,
    rows: asNumber(payload.rows) ?? undefined,
    note: asNullableString(payload.note) ?? undefined,
    provider_chain: asArray<string>(payload.provider_chain)
  };
}

function normalizeOvernightContext(value: unknown, fallbackTicker?: string | null): OvernightTaskContext | null {
  const payload = asRecord(value);
  if (!Object.keys(payload).length) {
    return null;
  }
  return {
    scan_id: asNullableString(payload.scan_id) ?? undefined,
    scan_trade_date: asNullableString(payload.scan_trade_date) ?? undefined,
    scan_mode: payload.scan_mode ? normalizeOvernightModeValue(payload.scan_mode) : undefined,
    source_name: readableName(payload.source_name, fallbackTicker) ?? undefined,
    bucket: asNullableString(payload.bucket) ?? undefined,
    quality: normalizeOvernightQualityValue(payload.quality) as OvernightTaskContext["quality"],
    quick_score: asNumber(payload.quick_score) ?? undefined,
    total_score: asNumber(payload.total_score) ?? undefined,
    factor_breakdown: asRecord(payload.factor_breakdown) as Record<string, number>,
    tail_metrics: normalizeTailMetrics(payload.tail_metrics),
    provider_route: asRecord(payload.provider_route) as Record<string, string>,
    evaluation_config_version: asNullableString(payload.evaluation_config_version),
    evaluation_config_hash: asNullableString(payload.evaluation_config_hash),
    validation_status: asNullableString(payload.validation_status),
    next_open_return: asNumber(payload.next_open_return),
    next_open_date: asNullableString(payload.next_open_date)
  };
}

function normalizeReadinessComponent(value: unknown, name: string): ReadinessComponent {
  const payload = asRecord(value);
  return {
    name: asString(payload.name, name),
    ok: Boolean(payload.ok),
    status: asString(payload.status, payload.ok ? "ok" : "unknown"),
    message: asString(payload.message, "尚未获取组件状态。"),
    configured: typeof payload.configured === "boolean" ? payload.configured : null,
    active_keys: asNumber(payload.active_keys),
    rotation_enabled: typeof payload.rotation_enabled === "boolean" ? payload.rotation_enabled : null,
    probe_detail: asNullableString(payload.probe_detail),
    path: asNullableString(payload.path),
    dist: asNullableString(payload.dist),
    base_url: asNullableString(payload.base_url),
    provider: asNullableString(payload.provider),
    model: asNullableString(payload.model),
    rows: asNumber(payload.rows),
    dist_exists: typeof payload.dist_exists === "boolean" ? payload.dist_exists : null,
    index_exists: typeof payload.index_exists === "boolean" ? payload.index_exists : null,
    checks: asRecord(payload.checks)
  };
}

function normalizeCandidate(value: unknown, defaultBucket?: string | null): OvernightCandidate | null {
  const payload = asRecord(value);
  const ticker = asNullableString(payload.ticker);
  if (!ticker) {
    return null;
  }
  return {
    ticker,
    name: readableName(payload.name, ticker) ?? ticker,
    bucket: asNullableString(payload.bucket) ?? defaultBucket ?? null,
    pool: asString(payload.pool, "--"),
    quality: normalizeOvernightQualityValue(payload.quality) as OvernightCandidate["quality"],
    latest: asNumber(payload.latest) ?? 0,
    pct: asNumber(payload.pct) ?? 0,
    amount: asNumber(payload.amount) ?? 0,
    turnover: asNumber(payload.turnover) ?? 0,
    quick_score: asNumber(payload.quick_score) ?? 0,
    total_score: asNumber(payload.total_score) ?? 0,
    factor_breakdown: asRecord(payload.factor_breakdown) as Record<string, number>,
    selection_stage: asNullableString(payload.selection_stage),
    rejected_reason: asNullableString(payload.rejected_reason),
    tail_metrics: normalizeTailMetrics(payload.tail_metrics),
    filter_reason: asNullableString(payload.filter_reason),
    excluded_from_final: asNullableString(payload.excluded_from_final),
    linked_task_id: asNullableString(payload.linked_task_id),
    linked_task_status: asNullableString(payload.linked_task_status) as OvernightCandidate["linked_task_status"],
    linked_task_decision: asNullableString(payload.linked_task_decision),
    validation_status: asNullableString(payload.validation_status),
    next_open_return: asNumber(payload.next_open_return),
    next_open_date: asNullableString(payload.next_open_date),
    scan_close_price: asNumber(payload.scan_close_price)
  };
}

function normalizeCandidateList(value: unknown, defaultBucket?: string | null): OvernightCandidate[] {
  return asArray(value)
    .map((item) => normalizeCandidate(item, defaultBucket))
    .filter(Boolean) as OvernightCandidate[];
}

function normalizeBreakdownList(value: unknown): OvernightStratifiedBreakdownBucket[] {
  return asArray(value).map((item) => {
    const payload = asRecord(item);
    return {
      group: asString(payload.group, "未分组"),
      days_with_formal_picks: asInteger(payload.days_with_formal_picks, 0),
      candidate_count: asInteger(payload.candidate_count, 0),
      avg_next_open_return: asNumber(payload.avg_next_open_return),
      avg_excess_return: asNumber(payload.avg_excess_return),
      positive_pick_rate: asNumber(payload.positive_pick_rate)
    };
  });
}

function normalizeReviewSummarySnapshot(value: unknown): OvernightReviewSummarySnapshot | null {
  const payload = asRecord(value);
  if (!Object.keys(payload).length) {
    return null;
  }
  const avgStrategyReturn = asNumber(payload.avg_strategy_return) ?? asNumber(payload.avg_next_open_return);
  const medianStrategyReturn = asNumber(payload.median_strategy_return) ?? asNumber(payload.median_next_open_return);
  const avgDailyStrategyReturn =
    asNumber(payload.avg_daily_strategy_return) ?? asNumber(payload.avg_daily_equal_weight_return);
  const avgBenchmarkReturn =
    asNumber(payload.avg_benchmark_return) ?? asNumber(payload.avg_benchmark_next_open_return);
  const daysWithTrade = asInteger(payload.days_with_trade, asInteger(payload.days_with_formal_picks, 0));
  const tradeCount = asInteger(payload.trade_count, asInteger(payload.candidate_count, 0));
  return {
    end_trade_date: asString(payload.end_trade_date, ""),
    market_region: (asString(payload.market_region, "cn_a") as OvernightReviewSummarySnapshot["market_region"]),
    window_days: asInteger(payload.window_days, 60),
    mode: "strict",
    return_basis: normalizeReviewReturnBasisValue(payload.return_basis),
    candidate_count: tradeCount,
    trade_count: tradeCount,
    days_evaluated: asInteger(payload.days_evaluated, 0),
    days_with_formal_picks: daysWithTrade,
    days_with_trade: daysWithTrade,
    avg_strategy_return: avgStrategyReturn,
    median_strategy_return: medianStrategyReturn,
    avg_daily_strategy_return: avgDailyStrategyReturn,
    avg_benchmark_return: avgBenchmarkReturn,
    avg_next_open_return: avgStrategyReturn,
    median_next_open_return: medianStrategyReturn,
    positive_pick_rate: asNumber(payload.positive_pick_rate),
    avg_daily_equal_weight_return: avgDailyStrategyReturn,
    avg_benchmark_next_open_return: avgBenchmarkReturn,
    avg_excess_return: asNumber(payload.avg_excess_return),
    has_valid_samples: Boolean(payload.has_valid_samples),
    headline_message: asNullableString(payload.headline_message),
    best_day: normalizeReviewExtrema(payload.best_day),
    worst_day: normalizeReviewExtrema(payload.worst_day),
    audit: asRecord(payload.audit),
    evaluation_config_version: asNullableString(payload.evaluation_config_version),
    evaluation_config_hash: asNullableString(payload.evaluation_config_hash),
    regime_breakdown: normalizeBreakdownList(payload.regime_breakdown),
    pool_breakdown: normalizeBreakdownList(payload.pool_breakdown),
    tail_quality_breakdown: normalizeBreakdownList(payload.tail_quality_breakdown)
  };
}

function normalizeReviewExtrema(value: unknown): OvernightReviewExtrema | null {
  const payload = asRecord(value);
  if (!Object.keys(payload).length) {
    return null;
  }
  const strategyReturn = asNumber(payload.strategy_return) ?? asNumber(payload.equal_weight_next_open_return);
  const benchmarkReturn = asNumber(payload.benchmark_return) ?? asNumber(payload.benchmark_next_open_return);
  const excessReturn = asNumber(payload.excess_return) ?? asNumber(payload.avg_excess_return);
  const selectedTicker =
    asNullableString(payload.selected_ticker) ??
    (asArray(payload.formal_tickers).find((item): item is string => typeof item === "string") ?? null);
  return {
    trade_date: asString(payload.trade_date, ""),
    strategy_return: strategyReturn,
    benchmark_return: benchmarkReturn,
    excess_return: excessReturn,
    selected_ticker: selectedTicker,
    equal_weight_next_open_return: strategyReturn,
    benchmark_next_open_return: benchmarkReturn,
    avg_excess_return: excessReturn,
    formal_tickers: asArray(payload.formal_tickers).filter((item): item is string => typeof item === "string")
  };
}

function normalizeReviewDailyResult(value: unknown): OvernightReviewArtifactsResponse["daily_results"][number] {
  const payload = asRecord(value);
  const strategyReturn = asNumber(payload.strategy_return) ?? asNumber(payload.equal_weight_next_open_return);
  const benchmarkReturn = asNumber(payload.benchmark_return) ?? asNumber(payload.benchmark_next_open_return);
  const excessReturn = asNumber(payload.excess_return) ?? asNumber(payload.avg_excess_return);
  const selectedTicker =
    asNullableString(payload.selected_ticker) ??
    (asArray(payload.formal_tickers).find((item): item is string => typeof item === "string") ?? null);
  const formalTickers = asArray(payload.formal_tickers).filter((item): item is string => typeof item === "string");
  return {
    trade_date: asString(payload.trade_date, ""),
    trade_count: asInteger(payload.trade_count, strategyReturn !== null ? 1 : asInteger(payload.formal_count, 0)),
    selected_ticker: selectedTicker,
    selected_name: asNullableString(payload.selected_name),
    selected_pool: asNullableString(payload.selected_pool),
    selected_quality: normalizeOvernightQualityValue(payload.selected_quality, "missing"),
    selected_total_score: asNumber(payload.selected_total_score),
    formal_count: asInteger(payload.formal_count, strategyReturn !== null ? 1 : 0),
    watchlist_count: asInteger(payload.watchlist_count, 0),
    formal_tickers: formalTickers.length ? formalTickers : selectedTicker ? [selectedTicker] : [],
    market_message: asString(payload.market_message, ""),
    entry_target_time: asNullableString(payload.entry_target_time),
    entry_time_used: asNullableString(payload.entry_time_used),
    entry_price: asNumber(payload.entry_price),
    exit_target_time: asNullableString(payload.exit_target_time),
    exit_trade_date: asNullableString(payload.exit_trade_date),
    exit_time_used: asNullableString(payload.exit_time_used),
    exit_price: asNumber(payload.exit_price),
    strategy_return: strategyReturn,
    benchmark_return: benchmarkReturn,
    excess_return: excessReturn,
    counted_in_performance:
      typeof payload.counted_in_performance === "boolean" ? payload.counted_in_performance : strategyReturn !== null,
    benchmark_next_open_return: benchmarkReturn,
    equal_weight_next_open_return: strategyReturn,
    avg_excess_return: excessReturn,
    tail_quality_counts: asRecord(payload.tail_quality_counts) as Record<string, number>,
    passed_filters: asNumber(payload.passed_filters) ?? undefined,
    failed_filters: asNumber(payload.failed_filters) ?? undefined
  };
}

function normalizeReviewCandidateResult(value: unknown): OvernightReviewArtifactsResponse["candidate_results"][number] {
  const payload = asRecord(value);
  const strategyReturn = asNumber(payload.strategy_return) ?? asNumber(payload.next_open_return);
  const benchmarkReturn = asNumber(payload.benchmark_return) ?? asNumber(payload.benchmark_next_open_return);
  return {
    trade_date: asString(payload.trade_date, ""),
    category: (asString(payload.category, "selected") as OvernightReviewArtifactsResponse["candidate_results"][number]["category"]),
    ticker: asString(payload.ticker, ""),
    name: readableName(payload.name, asNullableString(payload.ticker)) ?? asString(payload.ticker, ""),
    quality: normalizeOvernightQualityValue(payload.quality),
    quick_score: asNumber(payload.quick_score) ?? 0,
    total_score: asNumber(payload.total_score) ?? 0,
    factor_breakdown: asRecord(payload.factor_breakdown) as Record<string, number>,
    tail_metrics: normalizeTailMetrics(payload.tail_metrics),
    filter_reason: asNullableString(payload.filter_reason),
    entry_target_time: asNullableString(payload.entry_target_time),
    entry_time_used: asNullableString(payload.entry_time_used),
    entry_price: asNumber(payload.entry_price),
    exit_target_time: asNullableString(payload.exit_target_time),
    exit_time_used: asNullableString(payload.exit_time_used),
    exit_price: asNumber(payload.exit_price),
    strategy_return: strategyReturn,
    benchmark_return: benchmarkReturn,
    next_trade_date: asNullableString(payload.next_trade_date),
    scan_close_price: asNumber(payload.scan_close_price),
    next_open_return: strategyReturn,
    benchmark_next_open_return: benchmarkReturn,
    excess_return: asNumber(payload.excess_return),
    counted_in_performance: Boolean(payload.counted_in_performance),
    skipped_reason: asNullableString(payload.skipped_reason)
  };
}

function normalizeValidatedCandidateSummary(value: unknown): OvernightValidatedCandidateSummary | null {
  const payload = asRecord(value);
  const ticker = asNullableString(payload.ticker);
  if (!ticker) {
    return null;
  }
  return {
    ticker,
    name: readableName(payload.name, ticker) ?? ticker,
    next_open_return: asNumber(payload.next_open_return),
    next_open_date: asNullableString(payload.next_open_date)
  };
}

export function normalizeTaskSummary(value: unknown): TaskSummary {
  const payload = asRecord(value);
  const ticker = asString(payload.ticker, "--");
  return {
    structured_summary: normalizeStructuredSummary(payload.structured_summary),
    source_context: normalizeSourceContext(payload.source_context, ticker),
    task_id: asString(payload.task_id, ""),
    ticker,
    analysis_date: asString(payload.analysis_date, ""),
    market_region: (asString(payload.market_region, "cn_a") as TaskSummary["market_region"]),
    analysts: asArray(payload.analysts) as TaskSummary["analysts"],
    research_depth: asInteger(payload.research_depth, 1),
    llm_provider: asString(payload.llm_provider, ""),
    quick_think_llm: asString(payload.quick_think_llm, ""),
    deep_think_llm: asString(payload.deep_think_llm, ""),
    online_tools: Boolean(payload.online_tools),
    status: asString(payload.status, "queued") as TaskSummary["status"],
    stage: asString(payload.stage, "initializing") as TaskSummary["stage"],
    progress_message: asString(payload.progress_message, ""),
    decision: asNullableString(payload.decision),
    error_message: asNullableString(payload.error_message),
    created_at: asString(payload.created_at, ""),
    started_at: asNullableString(payload.started_at),
    finished_at: asNullableString(payload.finished_at)
  };
}

export function normalizeTaskDetail(value: unknown): TaskDetail {
  const payload = asRecord(value);
  const base = normalizeTaskSummary(payload);
  return {
    ...base,
    config_snapshot: asRecord(payload.config_snapshot),
    overnight_context: normalizeOvernightContext(payload.overnight_context, base.ticker),
    artifact_dir: asString(payload.artifact_dir, ""),
    download_urls: asRecord(payload.download_urls) as Record<string, string>,
    report_status: asRecord(payload.report_status) as Record<string, boolean>
  };
}

export function normalizeTaskArtifacts(value: unknown): TaskArtifactsResponse {
  const payload = asRecord(value);
  const reports = asRecord(payload.reports) as Record<string, string>;
  return {
    task_id: asString(payload.task_id, ""),
    reports: {
      final_trade_decision: asString(reports.final_trade_decision, ""),
      market_report: asString(reports.market_report, ""),
      sentiment_report: asString(reports.sentiment_report, ""),
      news_report: asString(reports.news_report, ""),
      fundamentals_report: asString(reports.fundamentals_report, ""),
      investment_plan: asString(reports.investment_plan, ""),
      trader_investment_plan: asString(reports.trader_investment_plan, "")
    },
    downloads: asRecord(payload.downloads) as Record<string, string>,
    structured: {
      factor_snapshot: Object.keys(asRecord(asRecord(payload.structured).factor_snapshot)).length
        ? (asRecord(asRecord(payload.structured).factor_snapshot) as unknown as FactorSnapshot)
        : null,
      evidence_snapshot: Object.keys(asRecord(asRecord(payload.structured).evidence_snapshot)).length
        ? (asRecord(asRecord(payload.structured).evidence_snapshot) as TaskArtifactsResponse["structured"]["evidence_snapshot"])
        : null,
      structured_decision: Object.keys(asRecord(asRecord(payload.structured).structured_decision)).length
        ? (asRecord(asRecord(payload.structured).structured_decision) as unknown as StructuredDecision)
        : null
    }
  };
}

export function normalizeTaskListResponse(value: unknown): TaskListResponse {
  const payload = asRecord(value);
  const stats = asRecord(payload.stats);
  return {
    items: asArray(payload.items).map((item) => normalizeTaskSummary(item)),
    stats: {
      total: asInteger(stats.total, 0),
      queued: asInteger(stats.queued, 0),
      running: asInteger(stats.running, 0),
      succeeded: asInteger(stats.succeeded, 0),
      failed: asInteger(stats.failed, 0)
    }
  };
}

export function normalizeSystemReadinessResponse(value: unknown): SystemReadinessResponse {
  const payload = asRecord(value);
  const components = asRecord(payload.components);
  return {
    checked_at: asString(payload.checked_at, ""),
    ready: Boolean(payload.ready),
    components: Object.fromEntries(
      Object.entries(components).map(([key, item]) => [key, normalizeReadinessComponent(item, key)])
    )
  };
}

export function normalizeOvernightScanSummary(value: unknown): OvernightScanSummary {
  const payload = asRecord(value);
  return {
    scan_id: asString(payload.scan_id, ""),
    trade_date: asString(payload.trade_date, ""),
    market_region: "cn_a",
    mode: normalizeOvernightModeValue(payload.mode),
    status: asString(payload.status, "queued") as OvernightScanSummary["status"],
    progress_message: asString(payload.progress_message, ""),
    market_message: asString(payload.market_message, ""),
    formal_count: asInteger(payload.formal_count, 0),
    watchlist_count: asInteger(payload.watchlist_count, 0),
    created_at: asString(payload.created_at, ""),
    started_at: asNullableString(payload.started_at),
    finished_at: asNullableString(payload.finished_at),
    error_message: asNullableString(payload.error_message),
    summary_snapshot: asRecord(payload.summary_snapshot),
    top_formal_tickers: asArray(payload.top_formal_tickers).filter((item): item is string => typeof item === "string"),
    scored_count: asInteger(payload.scored_count, 0),
    rejected_count: asInteger(payload.rejected_count, 0),
    validated_formal_count: asInteger(payload.validated_formal_count, 0),
    avg_next_open_return: asNumber(payload.avg_next_open_return),
    best_candidate: normalizeValidatedCandidateSummary(payload.best_candidate),
    worst_candidate: normalizeValidatedCandidateSummary(payload.worst_candidate),
    validation_status: asNullableString(payload.validation_status),
    data_quality: Object.keys(asRecord(payload.data_quality)).length
      ? (asRecord(payload.data_quality) as Record<string, unknown>)
      : { status: "unknown", message: "历史扫描记录缺少数据质量摘要。" },
    provider_route: asRecord(payload.provider_route) as Record<string, string>,
    bias_flags: asArray(payload.bias_flags).filter((item): item is string => typeof item === "string"),
    universe_snapshot_date: asNullableString(payload.universe_snapshot_date),
    evaluation_config_version: asNullableString(payload.evaluation_config_version),
    evaluation_config_hash: asNullableString(payload.evaluation_config_hash)
  };
}

export function normalizeOvernightScanDetail(value: unknown): OvernightScanDetail {
  const payload = asRecord(value);
  const base = normalizeOvernightScanSummary(payload);
  return {
    ...base,
    artifact_dir: asString(payload.artifact_dir, ""),
    download_urls: asRecord(payload.download_urls) as Record<string, string>,
    preliminary_candidates: normalizeCandidateList(payload.preliminary_candidates, "preliminary"),
    total_score_candidates: normalizeCandidateList(payload.total_score_candidates, "scored"),
    formal_recommendations: normalizeCandidateList(payload.formal_recommendations, "formal"),
    watchlist: normalizeCandidateList(payload.watchlist, "watchlist"),
    rejected_candidates: normalizeCandidateList(payload.rejected_candidates, "rejected"),
    excluded_examples: normalizeCandidateList(payload.excluded_examples, "excluded"),
    audit: asRecord(payload.audit)
  };
}

export function normalizeOvernightScanListResponse(value: unknown): OvernightScanListResponse {
  const payload = asRecord(value);
  return {
    items: asArray(payload.items).map((item) => normalizeOvernightScanSummary(item))
  };
}

export function normalizeOvernightScanArtifacts(value: unknown): OvernightScanArtifactsResponse {
  const payload = asRecord(value);
  return {
    scan_id: asString(payload.scan_id, ""),
    summary: asRecord(payload.summary),
    preliminary_candidates: normalizeCandidateList(payload.preliminary_candidates, "preliminary"),
    total_score_candidates: normalizeCandidateList(payload.total_score_candidates, "scored"),
    formal_recommendations: normalizeCandidateList(payload.formal_recommendations, "formal"),
    watchlist: normalizeCandidateList(payload.watchlist, "watchlist"),
    rejected_candidates: normalizeCandidateList(payload.rejected_candidates, "rejected"),
    excluded_examples: normalizeCandidateList(payload.excluded_examples, "excluded"),
    audit: asRecord(payload.audit),
    downloads: asRecord(payload.downloads) as Record<string, string>
  };
}

export function normalizeOvernightTrackedTrade(value: unknown): OvernightTrackedTrade {
  const payload = asRecord(value);
  return {
    trade_id: asString(payload.trade_id, ""),
    trade_date: asString(payload.trade_date, ""),
    market_region: "cn_a",
    scan_id: asString(payload.scan_id, ""),
    scan_mode: normalizeOvernightModeValue(payload.scan_mode),
    source_bucket: normalizeTrackedTradeSourceBucketValue(payload.source_bucket),
    ticker: asString(payload.ticker, ""),
    name: readableName(payload.name, asNullableString(payload.ticker)) ?? asString(payload.ticker, ""),
    pool: asString(payload.pool, "--"),
    quality: normalizeOvernightQualityValue(payload.quality),
    quick_score: asNumber(payload.quick_score) ?? 0,
    total_score: asNumber(payload.total_score) ?? 0,
    factor_breakdown: asRecord(payload.factor_breakdown) as Record<string, number>,
    tail_metrics: normalizeTailMetrics(payload.tail_metrics),
    confirmed_at: asString(payload.confirmed_at, ""),
    entry_target_time: asString(payload.entry_target_time, "14:55"),
    entry_price: asNumber(payload.entry_price),
    entry_time_used: asNullableString(payload.entry_time_used),
    exit_target_time: asString(payload.exit_target_time, "10:00"),
    exit_trade_date: asNullableString(payload.exit_trade_date),
    exit_price: asNumber(payload.exit_price),
    exit_time_used: asNullableString(payload.exit_time_used),
    strategy_return: asNumber(payload.strategy_return),
    status: normalizeTrackedTradeStatusValue(payload.status),
    last_error: asNullableString(payload.last_error),
    last_checked_at: asNullableString(payload.last_checked_at),
    created_at: asString(payload.created_at, ""),
    updated_at: asString(payload.updated_at, "")
  };
}

export function normalizeOvernightTrackedTradeListResponse(value: unknown): OvernightTrackedTradeListResponse {
  const payload = asRecord(value);
  const stats = asRecord(payload.stats);
  return {
    items: asArray(payload.items).map((item) => normalizeOvernightTrackedTrade(item)),
    stats: {
      total_days: asInteger(stats.total_days, 0),
      validated_days: asInteger(stats.validated_days, 0),
      pending_count: asInteger(stats.pending_count, 0),
      unavailable_count: asInteger(stats.unavailable_count, 0),
      avg_return: asNumber(stats.avg_return),
      win_rate: asNumber(stats.win_rate),
      cumulative_return: asNumber(stats.cumulative_return)
    } as OvernightTrackedTradeStats
  };
}

export function normalizeOvernightReviewSummary(value: unknown): OvernightReviewSummary {
  const payload = asRecord(value);
  return {
    review_id: asString(payload.review_id, ""),
    end_trade_date: asString(payload.end_trade_date, ""),
    market_region: "cn_a",
    window_days: asInteger(payload.window_days, 60),
    mode: "strict",
    return_basis: normalizeReviewReturnBasisValue(payload.return_basis),
    status: asString(payload.status, "queued") as OvernightReviewSummary["status"],
    progress_message: asString(payload.progress_message, ""),
    created_at: asString(payload.created_at, ""),
    started_at: asNullableString(payload.started_at),
    finished_at: asNullableString(payload.finished_at),
    error_message: asNullableString(payload.error_message),
    summary_snapshot: normalizeReviewSummarySnapshot(payload.summary_snapshot),
    data_quality: Object.keys(asRecord(payload.data_quality)).length
      ? (asRecord(payload.data_quality) as Record<string, unknown>)
      : { status: "unknown", message: "历史验证记录缺少数据质量摘要。" },
    provider_route: asRecord(payload.provider_route) as Record<string, string>,
    bias_flags: asArray(payload.bias_flags).filter((item): item is string => typeof item === "string"),
    universe_snapshot_date: asNullableString(payload.universe_snapshot_date),
    survivorship_bias: Boolean(payload.survivorship_bias),
    evaluation_config_version: asNullableString(payload.evaluation_config_version),
    evaluation_config_hash: asNullableString(payload.evaluation_config_hash),
    regime_breakdown: normalizeBreakdownList(payload.regime_breakdown),
    pool_breakdown: normalizeBreakdownList(payload.pool_breakdown),
    tail_quality_breakdown: normalizeBreakdownList(payload.tail_quality_breakdown)
  };
}

export function normalizeOvernightReviewDetail(value: unknown): OvernightReviewDetail {
  const payload = asRecord(value);
  const base = normalizeOvernightReviewSummary(payload);
  return {
    ...base,
    artifact_dir: asString(payload.artifact_dir, ""),
    download_urls: asRecord(payload.download_urls) as Record<string, string>,
    audit: asRecord(payload.audit)
  };
}

export function normalizeOvernightReviewListResponse(value: unknown): OvernightReviewListResponse {
  const payload = asRecord(value);
  return {
    items: asArray(payload.items).map((item) => normalizeOvernightReviewSummary(item))
  };
}

export function normalizeOvernightReviewArtifacts(value: unknown): OvernightReviewArtifactsResponse {
  const payload = asRecord(value);
  return {
    review_id: asString(payload.review_id, ""),
    summary:
      normalizeReviewSummarySnapshot(payload.summary) ?? {
        end_trade_date: "",
        market_region: "cn_a",
        window_days: 60,
        mode: "strict",
        return_basis: "buy_1455_sell_next_day_1000",
        candidate_count: 0,
        trade_count: 0,
        days_evaluated: 0,
        days_with_formal_picks: 0,
        days_with_trade: 0,
        avg_strategy_return: null,
        median_strategy_return: null,
        avg_daily_strategy_return: null,
        avg_benchmark_return: null,
        avg_next_open_return: null,
        median_next_open_return: null,
        positive_pick_rate: null,
        avg_daily_equal_weight_return: null,
        avg_benchmark_next_open_return: null,
        avg_excess_return: null,
        has_valid_samples: false,
      },
    daily_results: asArray(payload.daily_results).map((item) => normalizeReviewDailyResult(item)),
    candidate_results: asArray(payload.candidate_results).map((item) => normalizeReviewCandidateResult(item)),
    audit: asRecord(payload.audit),
    downloads: asRecord(payload.downloads) as Record<string, string>
  };
}
