export type TaskStatus = "queued" | "running" | "succeeded" | "failed";
export type TaskStage =
  | "initializing"
  | "market"
  | "social"
  | "news"
  | "fundamentals"
  | "research"
  | "trader"
  | "risk"
  | "completed";
export type AnalystValue = "market" | "social" | "news" | "fundamentals";
export type MarketRegion = "cn_a" | "us";
export type OvernightMode = "strict" | "research_fallback";
export type OvernightQuality = "real" | "proxy" | "missing" | "invalid";

export interface OptionItem {
  value: string;
  label: string;
}

export interface MarketRegionOption {
  value: MarketRegion;
  label: string;
  description: string;
  example: string;
}

export interface DepthOption {
  value: number;
  label: string;
  description: string;
}

export interface ProviderOption {
  value: string;
  label: string;
  base_url: string;
}

export interface ModelsByProvider {
  quick: OptionItem[];
  deep: OptionItem[];
}

export interface TaskDefaults {
  ticker: string;
  analysis_date: string;
  market_region: MarketRegion;
  analysts: AnalystValue[];
  research_depth: number;
  llm_provider: string;
  quick_think_llm: string;
  deep_think_llm: string;
  online_tools: boolean;
}

export interface TaskOptionsResponse {
  analysts: OptionItem[];
  market_regions: MarketRegionOption[];
  research_depths: DepthOption[];
  providers: ProviderOption[];
  model_options: Record<string, ModelsByProvider>;
  defaults: TaskDefaults;
}

export interface StructuredSummary {
  composite_score: number | null;
  confidence: number | null;
  recommended_action: string | null;
  primary_driver: string | null;
  primary_risk: string | null;
}

export interface TaskSourceContext {
  type: string;
  scan_id?: string | null;
  trade_date?: string | null;
  mode?: string | null;
  ticker?: string | null;
  name?: string | null;
}

export interface TaskSummary {
  structured_summary?: StructuredSummary | null;
  source_context?: TaskSourceContext | null;
  task_id: string;
  ticker: string;
  analysis_date: string;
  market_region: MarketRegion;
  analysts: AnalystValue[];
  research_depth: number;
  llm_provider: string;
  quick_think_llm: string;
  deep_think_llm: string;
  online_tools: boolean;
  status: TaskStatus;
  stage: TaskStage;
  progress_message: string;
  decision: string | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface TaskDetail extends TaskSummary {
  config_snapshot: Record<string, unknown>;
  overnight_context?: OvernightTaskContext | null;
  artifact_dir: string;
  download_urls: Record<string, string>;
  report_status: Record<string, boolean>;
}

export interface ReadinessComponent {
  name: string;
  ok: boolean;
  status: string;
  message: string;
  configured?: boolean | null;
  active_keys?: number | null;
  rotation_enabled?: boolean | null;
  probe_detail?: string | null;
  path?: string | null;
  dist?: string | null;
  base_url?: string | null;
  provider?: string | null;
  model?: string | null;
  rows?: number | null;
  dist_exists?: boolean | null;
  index_exists?: boolean | null;
  checks?: Record<string, unknown> | null;
}

export interface SystemReadinessResponse {
  checked_at: string;
  ready: boolean;
  components: Record<string, ReadinessComponent>;
}

export interface TaskListStats {
  total: number;
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
}

export interface TaskListResponse {
  items: TaskSummary[];
  stats: TaskListStats;
}

export type EvidenceImpact = "positive" | "negative" | "neutral";

export interface FactorSignal {
  signal: string;
  value: unknown;
  impact: EvidenceImpact;
  source: string;
  weight: number;
}

export interface FactorSubscore {
  score: number;
  value: unknown;
  summary: string;
  weight: number;
}

export interface FactorBlock {
  score: number;
  confidence: number;
  summary: string;
  subscores?: Record<string, FactorSubscore>;
  top_positive_signals?: FactorSignal[];
  top_negative_signals?: FactorSignal[];
  confidence_drivers?: string[];
}

export interface FactorSnapshot {
  strategy?: string;
  market_region: MarketRegion;
  symbol: string;
  display_symbol: string;
  trade_date: string;
  composite_score: number;
  confidence: number;
  recommended_action: string;
  scores: Record<string, FactorBlock>;
}

export interface StructuredDecision {
  decision: string;
  score: number;
  confidence: number;
  summary: string;
  source: string;
  threshold_policy?: {
    style: string;
    buy_at_or_above: number;
    sell_at_or_below: number;
    min_confidence_for_directional_call: number;
  };
  primary_drivers?: string[];
  risk_flags?: string[];
}

export interface EvidenceGroup {
  strengths: FactorSignal[];
  risks: FactorSignal[];
  raw_metrics: FactorSignal[];
}

export interface TaskArtifactsResponse {
  task_id: string;
  reports: Record<string, string>;
  downloads: Record<string, string>;
  structured: {
    factor_snapshot?: FactorSnapshot | null;
    evidence_snapshot?: Record<string, EvidenceGroup | Array<Record<string, unknown>> | unknown> | null;
    structured_decision?: StructuredDecision | null;
  };
}

export interface CreateTaskRequest {
  ticker: string;
  analysis_date: string;
  market_region: MarketRegion;
  analysts: AnalystValue[];
  research_depth: number;
  llm_provider: string;
  quick_think_llm: string;
  deep_think_llm: string;
  online_tools: boolean;
}

export interface CreateTaskSource {
  type: "overnight_scan";
  scanId: string;
  tradeDate: string;
  mode: OvernightMode;
  name?: string;
}

export interface TailMetricsSummary {
  source?: string;
  quality?: OvernightQuality;
  tail_return_pct?: number;
  tail_amount_ratio?: number;
  last10_return_pct?: number;
  close_at_high_ratio?: number;
  auction_strength?: number;
  rows?: number;
  note?: string;
  provider_chain?: string[];
}

export interface OvernightTaskContext {
  scan_id?: string;
  scan_trade_date?: string;
  scan_mode?: string;
  source_name?: string;
  bucket?: string;
  quality?: OvernightQuality;
  quick_score?: number;
  total_score?: number;
  factor_breakdown?: Record<string, number>;
  tail_metrics?: TailMetricsSummary | null;
  provider_route?: Record<string, string> | null;
  evaluation_config_version?: string | null;
  evaluation_config_hash?: string | null;
  validation_status?: string | null;
  next_open_return?: number | null;
  next_open_date?: string | null;
}

export interface OvernightStratifiedBreakdownBucket {
  group: string;
  days_with_formal_picks: number;
  candidate_count: number;
  avg_next_open_return?: number | null;
  avg_excess_return?: number | null;
  positive_pick_rate?: number | null;
}

export interface OvernightCandidate {
  ticker: string;
  name: string;
  bucket?: string | null;
  pool: string;
  quality: OvernightQuality;
  latest: number;
  pct: number;
  amount: number;
  turnover: number;
  quick_score: number;
  total_score: number;
  factor_breakdown: Record<string, number>;
  selection_stage?: string | null;
  rejected_reason?: string | null;
  tail_metrics?: TailMetricsSummary | null;
  filter_reason?: string | null;
  excluded_from_final?: string | null;
  linked_task_id?: string | null;
  linked_task_status?: TaskStatus | null;
  linked_task_decision?: string | null;
  validation_status?: string | null;
  next_open_return?: number | null;
  next_open_date?: string | null;
  scan_close_price?: number | null;
}

export interface OvernightValidatedCandidateSummary {
  ticker: string;
  name: string;
  next_open_return: number | null;
  next_open_date?: string | null;
}

export interface OvernightScanSummary {
  scan_id: string;
  trade_date: string;
  market_region: "cn_a";
  mode: OvernightMode;
  status: TaskStatus;
  progress_message: string;
  market_message: string;
  formal_count: number;
  watchlist_count: number;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error_message: string | null;
  summary_snapshot?: Record<string, unknown> | null;
  top_formal_tickers: string[];
  scored_count: number;
  rejected_count: number;
  validated_formal_count: number;
  avg_next_open_return?: number | null;
  best_candidate?: OvernightValidatedCandidateSummary | null;
  worst_candidate?: OvernightValidatedCandidateSummary | null;
  validation_status?: string | null;
  data_quality?: Record<string, unknown> | null;
  provider_route?: Record<string, string> | null;
  bias_flags: string[];
  universe_snapshot_date?: string | null;
  evaluation_config_version?: string | null;
  evaluation_config_hash?: string | null;
}

export interface OvernightScanDetail extends OvernightScanSummary {
  artifact_dir: string;
  download_urls: Record<string, string>;
  preliminary_candidates: OvernightCandidate[];
  total_score_candidates: OvernightCandidate[];
  formal_recommendations: OvernightCandidate[];
  watchlist: OvernightCandidate[];
  rejected_candidates: OvernightCandidate[];
  excluded_examples: OvernightCandidate[];
  audit: Record<string, unknown>;
}

export interface OvernightScanListResponse {
  items: OvernightScanSummary[];
}

export interface OvernightScanArtifactsResponse {
  scan_id: string;
  summary: Record<string, unknown>;
  preliminary_candidates: OvernightCandidate[];
  total_score_candidates: OvernightCandidate[];
  formal_recommendations: OvernightCandidate[];
  watchlist: OvernightCandidate[];
  rejected_candidates: OvernightCandidate[];
  excluded_examples: OvernightCandidate[];
  audit: Record<string, unknown>;
  downloads: Record<string, string>;
}

export interface CreateOvernightScanRequest {
  trade_date: string;
  market_region: "cn_a";
  mode: OvernightMode;
}

export interface OvernightReviewSummarySnapshot {
  end_trade_date: string;
  market_region: "cn_a";
  window_days: number;
  mode: "strict";
  return_basis: "next_open";
  candidate_count: number;
  days_evaluated: number;
  days_with_formal_picks: number;
  avg_next_open_return: number | null;
  median_next_open_return: number | null;
  positive_pick_rate: number | null;
  avg_daily_equal_weight_return: number | null;
  avg_benchmark_next_open_return: number | null;
  avg_excess_return: number | null;
  has_valid_samples: boolean;
  headline_message?: string | null;
  best_day?: OvernightReviewExtrema | null;
  worst_day?: OvernightReviewExtrema | null;
  audit?: Record<string, unknown> | null;
  evaluation_config_version?: string | null;
  evaluation_config_hash?: string | null;
  regime_breakdown?: OvernightStratifiedBreakdownBucket[];
  pool_breakdown?: OvernightStratifiedBreakdownBucket[];
  tail_quality_breakdown?: OvernightStratifiedBreakdownBucket[];
}

export interface OvernightReviewExtrema {
  trade_date: string;
  equal_weight_next_open_return: number | null;
  benchmark_next_open_return: number | null;
  avg_excess_return: number | null;
  formal_tickers: string[];
}

export interface OvernightReviewSummary {
  review_id: string;
  end_trade_date: string;
  market_region: "cn_a";
  window_days: number;
  mode: "strict";
  return_basis: "next_open";
  status: TaskStatus;
  progress_message: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error_message: string | null;
  summary_snapshot?: OvernightReviewSummarySnapshot | null;
  data_quality?: Record<string, unknown> | null;
  provider_route?: Record<string, string> | null;
  bias_flags: string[];
  universe_snapshot_date?: string | null;
  survivorship_bias: boolean;
  evaluation_config_version?: string | null;
  evaluation_config_hash?: string | null;
  regime_breakdown: OvernightStratifiedBreakdownBucket[];
  pool_breakdown: OvernightStratifiedBreakdownBucket[];
  tail_quality_breakdown: OvernightStratifiedBreakdownBucket[];
}

export interface OvernightReviewDetail extends OvernightReviewSummary {
  artifact_dir: string;
  download_urls: Record<string, string>;
  audit: Record<string, unknown>;
}

export interface OvernightReviewListResponse {
  items: OvernightReviewSummary[];
}

export interface OvernightReviewDailyResult {
  trade_date: string;
  formal_count: number;
  watchlist_count: number;
  formal_tickers: string[];
  market_message: string;
  benchmark_next_open_return: number | null;
  equal_weight_next_open_return: number | null;
  avg_excess_return: number | null;
  tail_quality_counts?: Record<string, number>;
  passed_filters?: number;
  failed_filters?: number;
}

export interface OvernightReviewCandidateResult {
  trade_date: string;
  category: "formal" | "watchlist";
  ticker: string;
  name: string;
  quality: OvernightQuality;
  quick_score: number;
  total_score: number;
  factor_breakdown: Record<string, number>;
  tail_metrics?: TailMetricsSummary | null;
  filter_reason?: string | null;
  next_trade_date?: string | null;
  scan_close_price?: number | null;
  next_open_return?: number | null;
  benchmark_next_open_return?: number | null;
  excess_return?: number | null;
  counted_in_performance: boolean;
}

export interface OvernightReviewArtifactsResponse {
  review_id: string;
  summary: OvernightReviewSummarySnapshot;
  daily_results: OvernightReviewDailyResult[];
  candidate_results: OvernightReviewCandidateResult[];
  audit: Record<string, unknown>;
  downloads: Record<string, string>;
}

export interface CreateOvernightReviewRequest {
  end_trade_date: string;
  market_region: "cn_a";
}
