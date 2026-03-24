import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { dashboardApi } from "../api";
import { StatusBadge } from "../components/StatusBadge";
import { getOvernightModeLabel, getOvernightQualityLabel } from "../labels";
import type {
  CreateTaskRequest,
  OvernightCandidate,
  OvernightMode,
  OvernightQuality,
  OvernightReviewArtifactsResponse,
  OvernightReviewDetail,
  OvernightReviewListResponse,
  OvernightScanDetail,
  OvernightScanListResponse,
  OvernightStratifiedBreakdownBucket,
  TaskOptionsResponse
} from "../types";

const DEFAULT_MODE: OvernightMode = "strict";

const VALIDATION_LABELS: Record<string, string> = {
  validated: "已验证",
  pending: "待次日开盘",
  unavailable: "数据缺失",
  watchlist_only: "仅观察名单",
  empty: "无正式推荐"
};

const SELECTION_LABELS: Record<string, string> = {
  preliminary: "通过初筛",
  scored: "已完成总评分",
  formal: "正式推荐",
  watchlist: "观察名单",
  rejected: "已淘汰"
};

const REJECTED_REASON_LABELS: Record<string, string> = {
  missing_history: "缺少足够历史日线，未完成总评分",
  tail_quality_ineligible: "尾盘分钟线缺失或无效，当前质量不允许进入观察名单",
  below_watchlist_threshold: "已完成总评分，但低于观察名单阈值",
  watchlist_capacity_trim: "观察名单数量已满，按总分排序被截断",
  no_total_score: "当前没有形成有效总评分",
  formal_requires_real_tail: "尾盘质量达不到正式推荐要求"
};

const GROUP_LABELS: Record<string, string> = {
  normal: "正常市场",
  cautious: "谨慎市场",
  risk_off: "防守市场",
  main: "主板",
  gem: "创业板",
  star: "科创板",
  other: "其他",
  real: "真实尾盘",
  proxy: "代理尾盘",
  missing: "缺失",
  invalid: "无效"
};

const CAPABILITY_LABELS: Record<string, string> = {
  index_snapshot: "指数快照",
  realtime_spot: "实时现货",
  intraday_minute: "尾盘分钟线"
};

interface HubState {
  options: TaskOptionsResponse | null;
  scanList: OvernightScanListResponse | null;
  selectedScan: OvernightScanDetail | null;
  reviewList: OvernightReviewListResponse | null;
  selectedReview: OvernightReviewDetail | null;
  selectedReviewArtifacts: OvernightReviewArtifactsResponse | null;
}

const todayIso = () => new Date().toISOString().slice(0, 10);
const fmtScore = (value?: number | null) => (typeof value === "number" ? value.toFixed(1) : "--");
const fmtPct = (value?: number | null) => (typeof value === "number" ? `${value.toFixed(2)}%` : "--");
const fmtMode = (value?: string | null) => (value ? getOvernightModeLabel(value as OvernightMode) : "--");
const fmtQuality = (value?: string | null) => (value ? getOvernightQualityLabel(value as OvernightQuality) : "--");
const fmtValidation = (value?: string | null) => (value ? VALIDATION_LABELS[value] ?? value : "未验证");
const fmtSelection = (value?: string | null) => (value ? SELECTION_LABELS[value] ?? value : "--");
const fmtRejectedReason = (value?: string | null) => (value ? REJECTED_REASON_LABELS[value] ?? value : "--");
const fmtGroup = (value: string) => GROUP_LABELS[value] ?? value;
const fmtCapability = (value: string) => CAPABILITY_LABELS[value] ?? value;

const asRecord = (value: unknown) =>
  value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
const asStrings = (value: unknown) => (Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : []);

const routeText = (value?: Record<string, unknown> | null) => {
  if (!value || !Object.keys(value).length) return "当前没有额外的来源说明。";
  return Object.entries(value)
    .map(([key, item]) => `${key}: ${String(item)}`)
    .join(" / ");
};

const factorText = (value?: Record<string, number> | null) => {
  if (!value || !Object.keys(value).length) return "暂无因子拆解";
  return Object.entries(value)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 4)
    .map(([key, item]) => `${key}: ${item.toFixed(1)}`)
    .join(" / ");
};

function buildTaskPayload(candidate: OvernightCandidate, tradeDate: string, options: TaskOptionsResponse): CreateTaskRequest {
  return {
    ticker: candidate.ticker,
    analysis_date: tradeDate,
    market_region: "cn_a",
    analysts: options.defaults.analysts,
    research_depth: options.defaults.research_depth,
    llm_provider: options.defaults.llm_provider,
    quick_think_llm: options.defaults.quick_think_llm,
    deep_think_llm: options.defaults.deep_think_llm,
    online_tools: options.defaults.online_tools
  };
}

function useHub() {
  const [state, setState] = useState<HubState>({
    options: null,
    scanList: null,
    selectedScan: null,
    reviewList: null,
    selectedReview: null,
    selectedReviewArtifacts: null
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [options, scanList, reviewList] = await Promise.all([
          dashboardApi.getOptions(),
          dashboardApi.getOvernightScans(),
          dashboardApi.getOvernightReviews()
        ]);
        const latestScanId = scanList.items[0]?.scan_id;
        const latestReviewId = reviewList.items[0]?.review_id;
        const [selectedScan, selectedReview, selectedReviewArtifacts] = await Promise.all([
          latestScanId ? dashboardApi.getOvernightScan(latestScanId) : Promise.resolve(null),
          latestReviewId ? dashboardApi.getOvernightReview(latestReviewId) : Promise.resolve(null),
          latestReviewId ? dashboardApi.getOvernightReviewArtifacts(latestReviewId) : Promise.resolve(null)
        ]);
        if (!cancelled) {
          setState({ options, scanList, selectedScan, reviewList, selectedReview, selectedReviewArtifacts });
          setError("");
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "隔夜推荐数据加载失败。");
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  return { state, setState, error, setError, loading };
}

function BreakdownTable({ title, items, emptyText }: { title: string; items: OvernightStratifiedBreakdownBucket[]; emptyText: string }) {
  return (
    <article className="analysis-note-card">
      <strong>{title}</strong>
      {items.length ? (
        <div className="breakdown-table">
          <table>
            <thead>
              <tr>
                <th>分组</th>
                <th>有推荐日</th>
                <th>候选数</th>
                <th>平均次日开盘收益</th>
                <th>平均超额收益</th>
                <th>正收益率</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={`${title}-${item.group}`}>
                  <td>{fmtGroup(item.group)}</td>
                  <td>{item.days_with_formal_picks}</td>
                  <td>{item.candidate_count}</td>
                  <td>{fmtPct(item.avg_next_open_return)}</td>
                  <td>{fmtPct(item.avg_excess_return)}</td>
                  <td>{typeof item.positive_pick_rate === "number" ? `${(item.positive_pick_rate * 100).toFixed(1)}%` : "--"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="empty-state compact-empty">{emptyText}</div>
      )}
    </article>
  );
}

function CandidateSection({
  title,
  items,
  emptyText,
  showRejectedReason,
  showActions,
  busyKey,
  scanId,
  onAnalyze,
  onOpenTask
}: {
  title: string;
  items: OvernightCandidate[];
  emptyText: string;
  showRejectedReason?: boolean;
  showActions?: boolean;
  busyKey: string;
  scanId?: string;
  onAnalyze: (candidate: OvernightCandidate) => void;
  onOpenTask: (taskId: string) => void;
}) {
  return (
    <section className="panel panel-wide">
      <div className="panel-header">
        <div>
          <p className="eyebrow">{title}</p>
          <h3>{title}</h3>
        </div>
      </div>
      {items.length ? (
        <div className="breakdown-table">
          <table>
            <thead>
              <tr>
                <th>股票代码</th>
                <th>名称</th>
                <th>板块</th>
                <th>Quick Score</th>
                <th>Total Score</th>
                <th>尾盘质量</th>
                <th>{showRejectedReason ? "淘汰原因" : "当前去向"}</th>
                <th>因子拆解</th>
                <th>任务</th>
              </tr>
            </thead>
            <tbody>
              {items.map((candidate) => (
                <tr key={`${title}:${candidate.ticker}`}>
                  <td className="notranslate" translate="no" lang="en">
                    {candidate.ticker}
                  </td>
                  <td>{candidate.name}</td>
                  <td>{candidate.pool}</td>
                  <td>{fmtScore(candidate.quick_score)}</td>
                  <td>{fmtScore(candidate.total_score)}</td>
                  <td>{fmtQuality(candidate.quality)}</td>
                  <td className="text-multiline">
                    {showRejectedReason ? fmtRejectedReason(candidate.rejected_reason) : fmtSelection(candidate.selection_stage)}
                  </td>
                  <td className="text-multiline">{factorText(candidate.factor_breakdown)}</td>
                  <td>
                    {showActions ? (
                      candidate.linked_task_id ? (
                        <button type="button" className="table-link-button" onClick={() => onOpenTask(candidate.linked_task_id!)}>
                          查看分析
                        </button>
                      ) : (
                        <button
                          type="button"
                          className="table-link-button"
                          disabled={busyKey === `${scanId}:${candidate.ticker}`}
                          onClick={() => onAnalyze(candidate)}
                        >
                          {busyKey === `${scanId}:${candidate.ticker}` ? "创建中..." : "深度分析"}
                        </button>
                      )
                    ) : (
                      <span>{candidate.linked_task_id ? "已创建" : "--"}</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="empty-state">{emptyText}</div>
      )}
    </section>
  );
}

export function OvernightPage() {
  const navigate = useNavigate();
  const { state, setState, error, setError, loading } = useHub();
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [mode, setMode] = useState<OvernightMode>(DEFAULT_MODE);
  const [creatingScan, setCreatingScan] = useState(false);
  const [creatingReview, setCreatingReview] = useState(false);
  const [validatingScan, setValidatingScan] = useState(false);
  const [submittingTaskKey, setSubmittingTaskKey] = useState("");

  const shouldPollScan = state.selectedScan?.status === "queued" || state.selectedScan?.status === "running";
  const shouldPollReview = state.selectedReview?.status === "queued" || state.selectedReview?.status === "running";

  useEffect(() => {
    if (!shouldPollScan && !shouldPollReview) return;
    let cancelled = false;

    const refresh = async () => {
      try {
        const [scanList, reviewList] = await Promise.all([dashboardApi.getOvernightScans(), dashboardApi.getOvernightReviews()]);
        const [selectedScan, selectedReview, selectedReviewArtifacts] = await Promise.all([
          state.selectedScan?.scan_id ? dashboardApi.getOvernightScan(state.selectedScan.scan_id) : Promise.resolve(null),
          state.selectedReview?.review_id ? dashboardApi.getOvernightReview(state.selectedReview.review_id) : Promise.resolve(null),
          state.selectedReview?.review_id ? dashboardApi.getOvernightReviewArtifacts(state.selectedReview.review_id) : Promise.resolve(null)
        ]);
        if (!cancelled) {
          setState((current) => ({
            ...current,
            scanList,
            reviewList,
            selectedScan,
            selectedReview,
            selectedReviewArtifacts
          }));
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "隔夜推荐轮询失败。");
      }
    };

    const timer = window.setInterval(() => void refresh(), 3000);
    void refresh();
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [
    setError,
    setState,
    shouldPollReview,
    shouldPollScan,
    state.selectedReview?.review_id,
    state.selectedScan?.scan_id
  ]);

  const scan = state.selectedScan;
  const review = state.selectedReview;
  const reviewArtifacts = state.selectedReviewArtifacts;
  const reviewSummary = reviewArtifacts?.summary ?? review?.summary_snapshot ?? null;
  const recentDailyResults = reviewArtifacts?.daily_results?.slice(0, 6) ?? [];
  const enabledCapabilities = asStrings(scan?.audit?.qveris_enabled_capabilities);
  const skippedCapabilities = asStrings(scan?.audit?.qveris_skipped_capabilities);
  const skipReasons = asStrings(scan?.audit?.qveris_skip_reasons);
  const budgetEntries = Object.entries(asRecord(scan?.audit?.qveris_budget_policy)).map(([key, value]) => ({
    key,
    policy: asRecord(value)
  }));

  const createScan = async () => {
    setCreatingScan(true);
    try {
      const detail = await dashboardApi.createOvernightScan({ trade_date: tradeDate, market_region: "cn_a", mode });
      const scanList = await dashboardApi.getOvernightScans();
      setState((current) => ({ ...current, scanList, selectedScan: detail }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "隔夜扫描创建失败。");
    } finally {
      setCreatingScan(false);
    }
  };

  const createReview = async () => {
    setCreatingReview(true);
    try {
      const detail = await dashboardApi.createOvernightReview({ end_trade_date: tradeDate, market_region: "cn_a" });
      const [reviewList, selectedReviewArtifacts] = await Promise.all([
        dashboardApi.getOvernightReviews(),
        dashboardApi.getOvernightReviewArtifacts(detail.review_id)
      ]);
      setState((current) => ({ ...current, reviewList, selectedReview: detail, selectedReviewArtifacts }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "历史验证创建失败。");
    } finally {
      setCreatingReview(false);
    }
  };

  const validateScan = async () => {
    if (!scan) return;
    setValidatingScan(true);
    try {
      const [selectedScan, scanList] = await Promise.all([
        dashboardApi.validateOvernightScan(scan.scan_id),
        dashboardApi.getOvernightScans()
      ]);
      setState((current) => ({ ...current, selectedScan, scanList }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "次日结果回填失败。");
    } finally {
      setValidatingScan(false);
    }
  };

  const runDeepAnalysis = async (candidate: OvernightCandidate) => {
    if (!scan || !state.options) return;
    if (candidate.linked_task_id) {
      navigate(`/tasks/${candidate.linked_task_id}`);
      return;
    }
    const key = `${scan.scan_id}:${candidate.ticker}`;
    setSubmittingTaskKey(key);
    try {
      const task = await dashboardApi.createTask(buildTaskPayload(candidate, scan.trade_date, state.options), {
        type: "overnight_scan",
        scanId: scan.scan_id,
        tradeDate: scan.trade_date,
        mode: scan.mode,
        name: candidate.name
      });
      const selectedScan = await dashboardApi.getOvernightScan(scan.scan_id);
      setState((current) => ({ ...current, selectedScan }));
      navigate(`/tasks/${task.task_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "深度分析任务创建失败。");
    } finally {
      setSubmittingTaskKey("");
    }
  };

  return (
    <div className="content-shell">
      <section className="hero-card">
        <div>
          <p className="eyebrow">A股隔夜研究工作台</p>
          <h2>收盘扫描、总评分候选与次日回填</h2>
          <p className="hero-copy">
            这里会同时展示正式推荐、Total Score 候选池、淘汰原因、初筛结果、观察名单与历史验证结果。
            QVeris 采用批量优先、额度敏感的补充策略，不会为了补数据对股票逐只请求。
          </p>
        </div>
        <div className="hero-actions">
          <button type="button" className="primary-button" disabled={creatingScan} onClick={() => void createScan()}>
            {creatingScan ? "正在创建扫描..." : "发起隔夜扫描"}
          </button>
          <button type="button" className="secondary-button" disabled={creatingReview} onClick={() => void createReview()}>
            {creatingReview ? "正在创建验证..." : "发起历史验证"}
          </button>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">执行入口</p>
            <h3>扫描与回填</h3>
          </div>
        </div>
        <div className="analysis-main-grid">
          <div className="analysis-block">
            <div className="analysis-form-grid">
              <label className="analysis-field">
                <span>扫描日期</span>
                <input type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} />
              </label>
              <label className="analysis-field">
                <span>扫描模式</span>
                <select value={mode} onChange={(event) => setMode(event.target.value as OvernightMode)}>
                  <option value="strict">严格模式</option>
                  <option value="research_fallback">研究回退</option>
                </select>
              </label>
            </div>
            <div className="hero-actions">
              <button type="button" className="primary-button" disabled={creatingScan} onClick={() => void createScan()}>
                {creatingScan ? "正在创建扫描..." : "启动扫描"}
              </button>
              <button
                type="button"
                className="secondary-button"
                disabled={!scan || scan.status !== "succeeded" || validatingScan}
                onClick={() => void validateScan()}
              >
                {validatingScan ? "正在回填..." : "回填次日结果"}
              </button>
            </div>
          </div>
          <div className="analysis-note-card panel-subtle">
            <strong>QVeris 使用原则</strong>
            <p>
              指数、实时现货和尾盘分钟线都遵循批量优先、额度上限和单次扫描调用限制。如果没有合适的批量能力或候选池种子，
              系统会明确跳过 QVeris，而不是为了补数据扫完整个市场。
            </p>
          </div>
        </div>
      </section>

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">扫描记录</p>
            <h3>最近隔夜扫描</h3>
          </div>
        </div>
        <div className="scan-history-row">
          {loading ? (
            <div className="empty-state">正在加载扫描记录...</div>
          ) : state.scanList?.items.length ? (
            state.scanList.items.slice(0, 8).map((item) => (
              <button
                key={item.scan_id}
                type="button"
                className={scan?.scan_id === item.scan_id ? "scan-chip scan-chip-active" : "scan-chip"}
                onClick={() =>
                  void dashboardApi.getOvernightScan(item.scan_id).then((selectedScan) =>
                    setState((current) => ({ ...current, selectedScan }))
                  )
                }
              >
                <strong>{item.trade_date}</strong>
                <span>{item.formal_count} 只正式推荐</span>
              </button>
            ))
          ) : (
            <div className="empty-state">还没有隔夜扫描记录。</div>
          )}
        </div>

        {scan ? (
          <div className="overnight-home-grid">
            <article className="overnight-highlight-card">
              <div className="decision-head">
                <strong>本次扫描摘要</strong>
              </div>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row"><span>扫描日期</span><strong>{scan.trade_date}</strong></div>
                <div className="analysis-estimate-row"><span>模式</span><strong>{fmtMode(scan.mode)}</strong></div>
                <div className="analysis-estimate-row"><span>正式推荐</span><strong>{scan.formal_count}</strong></div>
                <div className="analysis-estimate-row"><span>Total Score 候选</span><strong>{scan.scored_count}</strong></div>
                <div className="analysis-estimate-row"><span>已淘汰</span><strong>{scan.rejected_count}</strong></div>
                <div className="analysis-estimate-row">
                  <span>配置哈希</span>
                  <strong className="notranslate" translate="no" lang="en">{scan.evaluation_config_hash ?? "--"}</strong>
                </div>
              </div>
            </article>

            <article className="overnight-highlight-card">
              <div className="decision-head">
                <strong>数据质量与来源</strong>
              </div>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row"><span>市场提示</span><strong className="text-multiline">{scan.market_message}</strong></div>
                <div className="analysis-estimate-row"><span>数据质量</span><strong>{String(scan.data_quality?.status ?? "--")}</strong></div>
                <div className="analysis-estimate-row"><span>快照日期</span><strong>{scan.universe_snapshot_date ?? "--"}</strong></div>
                <div className="analysis-estimate-row"><span>Provider Route</span><strong className="text-multiline">{routeText(scan.provider_route)}</strong></div>
              </div>
              <p className="card-note">
                {scan.bias_flags.length ? `偏差标记：${scan.bias_flags.join(" / ")}` : "当前没有额外偏差标记。"}
              </p>
            </article>

            <article className="overnight-highlight-card overnight-highlight-card-wide">
              <div className="decision-head">
                <strong>QVeris 审计</strong>
              </div>
              <div className="audit-grid">
                <div className="analysis-note-card">
                  <strong>已启用能力</strong>
                  {enabledCapabilities.length ? (
                    <div className="candidate-tag-row">
                      {enabledCapabilities.map((item) => (
                        <span key={item} className="candidate-tag">{fmtCapability(item)}</span>
                      ))}
                    </div>
                  ) : (
                    <p>本次扫描没有启用 QVeris 能力。</p>
                  )}
                </div>
                <div className="analysis-note-card">
                  <strong>已跳过能力</strong>
                  {skippedCapabilities.length ? (
                    <div className="candidate-tag-row">
                      {skippedCapabilities.map((item) => (
                        <span key={item} className="candidate-tag">{fmtCapability(item)}</span>
                      ))}
                    </div>
                  ) : (
                    <p>本次扫描没有额外跳过记录。</p>
                  )}
                </div>
                <div className="analysis-note-card">
                  <strong>预算策略摘要</strong>
                  {budgetEntries.length ? (
                    <div className="policy-grid">
                      {budgetEntries.map(({ key, policy }) => (
                        <div key={key} className="policy-card">
                          <h5>{fmtCapability(key)}</h5>
                          <div className="policy-row"><span>启用</span><strong>{policy.enabled ? "是" : "否"}</strong></div>
                          <div className="policy-row"><span>批量优先</span><strong>{policy.batch_only ? "是" : "否"}</strong></div>
                          <div className="policy-row"><span>最大调用次数</span><strong>{String(policy.max_calls_per_scan ?? "--")}</strong></div>
                          <div className="policy-row"><span>单次最大代码数</span><strong>{String(policy.max_codes_per_call ?? "--")}</strong></div>
                          <div className="policy-row"><span>本次扫描代码上限</span><strong>{String(policy.max_total_codes_per_scan ?? "--")}</strong></div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p>当前没有额外的 QVeris 预算策略记录。</p>
                  )}
                </div>
              </div>
              {skipReasons.length ? <p className="card-note text-multiline">跳过原因：{skipReasons.join(" / ")}</p> : null}
            </article>

            <article className="overnight-highlight-card">
              <div className="decision-head">
                <strong>真实次日回填</strong>
                <span className="candidate-tag">{fmtValidation(scan.validation_status)}</span>
              </div>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row"><span>验证状态</span><strong>{fmtValidation(scan.validation_status)}</strong></div>
                <div className="analysis-estimate-row"><span>已验证正式推荐</span><strong>{scan.validated_formal_count}</strong></div>
                <div className="analysis-estimate-row"><span>平均次日开盘收益</span><strong>{fmtPct(scan.avg_next_open_return)}</strong></div>
                <div className="analysis-estimate-row">
                  <span>最佳候选</span>
                  <strong className="notranslate text-multiline" translate="no" lang="en">
                    {scan.best_candidate ? `${scan.best_candidate.ticker} ${fmtPct(scan.best_candidate.next_open_return)}` : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>最差候选</span>
                  <strong className="notranslate text-multiline" translate="no" lang="en">
                    {scan.worst_candidate ? `${scan.worst_candidate.ticker} ${fmtPct(scan.worst_candidate.next_open_return)}` : "--"}
                  </strong>
                </div>
              </div>
            </article>
          </div>
        ) : (
          <div className="empty-state">请先发起一次隔夜扫描。</div>
        )}
      </section>

      <CandidateSection
        title="正式推荐"
        items={scan?.formal_recommendations ?? []}
        emptyText="当前没有正式推荐。"
        showActions
        busyKey={submittingTaskKey}
        scanId={scan?.scan_id}
        onAnalyze={(candidate) => void runDeepAnalysis(candidate)}
        onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)}
      />
      <CandidateSection
        title="Total Score 候选池"
        items={scan?.total_score_candidates ?? []}
        emptyText="当前没有完成总评分的候选。"
        busyKey={submittingTaskKey}
        scanId={scan?.scan_id}
        onAnalyze={(candidate) => void runDeepAnalysis(candidate)}
        onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)}
      />
      <CandidateSection
        title="被淘汰原因"
        items={scan?.rejected_candidates ?? []}
        emptyText="当前没有已淘汰候选。"
        showRejectedReason
        busyKey={submittingTaskKey}
        scanId={scan?.scan_id}
        onAnalyze={(candidate) => void runDeepAnalysis(candidate)}
        onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)}
      />
      <CandidateSection
        title="通过初步筛选的股票"
        items={scan?.preliminary_candidates ?? []}
        emptyText="当前没有通过初步筛选的股票。"
        busyKey={submittingTaskKey}
        scanId={scan?.scan_id}
        onAnalyze={(candidate) => void runDeepAnalysis(candidate)}
        onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)}
      />
      <CandidateSection
        title="观察名单"
        items={scan?.watchlist ?? []}
        emptyText="当前没有观察名单。"
        showActions
        busyKey={submittingTaskKey}
        scanId={scan?.scan_id}
        onAnalyze={(candidate) => void runDeepAnalysis(candidate)}
        onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)}
      />

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">历史验证</p>
            <h3>近 60 个交易日回看</h3>
          </div>
          <StatusBadge value={review?.status ?? null} />
        </div>
        <div className="scan-history-row">
          {loading ? (
            <div className="empty-state">正在加载历史验证记录...</div>
          ) : state.reviewList?.items.length ? (
            state.reviewList.items.slice(0, 6).map((item) => (
              <button
                key={item.review_id}
                type="button"
                className={review?.review_id === item.review_id ? "scan-chip scan-chip-active" : "scan-chip"}
                onClick={() =>
                  void Promise.all([
                    dashboardApi.getOvernightReview(item.review_id),
                    dashboardApi.getOvernightReviewArtifacts(item.review_id)
                  ]).then(([selectedReview, selectedReviewArtifacts]) =>
                    setState((current) => ({ ...current, selectedReview, selectedReviewArtifacts }))
                  )
                }
              >
                <strong>{item.end_trade_date}</strong>
                <span>{item.summary_snapshot?.days_with_formal_picks ?? 0} 个有推荐交易日</span>
              </button>
            ))
          ) : (
            <div className="empty-state">还没有历史验证记录。</div>
          )}
        </div>

        {review && reviewSummary ? (
          <div className="overnight-review-layout">
            <div className="overnight-home-grid">
              <article className="overnight-highlight-card">
                <div className="decision-head">
                  <strong>验证摘要</strong>
                </div>
                <div className="analysis-estimate-list">
                  <div className="analysis-estimate-row"><span>截止日期</span><strong>{review.end_trade_date}</strong></div>
                  <div className="analysis-estimate-row"><span>窗口</span><strong>{review.window_days} 交易日</strong></div>
                  <div className="analysis-estimate-row"><span>有推荐交易日</span><strong>{reviewSummary.days_with_formal_picks}</strong></div>
                  <div className="analysis-estimate-row"><span>平均次日开盘收益</span><strong>{fmtPct(reviewSummary.avg_next_open_return)}</strong></div>
                  <div className="analysis-estimate-row"><span>平均超额收益</span><strong>{fmtPct(reviewSummary.avg_excess_return)}</strong></div>
                  <div className="analysis-estimate-row">
                    <span>正收益命中率</span>
                    <strong>
                      {typeof reviewSummary.positive_pick_rate === "number"
                        ? `${(reviewSummary.positive_pick_rate * 100).toFixed(1)}%`
                        : "--"}
                    </strong>
                  </div>
                </div>
              </article>
              <article className="overnight-highlight-card">
                <div className="decision-head">
                  <strong>可重复性说明</strong>
                </div>
                <div className="analysis-estimate-list">
                  <div className="analysis-estimate-row"><span>配置版本</span><strong>{review.evaluation_config_version ?? "--"}</strong></div>
                  <div className="analysis-estimate-row">
                    <span>配置哈希</span>
                    <strong className="notranslate" translate="no" lang="en">{review.evaluation_config_hash ?? "--"}</strong>
                  </div>
                  <div className="analysis-estimate-row"><span>快照日期</span><strong>{review.universe_snapshot_date ?? "--"}</strong></div>
                  <div className="analysis-estimate-row"><span>存活偏差</span><strong>{review.survivorship_bias ? "是" : "否"}</strong></div>
                </div>
                <p className="card-note">{routeText(review.provider_route)}</p>
              </article>
            </div>

            <div className="structured-summary-grid">
              <BreakdownTable title="按市场 Regime" items={review.regime_breakdown ?? []} emptyText="当前没有市场 Regime 分层结果。" />
              <BreakdownTable title="按板块池" items={review.pool_breakdown ?? []} emptyText="当前没有板块池分层结果。" />
              <BreakdownTable title="按尾盘质量" items={review.tail_quality_breakdown ?? []} emptyText="当前没有尾盘质量分层结果。" />
            </div>

            <article className="analysis-note-card">
              <strong>最近几日表现</strong>
              {recentDailyResults.length ? (
                <div className="breakdown-table">
                  <table>
                    <thead>
                      <tr>
                        <th>交易日</th>
                        <th>正式推荐</th>
                        <th>候选代码</th>
                        <th>等权次日开盘收益</th>
                        <th>沪深 300 次日开盘收益</th>
                        <th>平均超额收益</th>
                      </tr>
                    </thead>
                    <tbody>
                      {recentDailyResults.map((item) => (
                        <tr key={item.trade_date}>
                          <td>{item.trade_date}</td>
                          <td>{item.formal_count}</td>
                          <td className="notranslate" translate="no" lang="en">
                            {item.formal_tickers?.length ? item.formal_tickers.join(", ") : "--"}
                          </td>
                          <td>{fmtPct(item.equal_weight_next_open_return)}</td>
                          <td>{fmtPct(item.benchmark_next_open_return)}</td>
                          <td>{fmtPct(item.avg_excess_return)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="empty-state compact-empty">当前没有可展示的最近几日表现。</div>
              )}
            </article>
          </div>
        ) : (
          <div className="empty-state">请先发起一次历史验证。</div>
        )}
      </section>
    </div>
  );
}
