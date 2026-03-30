import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { dashboardApi } from "../api";
import { OvernightExecutionStatus } from "../components/OvernightExecutionStatus";
import { StatusBadge } from "../components/StatusBadge";
import { getOvernightModeLabel, getOvernightQualityLabel } from "../labels";
import { getOvernightExecutionState } from "../overnightProgress";
import type {
  CreateTaskRequest,
  OvernightCandidate,
  OvernightMode,
  OvernightQuality,
  OvernightScanDetail,
  OvernightScanListResponse,
  OvernightScanSummary,
  OvernightStratifiedBreakdownBucket,
  OvernightTrackedTrade,
  OvernightTrackedTradeListResponse,
  OvernightTrackedTradeSourceBucket,
  TaskOptionsResponse
} from "../types";

type ScanModeSelection = OvernightMode | "auto";

const DEFAULT_MODE: ScanModeSelection = "auto";

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
  missing_history: "缺少足够历史日线，无法完成总评分",
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
  partial: "部分尾盘",
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
  trackedTradeList: OvernightTrackedTradeListResponse | null;
  selectedTrackedTrade: OvernightTrackedTrade | null;
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
const TRACKED_TRADE_STATUS_LABELS: Record<OvernightTrackedTrade["status"], string> = {
  pending_entry: "待买入",
  pending_exit: "待卖出",
  validated: "已验证",
  unavailable: "价格缺失"
};
const TRACKED_TRADE_SOURCE_LABELS: Record<OvernightTrackedTrade["source_bucket"], string> = {
  formal: "正式推荐",
  watchlist: "观察名单",
  total_score: "Total Score 候选"
};

function resolveScanModeSelection(
  value: ScanModeSelection,
  now: Date = new Date()
): OvernightMode {
  if (value !== "auto") {
    return value;
  }
  return now.getHours() < 15 ? "intraday_preview" : "strict";
}

function scanModeHelperText(value: ScanModeSelection, now: Date = new Date()) {
  if (value !== "auto") {
    return `当前将按${getOvernightModeLabel(value)}执行。`;
  }
  const resolved = resolveScanModeSelection(value, now);
  return `当前按本地时间将自动使用${getOvernightModeLabel(resolved)}，15:00 前默认盘中预估，15:00 后默认收盘确认。`;
}

const asRecord = (value: unknown) => value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
const asStrings = (value: unknown) => Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];

const routeText = (value?: Record<string, unknown> | null) => {
  if (!value || !Object.keys(value).length) return "当前没有额外的数据来源说明。";
  return Object.entries(value).map(([key, item]) => `${key}: ${String(item)}`).join(" / ");
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

function pickScanId(items: OvernightScanSummary[], preferredScanId?: string | null) {
  if (preferredScanId && items.some((item) => item.scan_id === preferredScanId)) {
    return preferredScanId;
  }
  return items[0]?.scan_id ?? null;
}

function pickTrackedTradeId(items: OvernightTrackedTrade[], preferredTradeId?: string | null) {
  if (preferredTradeId && items.some((item) => item.trade_id === preferredTradeId)) {
    return preferredTradeId;
  }
  return items[0]?.trade_id ?? null;
}

function shouldRefreshTrackedTrade(item: OvernightTrackedTrade) {
  return item.status === "pending_entry" || item.status === "pending_exit" || item.status === "unavailable";
}

function useHub() {
  const [state, setState] = useState<HubState>({
    options: null,
    scanList: null,
    selectedScan: null,
    trackedTradeList: null,
    selectedTrackedTrade: null
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [options, scanList, initialTrackedTradeList] = await Promise.all([
          dashboardApi.getOptions(),
          dashboardApi.getOvernightScans(),
          dashboardApi.getOvernightTrackedTrades()
        ]);
        const trackedTradeList = initialTrackedTradeList.items.some((item) => shouldRefreshTrackedTrade(item))
          ? await dashboardApi.refreshPendingOvernightTrackedTrades()
          : initialTrackedTradeList;
        const latestScanId = scanList.items[0]?.scan_id;
        const latestTradeId = trackedTradeList.items[0]?.trade_id;
        const [selectedScan, selectedTrackedTrade] = await Promise.all([
          latestScanId ? dashboardApi.getOvernightScan(latestScanId) : Promise.resolve(null),
          latestTradeId ? dashboardApi.getOvernightTrackedTrade(latestTradeId) : Promise.resolve(null)
        ]);
        if (!cancelled) {
          setState({ options, scanList, selectedScan, trackedTradeList, selectedTrackedTrade });
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
                <th>平均策略收益</th>
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
      ) : <div className="empty-state compact-empty">{emptyText}</div>}
    </article>
  );
}

function CandidateSection({
  title,
  items,
  emptyText,
  showRejectedReason,
  showAnalyzeAction,
  showTrackAction,
  busyKey,
  trackBusyKey,
  scanId,
  existingTradeForDate,
  onAnalyze,
  onTrackTrade,
  onOpenTask
}: {
  title: string;
  items: OvernightCandidate[];
  emptyText: string;
  showRejectedReason?: boolean;
  showAnalyzeAction?: boolean;
  showTrackAction?: boolean;
  busyKey: string;
  trackBusyKey: string;
  scanId?: string;
  existingTradeForDate?: OvernightTrackedTrade | null;
  onAnalyze: (candidate: OvernightCandidate) => void;
  onTrackTrade: (candidate: OvernightCandidate) => void;
  onOpenTask: (taskId: string) => void;
}) {
  return (
    <section className="panel panel-wide">
      <div className="panel-header"><div><p className="eyebrow">{title}</p><h3>{title}</h3></div></div>
      {items.length ? (
        <div className="breakdown-table">
          <table>
            <thead>
              <tr>
                <th>股票代码</th><th>名称</th><th>板块</th><th>Quick Score</th><th>Total Score</th><th>尾盘质量</th><th>{showRejectedReason ? "淘汰原因" : "当前去向"}</th><th>因子拆解</th><th>操作</th>
              </tr>
            </thead>
            <tbody>
              {items.map((candidate) => (
                <tr key={`${title}:${candidate.ticker}`}>
                  <td className="notranslate" translate="no" lang="en">{candidate.ticker}</td>
                  <td>{candidate.name}</td>
                  <td>{candidate.pool}</td>
                  <td>{fmtScore(candidate.quick_score)}</td>
                  <td>{fmtScore(candidate.total_score)}</td>
                  <td>{fmtQuality(candidate.quality)}</td>
                  <td className="text-multiline">{showRejectedReason ? fmtRejectedReason(candidate.rejected_reason) : fmtSelection(candidate.selection_stage)}</td>
                  <td className="text-multiline">{factorText(candidate.factor_breakdown)}</td>
                  <td>
                    <div style={{ display: "grid", gap: 6, justifyItems: "start" }}>
                      {showAnalyzeAction ? (
                        candidate.linked_task_id ? (
                          <button type="button" className="table-link-button" onClick={() => onOpenTask(candidate.linked_task_id!)}>查看分析</button>
                        ) : (
                          <button type="button" className="table-link-button" disabled={busyKey === `${scanId}:${candidate.ticker}`} onClick={() => onAnalyze(candidate)}>
                            {busyKey === `${scanId}:${candidate.ticker}` ? "创建中..." : "深度分析"}
                          </button>
                        )
                      ) : (
                        <span>{candidate.linked_task_id ? "已创建" : "--"}</span>
                      )}
                      {showTrackAction ? (
                        existingTradeForDate ? (
                          <button type="button" className="table-link-button" disabled>
                            {existingTradeForDate.ticker === candidate.ticker ? "已记录" : "当日已记录"}
                          </button>
                        ) : (
                          <button type="button" className="table-link-button" disabled={trackBusyKey === `${scanId}:${candidate.ticker}`} onClick={() => onTrackTrade(candidate)}>
                            {trackBusyKey === `${scanId}:${candidate.ticker}` ? "记录中..." : "记录今日买入"}
                          </button>
                        )
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : <div className="empty-state">{emptyText}</div>}
    </section>
  );
}

function HistoryChip({ title, status, statusLabel, stepLabel, detailLabel, active, deleting, onSelect, onDelete }: { title: string; status: string | null; statusLabel?: string; stepLabel: string; detailLabel: string; active: boolean; deleting: boolean; onSelect: () => void; onDelete: () => void; }) {
  return (
    <article className={active ? "scan-chip scan-chip-active" : "scan-chip"}>
      <button type="button" className="scan-chip-main" disabled={deleting} onClick={onSelect}>
        <strong>{title}</strong>
        <div className="scan-chip-status">{statusLabel ? <span className="candidate-tag">{statusLabel}</span> : <StatusBadge value={status} />}<span className="scan-chip-step">{stepLabel}</span></div>
        <span className="scan-chip-progress">{detailLabel}</span>
      </button>
      <button type="button" className="scan-chip-delete" disabled={deleting} onClick={onDelete}>
        {deleting ? "删除中..." : "删除"}
      </button>
    </article>
  );
}

export function OvernightPage() {
  const navigate = useNavigate();
  const { state, setState, error, setError, loading } = useHub();
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [mode, setMode] = useState<ScanModeSelection>(DEFAULT_MODE);
  const [creatingScan, setCreatingScan] = useState(false);
  const [validatingScan, setValidatingScan] = useState(false);
  const [submittingTaskKey, setSubmittingTaskKey] = useState("");
  const [submittingTradeKey, setSubmittingTradeKey] = useState("");
  const [refreshingTrades, setRefreshingTrades] = useState(false);
  const [deletingScanId, setDeletingScanId] = useState("");
  const [deletingTradeId, setDeletingTradeId] = useState("");

  const shouldPollScan = (state.selectedScan?.status === "queued" || state.selectedScan?.status === "running") && state.selectedScan?.scan_id !== deletingScanId;

  useEffect(() => {
    if (!shouldPollScan) return;
    let cancelled = false;
    const refresh = async () => {
      try {
        const scanList = await dashboardApi.getOvernightScans();
        const selectedScan = state.selectedScan?.scan_id
          ? await dashboardApi.getOvernightScan(state.selectedScan.scan_id)
          : null;
        if (!cancelled) {
          setState((current) => ({ ...current, scanList, selectedScan }));
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "隔夜推荐轮询失败。");
      }
    };
    const timer = window.setInterval(() => void refresh(), 3000);
    void refresh();
    return () => { cancelled = true; window.clearInterval(timer); };
  }, [setError, setState, shouldPollScan, state.selectedScan?.scan_id]);

  const scan = state.selectedScan;
  const trackedTradeList = state.trackedTradeList;
  const trackedTrade = state.selectedTrackedTrade;
  const trackedTradeStats = trackedTradeList?.stats ?? null;
  const recentTrackedTrades = trackedTradeList?.items.slice(0, 8) ?? [];
  const currentTradeForScanDate = scan
    ? trackedTradeList?.items.find((item) => item.trade_date === scan.trade_date) ?? null
    : null;
  const enabledCapabilities = asStrings(scan?.audit?.qveris_enabled_capabilities);
  const skippedCapabilities = asStrings(scan?.audit?.qveris_skipped_capabilities);
  const skipReasons = asStrings(scan?.audit?.qveris_skip_reasons);
  const budgetEntries = Object.entries(asRecord(scan?.audit?.qveris_budget_policy)).map(([key, value]) => ({ key, policy: asRecord(value) }));
  const selectedMode = resolveScanModeSelection(mode);

  const selectScan = async (scanId: string) => {
    try {
      const selectedScan = await dashboardApi.getOvernightScan(scanId);
      setState((current) => ({ ...current, selectedScan }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "隔夜扫描详情加载失败。");
    }
  };

  const selectTrackedTrade = async (tradeId: string) => {
    try {
      const selectedTrackedTrade = await dashboardApi.getOvernightTrackedTrade(tradeId);
      setState((current) => ({ ...current, selectedTrackedTrade }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "每日跟踪详情加载失败。");
    }
  };

  const createScan = async () => {
    setCreatingScan(true);
    try {
      const detail = await dashboardApi.createOvernightScan({ trade_date: tradeDate, market_region: "cn_a", mode: selectedMode });
      const scanList = await dashboardApi.getOvernightScans();
      setState((current) => ({ ...current, scanList, selectedScan: detail }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "隔夜扫描创建失败。");
    } finally {
      setCreatingScan(false);
    }
  };

  const refreshTrackedTrades = async (preferredTradeId?: string | null) => {
    setRefreshingTrades(true);
    try {
      const trackedTradeList = await dashboardApi.refreshPendingOvernightTrackedTrades();
      const nextTradeId = pickTrackedTradeId(trackedTradeList.items, preferredTradeId ?? state.selectedTrackedTrade?.trade_id);
      const selectedTrackedTrade = nextTradeId
        ? trackedTradeList.items.find((item) => item.trade_id === nextTradeId) ?? null
        : null;
      setState((current) => ({ ...current, trackedTradeList, selectedTrackedTrade }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "待验证记录刷新失败。");
    } finally {
      setRefreshingTrades(false);
    }
  };

  const validateScan = async () => {
    if (!scan) return;
    setValidatingScan(true);
    try {
      const [selectedScan, scanList] = await Promise.all([dashboardApi.validateOvernightScan(scan.scan_id), dashboardApi.getOvernightScans()]);
      setState((current) => ({ ...current, selectedScan, scanList }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "次日结果回填失败。");
    } finally {
      setValidatingScan(false);
    }
  };

  const deleteScan = async (item: OvernightScanSummary) => {
    if (deletingScanId) return;
    const confirmMessage = item.status === "queued" || item.status === "running"
      ? "确认删除这个隔夜扫描吗？运行中的扫描会先被终止，扫描记录和产物也会一起删除。"
      : "确认删除这个隔夜扫描吗？扫描记录和产物也会一起删除。";
    if (!window.confirm(confirmMessage)) return;

    const currentSelectedScanId = state.selectedScan?.scan_id;
    setDeletingScanId(item.scan_id);
    try {
      await dashboardApi.deleteOvernightScan(item.scan_id);
      const scanList = await dashboardApi.getOvernightScans();
      const nextSelectedScanId = pickScanId(
        scanList.items,
        currentSelectedScanId === item.scan_id ? null : currentSelectedScanId
      );
      const selectedScan = nextSelectedScanId
        ? await dashboardApi.getOvernightScan(nextSelectedScanId)
        : null;
      setState((current) => ({ ...current, scanList, selectedScan }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "隔夜扫描删除失败。");
    } finally {
      setDeletingScanId("");
    }
  };

  const createTrackedTrade = async (candidate: OvernightCandidate, sourceBucket: OvernightTrackedTradeSourceBucket) => {
    if (!scan) return;
    const key = `${scan.scan_id}:${candidate.ticker}`;
    setSubmittingTradeKey(key);
    try {
      const created = await dashboardApi.createOvernightTrackedTrade({
        trade_date: scan.trade_date,
        market_region: "cn_a",
        scan_id: scan.scan_id,
        scan_mode: scan.mode,
        source_bucket: sourceBucket,
        candidate: {
          ticker: candidate.ticker,
          name: candidate.name,
          pool: candidate.pool,
          quality: candidate.quality,
          quick_score: candidate.quick_score,
          total_score: candidate.total_score,
          factor_breakdown: candidate.factor_breakdown,
          tail_metrics: candidate.tail_metrics ?? null
        }
      });
      await refreshTrackedTrades(created.trade_id);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "每日跟踪记录创建失败。");
    } finally {
      setSubmittingTradeKey("");
    }
  };

  const deleteTrackedTrade = async (item: OvernightTrackedTrade) => {
    if (deletingTradeId) return;
    const confirmMessage = "确认删除这条每日跟踪记录吗？删除后可重新选择当天买入标的。";
    if (!window.confirm(confirmMessage)) return;

    const currentSelectedTradeId = state.selectedTrackedTrade?.trade_id;
    setDeletingTradeId(item.trade_id);
    try {
      await dashboardApi.deleteOvernightTrackedTrade(item.trade_id);
      const trackedTradeList = await dashboardApi.getOvernightTrackedTrades();
      const nextSelectedTradeId = pickTrackedTradeId(
        trackedTradeList.items,
        currentSelectedTradeId === item.trade_id ? null : currentSelectedTradeId
      );
      const selectedTrackedTrade = nextSelectedTradeId
        ? trackedTradeList.items.find((tradeItem) => tradeItem.trade_id === nextSelectedTradeId) ?? null
        : null;
      setState((current) => ({ ...current, trackedTradeList, selectedTrackedTrade }));
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "每日跟踪记录删除失败。");
    } finally {
      setDeletingTradeId("");
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
      const task = await dashboardApi.createTask(buildTaskPayload(candidate, scan.trade_date, state.options), { type: "overnight_scan", scanId: scan.scan_id, tradeDate: scan.trade_date, mode: scan.mode, name: candidate.name });
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
          <p className="eyebrow">A 股隔夜研究工作台</p>
          <h2>盘中预估、收盘确认与每日隔夜跟踪</h2>
          <p className="hero-copy">这里集中展示正式推荐、观察名单、淘汰原因，以及从当前功能上线后逐日累计的隔夜跟踪记录。你可以手动确认当天 14:55 的买入标的，系统随后按次日 10:00 自动补齐收益。</p>
        </div>
        <div className="hero-actions">
          <button type="button" className="primary-button" disabled={creatingScan} onClick={() => void createScan()}>{creatingScan ? "正在创建扫描..." : "发起隔夜扫描"}</button>
          <button type="button" className="secondary-button" disabled={refreshingTrades} onClick={() => void refreshTrackedTrades()}>{refreshingTrades ? "正在刷新..." : "刷新待验证记录"}</button>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel panel-wide">
        <div className="panel-header"><div><p className="eyebrow">执行入口</p><h3>扫描与回填</h3></div></div>
        <div className="analysis-main-grid">
          <div className="analysis-block">
            <div className="analysis-form-grid">
              <label className="analysis-field"><span>扫描日期</span><input type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></label>
              <label className="analysis-field"><span>扫描模式</span><select value={mode} onChange={(event) => setMode(event.target.value as ScanModeSelection)}><option value="auto">自动</option><option value="intraday_preview">盘中预估</option><option value="strict">收盘确认</option></select></label>
            </div>
            <p className="card-note">{scanModeHelperText(mode)}</p>
            <div className="hero-actions">
              <button type="button" className="primary-button" disabled={creatingScan} onClick={() => void createScan()}>{creatingScan ? "正在创建扫描..." : "启动扫描"}</button>
              <button type="button" className="secondary-button" disabled={!scan || scan.status !== "succeeded" || validatingScan} onClick={() => void validateScan()}>{validatingScan ? "正在回填..." : "回填次日结果"}</button>
            </div>
          </div>
          <div className="analysis-note-card panel-subtle"><strong>QVeris 使用原则</strong><p>实时补充能力遵循批量优先与额度受限策略，无法满足条件时会明确跳过，不会盲目扩大扫描范围。</p></div>
        </div>
      </section>

      <section className="panel panel-wide">
        <div className="panel-header"><div><p className="eyebrow">扫描记录</p><h3>最近隔夜扫描</h3></div></div>
        <div className="scan-history-row">
          {loading ? <div className="empty-state">正在加载扫描记录...</div> : state.scanList?.items.length ? state.scanList.items.slice(0, 8).map((item) => {
            const itemProgress = getOvernightExecutionState("scan", item.status, item.progress_message);
            return <HistoryChip key={item.scan_id} title={item.trade_date} status={item.status} stepLabel={itemProgress.currentStepLabel} detailLabel={item.status === "succeeded" ? `${item.formal_count} 只正式推荐` : itemProgress.detailLabel} active={scan?.scan_id === item.scan_id} deleting={deletingScanId === item.scan_id} onSelect={() => void selectScan(item.scan_id)} onDelete={() => void deleteScan(item)} />;
          }) : <div className="empty-state">还没有隔夜扫描记录。</div>}
        </div>

        {scan ? <div className="overnight-home-grid">
          <article className="overnight-highlight-card overnight-highlight-card-wide"><OvernightExecutionStatus title="扫描执行状态" kind="scan" status={scan.status} progressMessage={scan.progress_message} startedAt={scan.started_at} finishedAt={scan.finished_at} errorMessage={scan.error_message} /></article>
          <article className="overnight-highlight-card"><div className="decision-head"><strong>本次扫描摘要</strong></div><div className="analysis-estimate-list"><div className="analysis-estimate-row"><span>扫描日期</span><strong>{scan.trade_date}</strong></div><div className="analysis-estimate-row"><span>执行模式</span><strong>{fmtMode(scan.mode)}</strong></div><div className="analysis-estimate-row"><span>正式推荐</span><strong>{scan.formal_count}</strong></div><div className="analysis-estimate-row"><span>Total Score 候选</span><strong>{scan.scored_count}</strong></div><div className="analysis-estimate-row"><span>已淘汰</span><strong>{scan.rejected_count}</strong></div><div className="analysis-estimate-row"><span>配置哈希</span><strong className="notranslate" translate="no" lang="en">{scan.evaluation_config_hash ?? "--"}</strong></div></div><p className="card-note">{scan.mode === "intraday_preview" ? "盘中预估结果仅代表盘中可用判断；若要记录今日买入，可直接从正式推荐、观察名单或 Total Score 候选中手动确认。" : "当前结果为收盘确认口径，可用于次日回填对照，也可手动沉淀为每日隔夜跟踪记录。"}</p></article>
          <article className="overnight-highlight-card"><div className="decision-head"><strong>数据质量与来源</strong></div><div className="analysis-estimate-list"><div className="analysis-estimate-row"><span>市场提示</span><strong className="text-multiline">{scan.market_message}</strong></div><div className="analysis-estimate-row"><span>数据质量</span><strong>{String(scan.data_quality?.status ?? "--")}</strong></div><div className="analysis-estimate-row"><span>快照日期</span><strong>{scan.universe_snapshot_date ?? "--"}</strong></div><div className="analysis-estimate-row"><span>Provider Route</span><strong className="text-multiline">{routeText(scan.provider_route)}</strong></div></div><p className="card-note">{scan.bias_flags.length ? `偏差标记：${scan.bias_flags.join(" / ")}` : "当前没有额外偏差标记。"}{scan.mode === "intraday_preview" ? " 尾盘质量中的“部分/代理”代表盘中预估，不等同于收盘后完整确认。" : ""}</p></article>
          <article className="overnight-highlight-card overnight-highlight-card-wide"><div className="decision-head"><strong>QVeris 审计</strong></div><div className="audit-grid"><div className="analysis-note-card"><strong>已启用能力</strong>{enabledCapabilities.length ? <div className="candidate-tag-row">{enabledCapabilities.map((item) => <span key={item} className="candidate-tag">{fmtCapability(item)}</span>)}</div> : <p>本次扫描没有启用 QVeris 能力。</p>}</div><div className="analysis-note-card"><strong>已跳过能力</strong>{skippedCapabilities.length ? <div className="candidate-tag-row">{skippedCapabilities.map((item) => <span key={item} className="candidate-tag">{fmtCapability(item)}</span>)}</div> : <p>本次扫描没有额外跳过记录。</p>}</div><div className="analysis-note-card"><strong>预算策略摘要</strong>{budgetEntries.length ? <div className="policy-grid">{budgetEntries.map(({ key, policy }) => <div key={key} className="policy-card"><h5>{fmtCapability(key)}</h5><div className="policy-row"><span>启用</span><strong>{policy.enabled ? "是" : "否"}</strong></div><div className="policy-row"><span>批量优先</span><strong>{policy.batch_only ? "是" : "否"}</strong></div><div className="policy-row"><span>最大调用次数</span><strong>{String(policy.max_calls_per_scan ?? "--")}</strong></div><div className="policy-row"><span>单次最大代码数</span><strong>{String(policy.max_codes_per_call ?? "--")}</strong></div><div className="policy-row"><span>本次扫描代码上限</span><strong>{String(policy.max_total_codes_per_scan ?? "--")}</strong></div></div>)}</div> : <p>当前没有额外的 QVeris 预算策略记录。</p>}</div></div>{skipReasons.length ? <p className="card-note text-multiline">跳过原因：{skipReasons.join(" / ")}</p> : null}</article>
          <article className="overnight-highlight-card"><div className="decision-head"><strong>真实次日回填</strong><span className="candidate-tag">{fmtValidation(scan.validation_status)}</span></div><div className="analysis-estimate-list"><div className="analysis-estimate-row"><span>验证状态</span><strong>{fmtValidation(scan.validation_status)}</strong></div><div className="analysis-estimate-row"><span>已验证正式推荐</span><strong>{scan.validated_formal_count}</strong></div><div className="analysis-estimate-row"><span>平均次日开盘收益</span><strong>{fmtPct(scan.avg_next_open_return)}</strong></div><div className="analysis-estimate-row"><span>最佳候选</span><strong className="notranslate text-multiline" translate="no" lang="en">{scan.best_candidate ? `${scan.best_candidate.ticker} ${fmtPct(scan.best_candidate.next_open_return)}` : "--"}</strong></div><div className="analysis-estimate-row"><span>最差候选</span><strong className="notranslate text-multiline" translate="no" lang="en">{scan.worst_candidate ? `${scan.worst_candidate.ticker} ${fmtPct(scan.worst_candidate.next_open_return)}` : "--"}</strong></div></div></article>
        </div> : <div className="empty-state">请先发起一次隔夜扫描。</div>}
      </section>

      <CandidateSection title="正式推荐" items={scan?.formal_recommendations ?? []} emptyText="当前没有正式推荐。" showAnalyzeAction showTrackAction busyKey={submittingTaskKey} trackBusyKey={submittingTradeKey} scanId={scan?.scan_id} existingTradeForDate={currentTradeForScanDate} onAnalyze={(candidate) => void runDeepAnalysis(candidate)} onTrackTrade={(candidate) => void createTrackedTrade(candidate, "formal")} onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)} />
      <CandidateSection title="Total Score 候选池" items={scan?.total_score_candidates ?? []} emptyText="当前没有完成总评分的候选。" showTrackAction busyKey={submittingTaskKey} trackBusyKey={submittingTradeKey} scanId={scan?.scan_id} existingTradeForDate={currentTradeForScanDate} onAnalyze={(candidate) => void runDeepAnalysis(candidate)} onTrackTrade={(candidate) => void createTrackedTrade(candidate, "total_score")} onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)} />
      <CandidateSection title="淘汰原因" items={scan?.rejected_candidates ?? []} emptyText="当前没有已淘汰候选。" showRejectedReason busyKey={submittingTaskKey} trackBusyKey={submittingTradeKey} scanId={scan?.scan_id} existingTradeForDate={currentTradeForScanDate} onAnalyze={(candidate) => void runDeepAnalysis(candidate)} onTrackTrade={(candidate) => void createTrackedTrade(candidate, "formal")} onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)} />
      <CandidateSection title="通过初筛的股票" items={scan?.preliminary_candidates ?? []} emptyText="当前没有通过初筛的股票。" busyKey={submittingTaskKey} trackBusyKey={submittingTradeKey} scanId={scan?.scan_id} existingTradeForDate={currentTradeForScanDate} onAnalyze={(candidate) => void runDeepAnalysis(candidate)} onTrackTrade={(candidate) => void createTrackedTrade(candidate, "formal")} onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)} />
      <CandidateSection title="观察名单" items={scan?.watchlist ?? []} emptyText="当前没有观察名单。" showAnalyzeAction showTrackAction busyKey={submittingTaskKey} trackBusyKey={submittingTradeKey} scanId={scan?.scan_id} existingTradeForDate={currentTradeForScanDate} onAnalyze={(candidate) => void runDeepAnalysis(candidate)} onTrackTrade={(candidate) => void createTrackedTrade(candidate, "watchlist")} onOpenTask={(taskId) => navigate(`/tasks/${taskId}`)} />

      <section className="panel panel-wide">
        <div className="panel-header"><div><p className="eyebrow">每日跟踪</p><h3>从现在开始累计的真实隔夜记录</h3></div></div>
        <div className="scan-history-row">
          {loading ? <div className="empty-state">正在加载每日跟踪记录...</div> : recentTrackedTrades.length ? recentTrackedTrades.map((item) => (
            <HistoryChip
              key={item.trade_id}
              title={item.trade_date}
              status={item.status}
              statusLabel={TRACKED_TRADE_STATUS_LABELS[item.status]}
              stepLabel={TRACKED_TRADE_STATUS_LABELS[item.status]}
              detailLabel={`${item.ticker} / ${TRACKED_TRADE_SOURCE_LABELS[item.source_bucket]}`}
              active={trackedTrade?.trade_id === item.trade_id}
              deleting={deletingTradeId === item.trade_id}
              onSelect={() => void selectTrackedTrade(item.trade_id)}
              onDelete={() => void deleteTrackedTrade(item)}
            />
          )) : <div className="empty-state">还没有每日跟踪记录。扫描完成后，可从正式推荐、观察名单或 Total Score 候选中手动记录当天买入标的。</div>}
        </div>

        <div className="overnight-home-grid">
          <article className="overnight-highlight-card">
            <div className="decision-head"><strong>跟踪摘要</strong></div>
            <div className="analysis-estimate-list">
              <div className="analysis-estimate-row"><span>记录天数</span><strong>{trackedTradeStats?.total_days ?? 0}</strong></div>
              <div className="analysis-estimate-row"><span>已验证天数</span><strong>{trackedTradeStats?.validated_days ?? 0}</strong></div>
              <div className="analysis-estimate-row"><span>待验证</span><strong>{trackedTradeStats?.pending_count ?? 0}</strong></div>
              <div className="analysis-estimate-row"><span>价格缺失</span><strong>{trackedTradeStats?.unavailable_count ?? 0}</strong></div>
              <div className="analysis-estimate-row"><span>平均收益</span><strong>{fmtPct(trackedTradeStats?.avg_return)}</strong></div>
              <div className="analysis-estimate-row"><span>胜率</span><strong>{typeof trackedTradeStats?.win_rate === "number" ? `${(trackedTradeStats.win_rate * 100).toFixed(1)}%` : "--"}</strong></div>
              <div className="analysis-estimate-row"><span>累计收益</span><strong>{fmtPct(trackedTradeStats?.cumulative_return)}</strong></div>
            </div>
            <p className="card-note">收益仅基于已验证记录计算，口径固定为当日 14:55 买入、次日 10:00 卖出；不回填历史 review。</p>
          </article>
          <article className="overnight-highlight-card">
            <div className="decision-head"><strong>当前选中记录</strong></div>
            {trackedTrade ? (
              <>
                <div className="analysis-estimate-list">
                  <div className="analysis-estimate-row"><span>交易日</span><strong>{trackedTrade.trade_date}</strong></div>
                  <div className="analysis-estimate-row"><span>来源</span><strong>{TRACKED_TRADE_SOURCE_LABELS[trackedTrade.source_bucket]}</strong></div>
                  <div className="analysis-estimate-row"><span>买入标的</span><strong className="notranslate text-multiline" translate="no" lang="en">{trackedTrade.ticker} / {trackedTrade.name}</strong></div>
                  <div className="analysis-estimate-row"><span>执行模式</span><strong>{fmtMode(trackedTrade.scan_mode)}</strong></div>
                  <div className="analysis-estimate-row"><span>尾盘质量</span><strong>{fmtQuality(trackedTrade.quality)}</strong></div>
                  <div className="analysis-estimate-row"><span>Total Score</span><strong>{fmtScore(trackedTrade.total_score)}</strong></div>
                  <div className="analysis-estimate-row"><span>状态</span><strong>{TRACKED_TRADE_STATUS_LABELS[trackedTrade.status]}</strong></div>
                  <div className="analysis-estimate-row"><span>买入</span><strong>{trackedTrade.entry_price != null ? `${trackedTrade.entry_price.toFixed(2)} (${trackedTrade.entry_time_used ?? trackedTrade.entry_target_time})` : `等待 ${trackedTrade.entry_target_time}`}</strong></div>
                  <div className="analysis-estimate-row"><span>卖出</span><strong>{trackedTrade.exit_price != null ? `${trackedTrade.exit_price.toFixed(2)} (${trackedTrade.exit_time_used ?? trackedTrade.exit_target_time})` : trackedTrade.exit_trade_date ? `${trackedTrade.exit_trade_date} ${trackedTrade.exit_target_time}` : `等待 ${trackedTrade.exit_target_time}`}</strong></div>
                  <div className="analysis-estimate-row"><span>收益</span><strong>{fmtPct(trackedTrade.strategy_return)}</strong></div>
                </div>
                <p className="card-note">{trackedTrade.last_error ? `最近错误：${trackedTrade.last_error}` : "当前没有额外错误信息。"} 因子拆解：{factorText(trackedTrade.factor_breakdown)}</p>
              </>
            ) : <div className="empty-state compact-empty">请选择一条每日跟踪记录查看详情。</div>}
          </article>
        </div>

        <article className="analysis-note-card">
          <strong>最近记录</strong>
          {recentTrackedTrades.length ? (
            <div className="breakdown-table">
              <table>
                <thead>
                  <tr>
                    <th>交易日</th>
                    <th>标的</th>
                    <th>来源</th>
                    <th>状态</th>
                    <th>买入</th>
                    <th>卖出</th>
                    <th>收益</th>
                  </tr>
                </thead>
                <tbody>
                  {recentTrackedTrades.map((item) => (
                    <tr key={item.trade_id}>
                      <td>{item.trade_date}</td>
                      <td className="notranslate" translate="no" lang="en">{item.ticker}</td>
                      <td>{TRACKED_TRADE_SOURCE_LABELS[item.source_bucket]}</td>
                      <td>{TRACKED_TRADE_STATUS_LABELS[item.status]}</td>
                      <td>{item.entry_price != null ? `${item.entry_price.toFixed(2)} (${item.entry_time_used ?? item.entry_target_time})` : item.entry_target_time}</td>
                      <td>{item.exit_price != null ? `${item.exit_price.toFixed(2)} (${item.exit_time_used ?? item.exit_target_time})` : item.exit_trade_date ? `${item.exit_trade_date} ${item.exit_target_time}` : "--"}</td>
                      <td>{fmtPct(item.strategy_return)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="empty-state compact-empty">当前还没有可展示的每日跟踪明细。</div>}
        </article>
      </section>
    </div>
  );
}
