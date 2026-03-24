import { useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useNavigate, useParams } from "react-router-dom";

import { dashboardApi, resolveDownloadUrl } from "../api";
import { StatusBadge } from "../components/StatusBadge";
import {
  getMarketLabel,
  getOvernightModeLabel,
  getOvernightQualityLabel,
  getProgressMessageLabel,
  STAGE_LABELS
} from "../labels";
import type {
  EvidenceGroup,
  FactorBlock,
  FactorSignal,
  StructuredDecision,
  TaskArtifactsResponse,
  TaskDetail,
  TaskStage
} from "../types";

const REPORT_TABS = [
  { key: "final_trade_decision", label: "最终结论" },
  { key: "market_report", label: "市场分析" },
  { key: "sentiment_report", label: "情绪分析" },
  { key: "news_report", label: "新闻分析" },
  { key: "fundamentals_report", label: "基本面分析" },
  { key: "investment_plan", label: "研究总结" },
  { key: "trader_investment_plan", label: "交易计划" }
] as const;

type ReportTabKey = (typeof REPORT_TABS)[number]["key"];

const STAGE_SEQUENCE: TaskStage[] = [
  "initializing",
  "market",
  "social",
  "news",
  "fundamentals",
  "research",
  "trader",
  "risk",
  "completed"
];

const FACTOR_LABELS: Record<string, string> = {
  technical: "技术面",
  sentiment: "情绪面",
  news: "新闻 / 公告",
  fundamentals: "基本面",
  overnight_tail: "隔夜尾盘来源"
};

const SUBSCORE_LABELS: Record<string, string> = {
  trend_strength: "趋势强弱",
  momentum: "近期动量",
  volatility_state: "波动状态",
  volume_confirmation: "量价配合",
  attention_heat: "个股热度",
  holder_concentration: "筹码结构",
  sentiment_balance: "情绪平衡",
  company_events: "公司事件",
  broker_coverage: "研报覆盖",
  news_tone: "新闻语义",
  profitability: "盈利能力",
  growth: "成长性",
  balance_sheet: "资产负债表",
  valuation: "估值修正",
  tail_signal_strength: "尾盘强度",
  scan_alignment: "扫描一致性",
  tail_quality: "尾盘质量"
};

function isNonEmptyRecord(value: unknown): value is Record<string, unknown> {
  return (
    Boolean(value) &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value as Record<string, unknown>).length > 0
  );
}

function formatValue(value: unknown): string {
  if (typeof value === "number") {
    return Number.isInteger(value) ? `${value}` : value.toFixed(2);
  }
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.join(" / ");
  }
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([key, item]) => `${key}: ${formatValue(item)}`)
      .join(" / ");
  }
  return "--";
}

function normalizeEvidenceGroup(value: unknown): EvidenceGroup {
  if (!value || typeof value !== "object") {
    return { strengths: [], risks: [], raw_metrics: [] };
  }
  const group = value as Partial<EvidenceGroup>;
  return {
    strengths: Array.isArray(group.strengths) ? group.strengths : [],
    risks: Array.isArray(group.risks) ? group.risks : [],
    raw_metrics: Array.isArray(group.raw_metrics) ? group.raw_metrics : []
  };
}

function getBucketLabel(value: string | null | undefined): string {
  if (value === "formal") {
    return "正式推荐";
  }
  if (value === "watchlist") {
    return "观察名单";
  }
  return value ?? "--";
}

function getValidationLabel(value: string | null | undefined): string {
  switch (value) {
    case "validated":
      return "已验证";
    case "pending":
      return "待次日开盘";
    case "unavailable":
      return "数据缺失";
    case "watchlist_only":
      return "仅观察名单";
    case "empty":
      return "无正式推荐";
    default:
      return value ?? "--";
  }
}

function FactorSignalList({
  title,
  items,
  emptyText,
  tone
}: {
  title: string;
  items: FactorSignal[];
  emptyText: string;
  tone: "positive" | "negative" | "neutral";
}) {
  return (
    <div className={`signal-section signal-section-${tone}`}>
      <h5>{title}</h5>
      {items.length ? (
        <ul className="signal-list">
          {items.map((item, index) => (
            <li key={`${title}-${index}`}>
              <strong>{item.signal}</strong>
              <span>{formatValue(item.value)}</span>
            </li>
          ))}
        </ul>
      ) : (
        <div className="empty-state compact-empty">{emptyText}</div>
      )}
    </div>
  );
}

export function TaskDetailPage() {
  const { taskId = "" } = useParams();
  const navigate = useNavigate();
  const [detail, setDetail] = useState<TaskDetail | null>(null);
  const [artifacts, setArtifacts] = useState<TaskArtifactsResponse | null>(null);
  const [activeTab, setActiveTab] = useState<ReportTabKey>("final_trade_decision");
  const [error, setError] = useState("");
  const [isMutating, setIsMutating] = useState(false);

  useEffect(() => {
    if (!taskId) return;
    let cancelled = false;

    const load = async () => {
      try {
        const [nextDetail, nextArtifacts] = await Promise.all([
          dashboardApi.getTask(taskId),
          dashboardApi.getArtifacts(taskId)
        ]);
        if (!cancelled) {
          setDetail(nextDetail);
          setArtifacts(nextArtifacts);
          setError("");
        }
      } catch (nextError) {
        if (!cancelled) {
          setError(nextError instanceof Error ? nextError.message : "任务详情加载失败。");
        }
      }
    };

    void load();
    const timer = window.setInterval(() => {
      if (detail?.status === "succeeded" || detail?.status === "failed") {
        return;
      }
      void load();
    }, 2000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [detail?.status, taskId]);

  useEffect(() => {
    if (!artifacts) {
      return;
    }
    if (artifacts.reports[activeTab]) {
      return;
    }
    const firstAvailable = REPORT_TABS.find((tab) => artifacts.reports[tab.key]);
    if (firstAvailable && firstAvailable.key !== activeTab) {
      setActiveTab(firstAvailable.key);
    }
  }, [activeTab, artifacts]);

  const factorSnapshot = isNonEmptyRecord(artifacts?.structured?.factor_snapshot)
    ? (artifacts?.structured?.factor_snapshot as NonNullable<
        TaskArtifactsResponse["structured"]["factor_snapshot"]
      >)
    : undefined;
  const evidenceSnapshot = isNonEmptyRecord(artifacts?.structured?.evidence_snapshot)
    ? artifacts?.structured?.evidence_snapshot
    : {};
  const structuredDecision = isNonEmptyRecord(artifacts?.structured?.structured_decision)
    ? (artifacts?.structured?.structured_decision as StructuredDecision)
    : undefined;
  const factorBlocks = useMemo(
    () => factorSnapshot?.scores ?? ({} as Record<string, FactorBlock>),
    [factorSnapshot]
  );
  const activeReport = artifacts?.reports[activeTab] ?? "";
  const stageIndex = STAGE_SEQUENCE.indexOf(detail?.stage ?? "initializing");
  const overnightContext = detail?.overnight_context;
  const canTerminate = detail?.status === "queued" || detail?.status === "running";

  async function handleTerminateTask(): Promise<void> {
    if (!taskId || !canTerminate || isMutating) {
      return;
    }
    if (!window.confirm("确认终止当前任务吗？")) {
      return;
    }
    setIsMutating(true);
    try {
      const [nextDetail, nextArtifacts] = await Promise.all([
        dashboardApi.terminateTask(taskId),
        dashboardApi.getArtifacts(taskId)
      ]);
      setDetail(nextDetail);
      setArtifacts(nextArtifacts);
      setError("");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "终止任务失败。");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleDeleteTask(): Promise<void> {
    if (!taskId || isMutating) {
      return;
    }
    if (!window.confirm("确认删除这个任务吗？任务产物也会一起移除。")) {
      return;
    }
    setIsMutating(true);
    try {
      await dashboardApi.deleteTask(taskId);
      navigate("/tasks");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "删除任务失败。");
      setIsMutating(false);
    }
  }

  if (!taskId) {
    return <div className="error-banner">缺少任务编号。</div>;
  }

  return (
    <div className="page-grid">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="hero-card">
        <div>
          <p className="eyebrow">任务详情</p>
          <h2>
            <span className="notranslate" translate="no" lang="en">
              {detail?.ticker ?? "--"}
            </span>{" "}
            分析任务
          </h2>
          <p className="hero-copy">
            {getProgressMessageLabel(detail?.progress_message) || "正在同步任务状态。"}
          </p>
          {detail?.source_context?.type === "overnight_scan" ? (
            <div className="source-context-card">
              <strong>来自隔夜推荐</strong>
              <span>
                扫描日期 {detail.source_context.trade_date ?? "--"} /{" "}
                {detail.source_context.mode
                  ? getOvernightModeLabel(
                      detail.source_context.mode as "strict" | "research_fallback"
                    )
                  : "隔夜扫描"}
              </span>
            </div>
          ) : null}
        </div>

        <div className="hero-side">
          <div className="hero-metrics">
            <StatusBadge value={detail?.status ?? null} />
            <StatusBadge value={detail?.stage ?? null} variant="stage" />
            <StatusBadge value={detail?.decision ?? null} variant="decision" />
          </div>
          <div className="hero-actions">
            {canTerminate ? (
              <button
                className="secondary-button"
                type="button"
                onClick={() => {
                  void handleTerminateTask();
                }}
                disabled={isMutating}
              >
                {isMutating ? "处理中..." : "终止运行"}
              </button>
            ) : null}
            <button
              className="secondary-button danger-button"
              type="button"
              onClick={() => {
                void handleDeleteTask();
              }}
              disabled={isMutating}
            >
              {isMutating && !canTerminate ? "处理中..." : "删除任务"}
            </button>
          </div>
        </div>
      </section>

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">执行进度</p>
            <h3>工作流阶段</h3>
          </div>
          <div className="download-row">
            {detail?.download_urls.final_state_json ? (
              <a
                className="secondary-button"
                href={resolveDownloadUrl(detail.download_urls.final_state_json)}
                target="_blank"
                rel="noreferrer"
              >
                下载 JSON
              </a>
            ) : null}
            {detail?.download_urls.events_log ? (
              <a
                className="secondary-button"
                href={resolveDownloadUrl(detail.download_urls.events_log)}
                target="_blank"
                rel="noreferrer"
              >
                下载日志
              </a>
            ) : null}
          </div>
        </div>

        <div className="stage-strip">
          {STAGE_SEQUENCE.map((stage, index) => (
            <div
              key={stage}
              className={
                index <= stageIndex ? "stage-chip stage-chip-active" : "stage-chip"
              }
            >
              {STAGE_LABELS[stage]}
            </div>
          ))}
        </div>

        {detail?.error_message ? <div className="error-banner">{detail.error_message}</div> : null}
      </section>

      {overnightContext ? (
        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="eyebrow">隔夜来源</p>
              <h3>尾盘候选上下文</h3>
            </div>
          </div>

          <div className="structured-summary-grid">
            <article className="analysis-note-card">
              <strong>来源摘要</strong>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>扫描日期</span>
                  <strong>{overnightContext.scan_trade_date ?? "--"}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>扫描模式</span>
                  <strong>
                    {overnightContext.scan_mode
                      ? getOvernightModeLabel(
                          overnightContext.scan_mode as "strict" | "research_fallback"
                        )
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>候选分层</span>
                  <strong>{getBucketLabel(overnightContext.bucket)}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>尾盘质量</span>
                  <strong>
                    {overnightContext.quality
                      ? getOvernightQualityLabel(overnightContext.quality)
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>配置哈希</span>
                  <strong className="notranslate" translate="no" lang="en">
                    {overnightContext.evaluation_config_hash ?? "--"}
                  </strong>
                </div>
              </div>
            </article>

            <article className="analysis-note-card">
              <strong>扫描分数</strong>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>Quick Score</span>
                  <strong>
                    {typeof overnightContext.quick_score === "number"
                      ? overnightContext.quick_score.toFixed(1)
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>Total Score</span>
                  <strong>
                    {typeof overnightContext.total_score === "number"
                      ? overnightContext.total_score.toFixed(1)
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>次日验证</span>
                  <strong>{getValidationLabel(overnightContext.validation_status)}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>次日开盘收益</span>
                  <strong>
                    {typeof overnightContext.next_open_return === "number"
                      ? `${overnightContext.next_open_return.toFixed(2)}%`
                      : "--"}
                  </strong>
                </div>
              </div>
            </article>

            <article className="analysis-note-card">
              <strong>关键尾盘指标</strong>
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>14:30 后涨幅</span>
                  <strong>
                    {typeof overnightContext.tail_metrics?.tail_return_pct === "number"
                      ? `${overnightContext.tail_metrics.tail_return_pct.toFixed(2)}%`
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>尾盘成交额占比</span>
                  <strong>
                    {typeof overnightContext.tail_metrics?.tail_amount_ratio === "number"
                      ? overnightContext.tail_metrics.tail_amount_ratio.toFixed(2)
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>最后 10 分钟涨幅</span>
                  <strong>
                    {typeof overnightContext.tail_metrics?.last10_return_pct === "number"
                      ? `${overnightContext.tail_metrics.last10_return_pct.toFixed(2)}%`
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>收盘靠近高点</span>
                  <strong>
                    {typeof overnightContext.tail_metrics?.close_at_high_ratio === "number"
                      ? overnightContext.tail_metrics.close_at_high_ratio.toFixed(2)
                      : "--"}
                  </strong>
                </div>
              </div>
            </article>

            <article className="analysis-note-card">
              <strong>规则来源</strong>
              <p>
                {overnightContext.provider_route
                  ? Object.entries(overnightContext.provider_route)
                      .map(([key, value]) => `${key}: ${String(value)}`)
                      .join(" / ")
                  : "当前没有额外的 provider route 记录。"}
              </p>
              <p>
                {overnightContext.factor_breakdown
                  ? Object.entries(overnightContext.factor_breakdown)
                      .map(([key, value]) => `${key}: ${Number(value).toFixed(1)}`)
                      .join(" / ")
                  : "当前没有扫描因子拆解。"}
              </p>
            </article>
          </div>
        </section>
      ) : null}

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">结构化结论</p>
            <h3>因子快照与证据摘要</h3>
          </div>
          <div className="download-row">
            {detail?.download_urls.factor_snapshot_json ? (
              <a
                className="secondary-button"
                href={resolveDownloadUrl(detail.download_urls.factor_snapshot_json)}
                target="_blank"
                rel="noreferrer"
              >
                下载因子快照
              </a>
            ) : null}
            {detail?.download_urls.evidence_snapshot_json ? (
              <a
                className="secondary-button"
                href={resolveDownloadUrl(detail.download_urls.evidence_snapshot_json)}
                target="_blank"
                rel="noreferrer"
              >
                下载证据快照
              </a>
            ) : null}
            {detail?.download_urls.structured_decision_json ? (
              <a
                className="secondary-button"
                href={resolveDownloadUrl(detail.download_urls.structured_decision_json)}
                target="_blank"
                rel="noreferrer"
              >
                下载规则结论
              </a>
            ) : null}
          </div>
        </div>

        {factorSnapshot ? (
          <div className="factor-summary-shell">
            <div className="factor-summary-hero">
              <div className="factor-summary-score">
                <span>综合评分</span>
                <strong>{factorSnapshot.composite_score.toFixed(2)}</strong>
              </div>
              <div className="factor-summary-meta">
                <div>
                  <span>推荐动作</span>
                  <strong>
                    {structuredDecision?.decision ?? factorSnapshot.recommended_action}
                  </strong>
                </div>
                <div>
                  <span>置信度</span>
                  <strong>
                    {Math.round(
                      (structuredDecision?.confidence ?? factorSnapshot.confidence) * 100
                    )}
                    %
                  </strong>
                </div>
                <div>
                  <span>市场</span>
                  <strong>{getMarketLabel(factorSnapshot.market_region)}</strong>
                </div>
              </div>
            </div>

            {structuredDecision ? (
              <div className="structured-summary-grid">
                <article className="analysis-note-card">
                  <strong>为什么是 {structuredDecision.decision}</strong>
                  <p>{structuredDecision.summary}</p>
                </article>
                <article className="analysis-note-card">
                  <strong>阈值规则</strong>
                  <p>
                    {structuredDecision.threshold_policy
                      ? `${structuredDecision.threshold_policy.style} 风格：BUY >= ${structuredDecision.threshold_policy.buy_at_or_above}，SELL <= ${structuredDecision.threshold_policy.sell_at_or_below}，最小方向性置信度 ${structuredDecision.threshold_policy.min_confidence_for_directional_call}`
                      : "当前任务没有额外记录阈值策略。"}
                  </p>
                </article>
                <article className="analysis-note-card">
                  <strong>主要利多</strong>
                  {structuredDecision.primary_drivers?.length ? (
                    <ul className="decision-driver-list">
                      {structuredDecision.primary_drivers.map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>当前没有提取到显著利多驱动。</p>
                  )}
                </article>
                <article className="analysis-note-card">
                  <strong>主要风险</strong>
                  {structuredDecision.risk_flags?.length ? (
                    <ul className="decision-driver-list">
                      {structuredDecision.risk_flags.map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>当前没有提取到显著风险标记。</p>
                  )}
                </article>
              </div>
            ) : null}

            <div className="factor-grid">
              {Object.entries(factorBlocks).map(([key, block]) => {
                const evidenceGroup = normalizeEvidenceGroup(
                  (evidenceSnapshot as Record<string, unknown>)[key]
                );
                const subscores = block.subscores ?? {};
                return (
                  <article key={key} className="factor-card">
                    <div className="factor-card-head">
                      <div>
                        <p>{FACTOR_LABELS[key] ?? key}</p>
                        <h4>{block.score.toFixed(1)}</h4>
                      </div>
                      <span>{Math.round(block.confidence * 100)}% 置信度</span>
                    </div>

                    <p className="factor-card-copy">{block.summary}</p>

                    {Object.keys(subscores).length ? (
                      <div className="factor-subscore-list">
                        {Object.entries(subscores).map(([subKey, item]) => (
                          <div key={subKey} className="factor-subscore-item">
                            <div>
                              <strong>{SUBSCORE_LABELS[subKey] ?? subKey}</strong>
                              <span>{item.summary}</span>
                            </div>
                            <em>{item.score.toFixed(1)}</em>
                          </div>
                        ))}
                      </div>
                    ) : null}

                    <div className="factor-signal-grid">
                      <FactorSignalList
                        title="主要利多"
                        items={block.top_positive_signals ?? evidenceGroup.strengths}
                        emptyText="当前没有显著利多信号。"
                        tone="positive"
                      />
                      <FactorSignalList
                        title="主要风险"
                        items={block.top_negative_signals ?? evidenceGroup.risks}
                        emptyText="当前没有显著风险信号。"
                        tone="negative"
                      />
                    </div>

                    <FactorSignalList
                      title="关键指标"
                      items={evidenceGroup.raw_metrics.slice(0, 4)}
                      emptyText="当前没有可展示的原始指标。"
                      tone="neutral"
                    />

                    {block.confidence_drivers?.length ? (
                      <div className="factor-confidence-list">
                        <strong>置信度来源</strong>
                        <ul className="decision-driver-list">
                          {block.confidence_drivers.map((item) => (
                            <li key={item}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </div>
          </div>
        ) : (
          <div className="empty-state">当前任务还没有生成结构化因子快照。</div>
        )}
      </section>

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">分析报告</p>
            <h3>智能体输出</h3>
          </div>
        </div>

        <div className="tab-row">
          {REPORT_TABS.map((tab) => (
            <button
              key={tab.key}
              className={
                activeTab === tab.key ? "filter-button filter-active" : "filter-button"
              }
              type="button"
              onClick={() => setActiveTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="report-shell">
          {activeReport ? (
            <div className="markdown-body">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{activeReport}</ReactMarkdown>
            </div>
          ) : (
            <div className="empty-state">当前标签还没有可展示的内容。</div>
          )}
        </div>
      </section>
    </div>
  );
}
