import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { dashboardApi } from "../api";
import { OvernightExecutionStatus } from "../components/OvernightExecutionStatus";
import { StatusBadge } from "../components/StatusBadge";
import { TaskTable } from "../components/TaskTable";
import { getProgressMessageLabel } from "../labels";
import type {
  OvernightReviewSummary,
  OvernightScanSummary,
  SystemReadinessResponse,
  TaskListResponse,
} from "../types";

interface DashboardData {
  tasks: TaskListResponse | null;
  latestScan: OvernightScanSummary | null;
  latestReview: OvernightReviewSummary | null;
  latestValidatedScan: OvernightScanSummary | null;
  readiness: SystemReadinessResponse | null;
}

function useDashboardPolling(intervalMs: number) {
  const [data, setData] = useState<DashboardData>({
    tasks: null,
    latestScan: null,
    latestReview: null,
    latestValidatedScan: null,
    readiness: null,
  });
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [tasks, scans, reviews] = await Promise.all([
          dashboardApi.getTasks(),
          dashboardApi.getOvernightScans(),
          dashboardApi.getOvernightReviews(),
        ]);
        const readiness = await dashboardApi.getSystemReadiness().catch(() => null);
        const latestScan = scans.items[0] ?? null;
        const latestReview = reviews.items[0] ?? null;
        const latestValidatedScan =
          scans.items.find(
            (item) =>
              item.status === "succeeded" &&
              item.validation_status === "validated" &&
              item.validated_formal_count > 0,
          ) ?? null;

        if (!cancelled) {
          setData({ tasks, latestScan, latestReview, latestValidatedScan, readiness });
          setError("");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "首页数据加载失败。");
        }
      }
    };

    void load();
    const timer = window.setInterval(() => void load(), intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [intervalMs]);

  return { data, error };
}

function formatPercent(value: number | null | undefined) {
  return typeof value === "number" ? `${value.toFixed(2)}%` : "--";
}

function formatHitRate(value: number | null | undefined) {
  return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
}

export function DashboardPage() {
  const { data, error } = useDashboardPolling(5000);
  const stats = data.tasks?.stats ?? {
    total: 0,
    queued: 0,
    running: 0,
    succeeded: 0,
    failed: 0,
  };
  const latestTasks = data.tasks?.items.slice(0, 5) ?? [];
  const latestDecisions =
    data.tasks?.items.filter((task) => task.decision).slice(0, 3) ?? [];
  const latestReviewSummary = data.latestReview?.summary_snapshot;

  return (
    <div className="page-grid">
      <section className="hero-card">
        <div>
          <p className="eyebrow">A 股研究工作台</p>
          <h2>
            <span className="notranslate" translate="no" lang="en">
              TradingAgents
            </span>{" "}
            仪表盘
          </h2>
          <p className="hero-copy">
            这里会统一展示单股深度分析、隔夜扫描结果、次日开盘回填，以及近 60
            个交易日的历史验证表现。
          </p>
        </div>
        <div className="hero-actions">
          <Link className="primary-button" to="/analyze">
            快速开始分析
          </Link>
          <Link className="secondary-button" to="/overnight">
            打开隔夜推荐
          </Link>
          <Link className="secondary-button" to="/tasks">
            查看全部任务
          </Link>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="stats-grid">
        <div className="stat-card">
          <span>任务总数</span>
          <strong>{stats.total}</strong>
        </div>
        <div className="stat-card">
          <span>运行中</span>
          <strong>{stats.running}</strong>
        </div>
        <div className="stat-card">
          <span>已完成</span>
          <strong>{stats.succeeded}</strong>
        </div>
        <div className="stat-card">
          <span>失败</span>
          <strong>{stats.failed}</strong>
        </div>
      </section>

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">系统 Readiness</p>
            <h3>环境可用性与数据连通状态</h3>
          </div>
        </div>
        {data.readiness ? (
          <div className="stats-grid">
            {Object.entries(data.readiness.components).map(([key, component]) => (
              <div key={key} className="stat-card">
                <span>{component.name}</span>
                <strong>{component.ok ? "可用" : "异常"}</strong>
                <p className="card-note">{component.message}</p>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-state">正在检测系统环境与数据连通性。</div>
        )}
      </section>

      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">隔夜推荐</p>
            <h3>扫描、真实回填与历史验证</h3>
          </div>
          <Link className="table-link" to="/overnight">
            进入隔夜推荐页
          </Link>
        </div>

        <div className="overnight-home-grid">
          <article className="overnight-highlight-card">
            <OvernightExecutionStatus
              title="最新扫描状态"
              kind="scan"
              status={data.latestScan?.status}
              progressMessage={data.latestScan?.progress_message}
              compact
            />
            {data.latestScan ? (
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>扫描日期</span>
                  <strong>{data.latestScan.trade_date}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>市场提示</span>
                  <strong className="text-multiline">{data.latestScan.market_message}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>正式推荐</span>
                  <strong>{data.latestScan.formal_count}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>观察名单</span>
                  <strong>{data.latestScan.watchlist_count}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>前 3 只正式推荐</span>
                  <strong className="notranslate text-multiline" translate="no" lang="en">
                    {data.latestScan.top_formal_tickers.length
                      ? data.latestScan.top_formal_tickers.slice(0, 3).join(", ")
                      : "--"}
                  </strong>
                </div>
              </div>
            ) : (
              <div className="empty-state compact-empty">还没有隔夜扫描记录。</div>
            )}
          </article>

          <article className="overnight-highlight-card">
            <div className="decision-head">
              <strong>上一条实际回填结果</strong>
              <span className="analysis-badge">
                {data.latestValidatedScan ? "已回填" : "暂无"}
              </span>
            </div>
            {data.latestValidatedScan ? (
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>扫描日期</span>
                  <strong>{data.latestValidatedScan.trade_date}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>正式推荐数量</span>
                  <strong>{data.latestValidatedScan.formal_count}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>已验证数量</span>
                  <strong>{data.latestValidatedScan.validated_formal_count}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>平均次日开盘收益</span>
                  <strong>{formatPercent(data.latestValidatedScan.avg_next_open_return)}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>最佳候选</span>
                  <strong className="notranslate text-multiline" translate="no" lang="en">
                    {data.latestValidatedScan.best_candidate
                      ? `${data.latestValidatedScan.best_candidate.ticker} ${formatPercent(
                          data.latestValidatedScan.best_candidate.next_open_return,
                        )}`
                      : "--"}
                  </strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>最差候选</span>
                  <strong className="notranslate text-multiline" translate="no" lang="en">
                    {data.latestValidatedScan.worst_candidate
                      ? `${data.latestValidatedScan.worst_candidate.ticker} ${formatPercent(
                          data.latestValidatedScan.worst_candidate.next_open_return,
                        )}`
                      : "--"}
                  </strong>
                </div>
              </div>
            ) : (
              <div className="empty-state compact-empty">
                当前还没有已完成回填的扫描结果。
              </div>
            )}
          </article>

          <article className="overnight-highlight-card">
            <OvernightExecutionStatus
              title="最新历史验证状态"
              kind="review"
              status={data.latestReview?.status}
              progressMessage={data.latestReview?.progress_message}
              compact
            />
            {latestReviewSummary ? (
              <div className="analysis-estimate-list">
                <div className="analysis-estimate-row">
                  <span>截止日期</span>
                  <strong>{data.latestReview?.end_trade_date}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>回看窗口</span>
                  <strong>{latestReviewSummary.window_days} 交易日</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>有推荐的交易日数</span>
                  <strong>{latestReviewSummary.days_with_formal_picks}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>平均次日开盘收益</span>
                  <strong>{formatPercent(latestReviewSummary.avg_next_open_return)}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>正收益命中率</span>
                  <strong>{formatHitRate(latestReviewSummary.positive_pick_rate)}</strong>
                </div>
                <div className="analysis-estimate-row">
                  <span>平均超额收益</span>
                  <strong>{formatPercent(latestReviewSummary.avg_excess_return)}</strong>
                </div>
              </div>
            ) : (
              <div className="empty-state compact-empty">还没有历史验证记录。</div>
            )}
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">最近分析</p>
            <h3>最新任务列表</h3>
          </div>
          <Link className="table-link" to="/tasks">
            查看全部任务
          </Link>
        </div>
        <TaskTable tasks={latestTasks} emptyText="当前还没有分析任务。" />
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">近期输出</p>
            <h3>最近形成的交易结论</h3>
          </div>
        </div>
        {latestDecisions.length ? (
          <div className="decision-list">
            {latestDecisions.map((task) => (
              <article key={task.task_id} className="decision-card">
                <div className="decision-head">
                  <div>
                    <strong className="notranslate" translate="no" lang="en">
                      {task.ticker}
                    </strong>
                    <p>{task.analysis_date}</p>
                  </div>
                  <StatusBadge value={task.decision} variant="decision" />
                </div>
                <div className="decision-summary-grid">
                  <span>{task.structured_summary?.primary_driver ?? "暂无主要利多"}</span>
                  <span>{task.structured_summary?.primary_risk ?? "暂无主要风险"}</span>
                </div>
                <span>{getProgressMessageLabel(task.progress_message)}</span>
                <Link className="table-link" to={`/tasks/${task.task_id}`}>
                  查看详情
                </Link>
              </article>
            ))}
          </div>
        ) : (
          <div className="empty-state">当前还没有生成交易结论。</div>
        )}
      </section>
    </div>
  );
}
