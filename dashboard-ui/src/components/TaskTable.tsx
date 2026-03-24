import { Link } from "react-router-dom";

import { getMarketLabel, getProviderLabel } from "../labels";
import { StatusBadge } from "./StatusBadge";
import type { TaskSummary } from "../types";

interface TaskTableProps {
  tasks: TaskSummary[];
  emptyText: string;
}

function formatDate(value: string | null) {
  if (!value) return "--";
  return new Date(value).toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatScore(task: TaskSummary) {
  const score = task.structured_summary?.composite_score;
  return typeof score === "number" ? score.toFixed(1) : "--";
}

export function TaskTable({ tasks, emptyText }: TaskTableProps) {
  if (!tasks.length) {
    return <div className="empty-state">{emptyText}</div>;
  }

  return (
    <div className="table-shell">
      <table className="task-table">
        <thead>
          <tr>
            <th>股票代码</th>
            <th>市场</th>
            <th>分析日期</th>
            <th>任务状态</th>
            <th>当前阶段</th>
            <th>最终动作</th>
            <th>结构化摘要</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((task) => (
            <tr key={task.task_id}>
              <td>
                <div className="ticker-cell">
                  <strong className="notranslate" translate="no" lang="en">
                    {task.ticker}
                  </strong>
                  <span className="notranslate" translate="no" lang="en">
                    {getProviderLabel(task.llm_provider)}
                  </span>
                  {task.source_context?.type === "overnight_scan" ? (
                    <span>来自隔夜扫描</span>
                  ) : null}
                </div>
              </td>
              <td>{getMarketLabel(task.market_region)}</td>
              <td>{task.analysis_date}</td>
              <td><StatusBadge value={task.status} /></td>
              <td><StatusBadge value={task.stage} variant="stage" /></td>
              <td><StatusBadge value={task.decision} variant="decision" /></td>
              <td>
                <div className="summary-cell">
                  <strong>评分 {formatScore(task)}</strong>
                  <span>{task.structured_summary?.primary_driver ?? "暂无主要利多"}</span>
                  <span>{task.structured_summary?.primary_risk ?? "暂无主要风险"}</span>
                </div>
              </td>
              <td>{formatDate(task.created_at)}</td>
              <td>
                <Link className="table-link" to={`/tasks/${task.task_id}`}>
                  查看详情
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
