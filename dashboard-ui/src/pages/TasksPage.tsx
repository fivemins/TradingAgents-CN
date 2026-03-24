import { useEffect, useState } from "react";

import { dashboardApi } from "../api";
import { TaskTable } from "../components/TaskTable";
import type { TaskListResponse, TaskStatus } from "../types";

const STATUS_FILTERS: Array<{ label: string; value: TaskStatus | "all" }> = [
  { label: "全部", value: "all" },
  { label: "排队中", value: "queued" },
  { label: "运行中", value: "running" },
  { label: "已完成", value: "succeeded" },
  { label: "失败", value: "failed" }
];

const SOURCE_FILTERS: Array<{ label: string; value: "all" | "overnight_scan" }> = [
  { label: "全部来源", value: "all" },
  { label: "来自隔夜扫描", value: "overnight_scan" }
];

export function TasksPage() {
  const [data, setData] = useState<TaskListResponse | null>(null);
  const [error, setError] = useState("");
  const [statusFilter, setStatusFilter] = useState<TaskStatus | "all">("all");
  const [sourceFilter, setSourceFilter] = useState<"all" | "overnight_scan">("all");

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const next = await dashboardApi.getTasks(sourceFilter === "all" ? undefined : sourceFilter);
        if (!cancelled) {
          setData(next);
          setError("");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "任务加载失败。");
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [sourceFilter]);

  const tasks = statusFilter === "all" ? data?.items ?? [] : data?.items.filter((task) => task.status === statusFilter) ?? [];
  const toggleStatus = (value: TaskStatus | "all") => setStatusFilter((current) => (current === value && value !== "all" ? "all" : value));
  const toggleSource = (value: "all" | "overnight_scan") => setSourceFilter((current) => (current === value && value !== "all" ? "all" : value));

  return (
    <div className="page-grid">
      <section className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">任务中心</p>
            <h3>全部分析任务</h3>
          </div>
        </div>

        <div className="filter-toolbar">
          <div className="filter-group">
            <span className="filter-group-title">状态</span>
            <div className="filter-row">
              {STATUS_FILTERS.map((item) => (
                <button key={item.value} className={statusFilter === item.value ? "filter-button filter-active" : "filter-button"} onClick={() => toggleStatus(item.value)} type="button">
                  {item.label}
                </button>
              ))}
            </div>
          </div>

          <div className="filter-group">
            <span className="filter-group-title">来源</span>
            <div className="filter-row">
              {SOURCE_FILTERS.map((item) => (
                <button key={item.value} className={sourceFilter === item.value ? "filter-button filter-active" : "filter-button"} onClick={() => toggleSource(item.value)} type="button">
                  {item.label}
                </button>
              ))}
            </div>
          </div>
        </div>

        {error ? <div className="error-banner">{error}</div> : null}
        <TaskTable tasks={tasks} emptyText="当前筛选条件下没有任务。" />
      </section>
    </div>
  );
}
