import { useEffect, useRef, useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { dashboardApi } from "../api";
import {
  ANALYST_CARD_META,
  RESEARCH_DEPTH_META,
  getProviderLabel
} from "../labels";
import type {
  AnalystValue,
  CreateTaskRequest,
  MarketRegion,
  MarketRegionOption,
  TaskOptionsResponse
} from "../types";

interface CalendarDay {
  iso: string;
  label: string;
  isCurrentMonth: boolean;
  isSelected: boolean;
  isToday: boolean;
  date: Date;
}

const ANALYST_ORDER: AnalystValue[] = [
  "market",
  "social",
  "news",
  "fundamentals"
];

const DEPTH_ORDER = [1, 3, 5] as const;
const WEEKDAY_LABELS = ["一", "二", "三", "四", "五", "六", "日"] as const;

function todayIso() {
  const today = new Date();
  today.setMinutes(today.getMinutes() - today.getTimezoneOffset());
  return today.toISOString().slice(0, 10);
}

function parseIsoDate(value: string) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) {
    return null;
  }

  return new Date(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    12,
    0,
    0,
    0
  );
}

function formatIsoDate(date: Date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getMonthAnchor(value: string) {
  const date = parseIsoDate(value) ?? parseIsoDate(todayIso())!;
  return new Date(date.getFullYear(), date.getMonth(), 1, 12, 0, 0, 0);
}

function formatMonthTitle(date: Date) {
  return `${date.getFullYear()} 年 ${date.getMonth() + 1} 月`;
}

function inferMarketRegion(ticker: string): MarketRegion {
  const normalized = ticker.trim().toUpperCase();
  if (/^\d{6}(\.(SZ|SS))?$/.test(normalized)) {
    return "cn_a";
  }
  return "us";
}

function previewNormalizedTicker(ticker: string, marketRegion: MarketRegion) {
  const normalized = ticker.trim().toUpperCase();

  if (!normalized || marketRegion !== "cn_a") {
    return normalized;
  }

  if (/\.(SZ|SS)$/.test(normalized)) {
    return normalized;
  }

  if (!/^\d{6}$/.test(normalized)) {
    return normalized;
  }

  if (/^[569]\d{5}$/.test(normalized)) {
    return `${normalized}.SS`;
  }

  return `${normalized}.SZ`;
}

function buildCalendarDays(monthAnchor: Date, selectedIso: string): CalendarDay[] {
  const monthStart = new Date(
    monthAnchor.getFullYear(),
    monthAnchor.getMonth(),
    1,
    12,
    0,
    0,
    0
  );
  const startOffset = (monthStart.getDay() + 6) % 7;
  const firstVisibleDay = new Date(
    monthStart.getFullYear(),
    monthStart.getMonth(),
    1 - startOffset,
    12,
    0,
    0,
    0
  );
  const today = todayIso();

  return Array.from({ length: 42 }, (_, index) => {
    const day = new Date(
      firstVisibleDay.getFullYear(),
      firstVisibleDay.getMonth(),
      firstVisibleDay.getDate() + index,
      12,
      0,
      0,
      0
    );
    const iso = formatIsoDate(day);

    return {
      iso,
      label: `${day.getDate()}`,
      isCurrentMonth: day.getMonth() === monthAnchor.getMonth(),
      isSelected: iso === selectedIso,
      isToday: iso === today,
      date: day
    };
  });
}

export function AnalyzePage() {
  const navigate = useNavigate();
  const datePickerRef = useRef<HTMLDivElement | null>(null);
  const [options, setOptions] = useState<TaskOptionsResponse | null>(null);
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [marketRegion, setMarketRegion] = useState<MarketRegion>("cn_a");
  const [isDatePickerOpen, setIsDatePickerOpen] = useState(false);
  const [displayMonth, setDisplayMonth] = useState(() => getMonthAnchor(todayIso()));
  const [form, setForm] = useState<CreateTaskRequest>({
    ticker: "600519",
    analysis_date: todayIso(),
    market_region: "cn_a",
    analysts: ANALYST_ORDER,
    research_depth: 1,
    llm_provider: "ark",
    quick_think_llm: "doubao-seed-2.0-lite",
    deep_think_llm: "doubao-seed-2.0-pro",
    online_tools: true
  });

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const result = await dashboardApi.getOptions();
        if (cancelled) {
          return;
        }

        const analysisDate = result.defaults.analysis_date || todayIso();
        const defaultRegion =
          result.defaults.market_region || inferMarketRegion(result.defaults.ticker);

        setOptions(result);
        setMarketRegion(defaultRegion);
        setDisplayMonth(getMonthAnchor(analysisDate));
        setForm({
          ticker: result.defaults.ticker,
          analysis_date: analysisDate,
          market_region: defaultRegion,
          analysts: result.defaults.analysts,
          research_depth: result.defaults.research_depth,
          llm_provider: result.defaults.llm_provider,
          quick_think_llm: result.defaults.quick_think_llm,
          deep_think_llm: result.defaults.deep_think_llm,
          online_tools: result.defaults.online_tools
        });
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "分析配置加载失败。");
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!isDatePickerOpen) {
      return;
    }

    const handlePointerDown = (event: MouseEvent) => {
      if (!datePickerRef.current?.contains(event.target as Node)) {
        setIsDatePickerOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsDatePickerOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [isDatePickerOpen]);

  const modelOptions = options?.model_options[form.llm_provider];
  const selectedDepth = RESEARCH_DEPTH_META[form.research_depth];
  const selectedTeamTitles = ANALYST_ORDER.filter((item) =>
    form.analysts.includes(item)
  ).map((item) => ANALYST_CARD_META[item].title);
  const marketOptions = options?.market_regions ?? [];
  const selectedMarketOption =
    marketOptions.find((item) => item.value === marketRegion) ?? marketOptions[0];
  const normalizedTicker = previewNormalizedTicker(form.ticker, marketRegion);
  const calendarDays = buildCalendarDays(displayMonth, form.analysis_date);
  const showsNormalizationHint =
    marketRegion === "cn_a" &&
    normalizedTicker.length > 0 &&
    normalizedTicker !== form.ticker.trim().toUpperCase();

  const updateProvider = (provider: string) => {
    const nextQuick = options?.model_options[provider]?.quick[0]?.value ?? "";
    const nextDeep = options?.model_options[provider]?.deep[0]?.value ?? "";
    setForm((current) => ({
      ...current,
      llm_provider: provider,
      quick_think_llm: nextQuick,
      deep_think_llm: nextDeep
    }));
  };

  const updateMarketRegion = (value: MarketRegion) => {
    setMarketRegion(value);
    setForm((current) => ({
      ...current,
      market_region: value
    }));
  };

  const toggleAnalyst = (value: AnalystValue) => {
    setForm((current) => {
      const hasValue = current.analysts.includes(value);
      const nextAnalysts = hasValue
        ? current.analysts.filter((item) => item !== value)
        : [...current.analysts, value];
      return {
        ...current,
        analysts: nextAnalysts.length > 0 ? nextAnalysts : current.analysts
      };
    });
  };

  const selectDate = (iso: string, date: Date) => {
    setForm((current) => ({
      ...current,
      analysis_date: iso
    }));
    setDisplayMonth(new Date(date.getFullYear(), date.getMonth(), 1, 12, 0, 0, 0));
    setIsDatePickerOpen(false);
  };

  const selectToday = () => {
    const today = todayIso();
    selectDate(today, parseIsoDate(today) ?? new Date());
  };

  const clearDate = () => {
    setForm((current) => ({
      ...current,
      analysis_date: ""
    }));
    setIsDatePickerOpen(false);
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    setError("");

    try {
      const task = await dashboardApi.createTask({
        ...form,
        ticker: form.ticker.trim().toUpperCase(),
        market_region: marketRegion
      });
      navigate(`/tasks/${task.task_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "任务提交失败。");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="analysis-page">
      <section className="analysis-overview-card">
        <div className="analysis-overview-icon" aria-hidden="true">
          股
        </div>
        <div className="analysis-overview-copy">
          <p className="analysis-overview-label">股票分析</p>
          <h2>单股分析</h2>
          <p>
            面向 A 股优先场景的多智能体研究台，支持市场、情绪、新闻和基本面四路协同分析。
          </p>
        </div>
        <div className="analysis-overview-stats">
          <div className="analysis-overview-stat">
            <span>当前深度</span>
            <strong>{selectedDepth.shortLabel}</strong>
          </div>
          <div className="analysis-overview-stat">
            <span>分析团队</span>
            <strong>{form.analysts.length} 位智能体</strong>
          </div>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <form className="analysis-shell" onSubmit={submit}>
        <div className="analysis-main">
          <section className="analysis-composer-card">
            <div className="analysis-section-head">
              <div>
                <h3>分析配置</h3>
                <p>设置股票信息、分析深度与分析团队。</p>
              </div>
              <span className="analysis-section-tag">必填信息</span>
            </div>

            <div className="analysis-section-body">
              <section className="analysis-group">
                <div className="analysis-group-header">
                  <div className="analysis-group-title-row">
                    <span className="analysis-group-dot" />
                    <h4>股票信息</h4>
                  </div>
                  <p>输入股票代码、目标分析日期，并选择分析市场。</p>
                </div>

                <div className="analysis-input-grid analysis-input-grid-stock">
                  <label className="field">
                    <span className="field-label-row">
                      <span className="field-label field-label-required">
                        股票代码（Ticker）
                      </span>
                      <span className="field-note">
                        {selectedMarketOption?.example ?? "例如 600519, NVDA"}
                      </span>
                    </span>
                    <input
                      className="notranslate"
                      translate="no"
                      lang="en"
                      value={form.ticker}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          ticker: event.target.value.toUpperCase()
                        }))
                      }
                      placeholder={
                        marketRegion === "cn_a" ? "请输入 6 位 A 股代码" : "请输入美股代码"
                      }
                      required
                    />
                  </label>

                  <label className="field">
                    <span className="field-label-row">
                      <span className="field-label">市场类型</span>
                      <span className="field-note">决定数据源和代码规则</span>
                    </span>
                    <select
                      value={marketRegion}
                      onChange={(event) =>
                        updateMarketRegion(event.target.value as MarketRegion)
                      }
                    >
                      {marketOptions.map((option: MarketRegionOption) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>

                  <div className="field field-span-2">
                    <div className="field-label-row">
                      <span className="field-label">分析日期</span>
                      <span className="field-note">支持历史回放分析</span>
                    </div>

                    <div className="date-picker-shell" ref={datePickerRef}>
                      <button
                        type="button"
                        className={
                          isDatePickerOpen
                            ? "date-picker-trigger date-picker-trigger-open"
                            : "date-picker-trigger"
                        }
                        aria-expanded={isDatePickerOpen}
                        aria-haspopup="dialog"
                        onClick={() => {
                          setDisplayMonth(getMonthAnchor(form.analysis_date || todayIso()));
                          setIsDatePickerOpen((current) => !current);
                        }}
                      >
                        <span className="date-picker-trigger-main">
                          <span className="date-picker-icon" aria-hidden="true">
                            日
                          </span>
                          <span
                            className={
                              form.analysis_date
                                ? "date-picker-value"
                                : "date-picker-placeholder"
                            }
                          >
                            {form.analysis_date || "请选择分析日期"}
                          </span>
                        </span>
                        <span className="date-picker-chevron" aria-hidden="true">
                          ▾
                        </span>
                      </button>

                      {isDatePickerOpen ? (
                        <div className="date-picker-popover" role="dialog" aria-modal="false">
                          <div className="date-picker-header">
                            <div>
                              <span className="date-picker-kicker">扫描日期</span>
                              <strong>{formatMonthTitle(displayMonth)}</strong>
                            </div>
                            <div className="date-picker-nav-group">
                              <button
                                type="button"
                                className="date-picker-nav"
                                aria-label="上个月"
                                onClick={() =>
                                  setDisplayMonth(
                                    (current) =>
                                      new Date(
                                        current.getFullYear(),
                                        current.getMonth() - 1,
                                        1,
                                        12,
                                        0,
                                        0,
                                        0
                                      )
                                  )
                                }
                              >
                                ‹
                              </button>
                              <button
                                type="button"
                                className="date-picker-nav"
                                aria-label="下个月"
                                onClick={() =>
                                  setDisplayMonth(
                                    (current) =>
                                      new Date(
                                        current.getFullYear(),
                                        current.getMonth() + 1,
                                        1,
                                        12,
                                        0,
                                        0,
                                        0
                                      )
                                  )
                                }
                              >
                                ›
                              </button>
                            </div>
                          </div>

                          <div className="date-picker-weekdays">
                            {WEEKDAY_LABELS.map((weekday) => (
                              <span key={weekday}>{weekday}</span>
                            ))}
                          </div>

                          <div className="date-picker-grid">
                            {calendarDays.map((day) => (
                              <button
                                key={day.iso}
                                type="button"
                                className={[
                                  "date-picker-day",
                                  day.isCurrentMonth ? "" : "date-picker-day-muted",
                                  day.isSelected ? "date-picker-day-selected" : "",
                                  day.isToday ? "date-picker-day-today" : ""
                                ]
                                  .filter(Boolean)
                                  .join(" ")}
                                onClick={() => selectDate(day.iso, day.date)}
                              >
                                {day.label}
                              </button>
                            ))}
                          </div>

                          <div className="date-picker-footer">
                            <button
                              type="button"
                              className="date-picker-footer-button date-picker-footer-button-secondary"
                              onClick={clearDate}
                            >
                              清除
                            </button>
                            <button
                              type="button"
                              className="date-picker-footer-button"
                              onClick={selectToday}
                            >
                              今天
                            </button>
                            <button
                              type="button"
                              className="date-picker-footer-button date-picker-footer-button-secondary"
                              onClick={() => setIsDatePickerOpen(false)}
                            >
                              关闭
                            </button>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </div>

                <div className="analysis-ticker-note">
                  <strong>{selectedMarketOption?.label ?? "市场规则"}</strong>
                  <p>
                    {selectedMarketOption?.description ??
                      "根据市场选择不同的数据源与代码格式。"}
                  </p>
                  {showsNormalizationHint ? (
                    <p>
                      后端会按{" "}
                      <span className="notranslate" translate="no" lang="en">
                        {normalizedTicker}
                      </span>{" "}
                      进行规范化处理。
                    </p>
                  ) : null}
                </div>
              </section>

              <section className="analysis-group">
                <div className="analysis-group-header">
                  <div className="analysis-group-title-row">
                    <span className="analysis-group-dot" />
                    <h4>分析深度</h4>
                  </div>
                  <p>对应后端真实辩论轮数与风险讨论轮数。</p>
                </div>

                <div className="analysis-card-grid analysis-card-grid-depth">
                  {DEPTH_ORDER.map((depth) => {
                    const meta = RESEARCH_DEPTH_META[depth];
                    const isSelected = form.research_depth === depth;

                    return (
                      <button
                        key={depth}
                        type="button"
                        className={
                          isSelected
                            ? "analysis-option-card analysis-option-card-selected"
                            : "analysis-option-card"
                        }
                        aria-pressed={isSelected}
                        onClick={() =>
                          setForm((current) => ({ ...current, research_depth: depth }))
                        }
                      >
                        <span className="analysis-card-icon">{meta.tone}</span>
                        <div className="analysis-card-meta">
                          <strong>{meta.title}</strong>
                          <p>{meta.description}</p>
                          <span>{meta.duration}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </section>

              <section className="analysis-group">
                <div className="analysis-group-header">
                  <div className="analysis-group-title-row">
                    <span className="analysis-group-dot" />
                    <h4>分析团队</h4>
                  </div>
                  <p>至少保留 1 位分析师参与本次任务。</p>
                </div>

                <div className="analysis-card-grid analysis-card-grid-team">
                  {ANALYST_ORDER.map((analyst) => {
                    const meta = ANALYST_CARD_META[analyst];
                    const isSelected = form.analysts.includes(analyst);

                    return (
                      <button
                        key={analyst}
                        type="button"
                        className={
                          isSelected
                            ? "analysis-option-card analysis-option-card-selected"
                            : "analysis-option-card"
                        }
                        aria-pressed={isSelected}
                        onClick={() => toggleAnalyst(analyst)}
                      >
                        <span className="analysis-card-icon">{meta.tone}</span>
                        <div className="analysis-card-meta">
                          <strong>{meta.title}</strong>
                          <p>{meta.description}</p>
                          <span>
                            {isSelected ? "已加入本次分析" : "点击加入本次分析"}
                          </span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </section>
            </div>
          </section>
        </div>

        <aside className="analysis-sidebar">
          <section className="analysis-side-card">
            <div className="analysis-section-head analysis-section-head-side">
              <div>
                <h3>高级配置</h3>
                <p>模型选择、在线工具开关与执行预估。</p>
              </div>
              <span className="analysis-section-tag analysis-section-tag-muted">
                可选设置
              </span>
            </div>

            <div className="analysis-section-body analysis-side-body">
              <div className="analysis-side-block">
                <div className="analysis-side-heading">
                  <h4>AI 模型配置</h4>
                  <p>保留当前真实支持的 provider 与模型参数。</p>
                </div>

                <label className="field field-compact">
                  <span className="field-label">LLM 服务商</span>
                  <select
                    className="notranslate"
                    translate="no"
                    lang="en"
                    value={form.llm_provider}
                    onChange={(event) => updateProvider(event.target.value)}
                  >
                    {options?.providers.map((provider) => (
                      <option
                        key={provider.value}
                        value={provider.value}
                        className="notranslate"
                        lang="en"
                      >
                        {getProviderLabel(provider.value)}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="field field-compact">
                  <span className="field-label">快速分析模型</span>
                  <select
                    className="notranslate"
                    translate="no"
                    lang="en"
                    value={form.quick_think_llm}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        quick_think_llm: event.target.value
                      }))
                    }
                  >
                    {modelOptions?.quick.map((model) => (
                      <option
                        key={model.value}
                        value={model.value}
                        className="notranslate"
                        lang="en"
                      >
                        {model.value}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="field field-compact">
                  <span className="field-label">深度决策模型</span>
                  <select
                    className="notranslate"
                    translate="no"
                    lang="en"
                    value={form.deep_think_llm}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        deep_think_llm: event.target.value
                      }))
                    }
                  >
                    {modelOptions?.deep.map((model) => (
                      <option
                        key={model.value}
                        value={model.value}
                        className="notranslate"
                        lang="en"
                      >
                        {model.value}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="analysis-note-card">
                  <strong>模型建议</strong>
                  <p>
                    A 股标准分析可以先用轻量模型做快速归纳，再用更强模型负责综合决策和风险裁决。
                  </p>
                </div>
              </div>

              <div className="analysis-side-block">
                <div className="analysis-side-heading">
                  <h4>分析选项</h4>
                  <p>在线工具开启后，A 股会优先走本地化证据源。</p>
                </div>

                <button
                  type="button"
                  className={
                    form.online_tools
                      ? "analysis-switch analysis-switch-enabled"
                      : "analysis-switch"
                  }
                  aria-pressed={form.online_tools}
                  onClick={() =>
                    setForm((current) => ({
                      ...current,
                      online_tools: !current.online_tools
                    }))
                  }
                >
                  <div>
                    <strong>在线工具</strong>
                    <p>启用公告、热度、财务与新闻等在线数据抓取。</p>
                  </div>
                  <span className="analysis-switch-track">
                    <span className="analysis-switch-thumb" />
                  </span>
                </button>
              </div>

              <div className="analysis-side-block">
                <div className="analysis-side-heading">
                  <h4>执行预估</h4>
                  <p>这些信息只用于前端提示，不改变后端请求结构。</p>
                </div>

                <div className="analysis-estimate-list">
                  <div className="analysis-estimate-row">
                    <span>分析市场</span>
                    <strong>{selectedMarketOption?.label ?? marketRegion}</strong>
                  </div>
                  <div className="analysis-estimate-row">
                    <span>分析深度</span>
                    <strong>{selectedDepth.shortLabel}</strong>
                  </div>
                  <div className="analysis-estimate-row">
                    <span>预计耗时</span>
                    <strong>{selectedDepth.duration}</strong>
                  </div>
                  <div className="analysis-estimate-row">
                    <span>分析团队</span>
                    <strong>{form.analysts.length} 位智能体</strong>
                  </div>
                  <div className="analysis-estimate-row">
                    <span>在线工具</span>
                    <strong>{form.online_tools ? "已开启" : "已关闭"}</strong>
                  </div>
                  <div className="analysis-estimate-row">
                    <span>当前服务商</span>
                    <strong className="notranslate" translate="no" lang="en">
                      {getProviderLabel(form.llm_provider)}
                    </strong>
                  </div>
                </div>

                <div className="analysis-team-summary">
                  <span>已选团队</span>
                  <p>{selectedTeamTitles.join(" / ")}</p>
                </div>
              </div>

              <button
                className="primary-button analysis-submit-button"
                type="submit"
                disabled={submitting}
              >
                {submitting ? "正在提交分析任务..." : "开始分析"}
              </button>
            </div>
          </section>
        </aside>
      </form>
    </div>
  );
}
