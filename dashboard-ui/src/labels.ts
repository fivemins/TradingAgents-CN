import type {
  AnalystValue,
  OvernightMode,
  OvernightQuality,
  TaskStage,
  TaskStatus
} from "./types";

type BadgeVariant = "status" | "decision" | "stage";

export const STATUS_LABELS: Record<TaskStatus, string> = {
  queued: "排队中",
  running: "运行中",
  succeeded: "已完成",
  failed: "失败"
};

export const STAGE_LABELS: Record<TaskStage, string> = {
  initializing: "初始化",
  market: "市场分析",
  social: "情绪分析",
  news: "新闻分析",
  fundamentals: "基本面分析",
  research: "研究辩论",
  trader: "交易计划",
  risk: "风险评估",
  completed: "已完成"
};

export const DECISION_LABELS: Record<string, string> = {
  BUY: "买入",
  HOLD: "观望",
  SELL: "卖出"
};

export const PROVIDER_LABELS: Record<string, string> = {
  ark: "Ark",
  openai: "OpenAI",
  anthropic: "Anthropic",
  google: "Google",
  openrouter: "OpenRouter",
  ollama: "Ollama"
};

export const MODE_LABELS: Record<OvernightMode, string> = {
  strict: "收盘确认",
  intraday_preview: "盘中预估"
};

export const QUALITY_LABELS: Record<OvernightQuality, string> = {
  real: "真实尾盘",
  partial: "部分尾盘",
  proxy: "代理尾盘",
  missing: "缺失",
  invalid: "无效"
};

export const ANALYST_CARD_META: Record<
  AnalystValue,
  { title: string; description: string; tone: string }
> = {
  market: {
    title: "市场分析师",
    description: "分析价格趋势、成交结构、板块强弱和市场节奏。",
    tone: "市"
  },
  social: {
    title: "情绪分析师",
    description: "分析市场热度、投资者情绪和短期关注度变化。",
    tone: "情"
  },
  news: {
    title: "新闻分析师",
    description: "分析公告、公司事件、政策新闻和行业催化因素。",
    tone: "新"
  },
  fundamentals: {
    title: "基本面分析师",
    description: "分析财务质量、成长能力、估值和研究覆盖情况。",
    tone: "基"
  }
};

export const RESEARCH_DEPTH_META: Record<
  number,
  {
    title: string;
    description: string;
    duration: string;
    shortLabel: string;
    tone: string;
  }
> = {
  1: {
    title: "1级 - 快速分析",
    description: "快速洞察核心信号，适合快速决策。",
    duration: "约 2-5 分钟",
    shortLabel: "快速分析",
    tone: "1"
  },
  3: {
    title: "3级 - 标准分析",
    description: "平衡技术、情绪与基本面，适合常规判断。",
    duration: "约 4-8 分钟",
    shortLabel: "标准分析",
    tone: "3"
  },
  5: {
    title: "5级 - 全面分析",
    description: "多轮辩论与综合评估，适合高置信度研究。",
    duration: "约 8-16 分钟",
    shortLabel: "全面分析",
    tone: "5"
  }
};

const PROGRESS_MESSAGE_LABELS: Record<string, string> = {
  "Task created and waiting to start.": "任务已创建，等待启动。",
  "Task queued. Runner process started.": "任务已进入队列，后台执行器已启动。",
  "Initializing analysis graph.": "正在初始化分析图。",
  "Analysis completed successfully.": "分析已完成。",
  "Failed to start runner process.": "后台执行器启动失败。",
  "Task failed during execution.": "任务执行失败。",
  "Research debate started.": "研究辩论已开始。",
  "Market analysis report generated.": "市场分析报告已生成。",
  "Sentiment analysis report generated.": "情绪分析报告已生成。",
  "News analysis report generated.": "新闻分析报告已生成。",
  "Fundamentals report generated.": "基本面报告已生成。",
  "Bull and bear researchers are debating the thesis.": "多空研究员正在进行观点辩论。",
  "Research manager finalized the investment debate.":
    "研究经理已总结投资辩论结论。",
  "Trader synthesized an investment plan.": "交易员已整理投资计划。",
  "Risky analyst updated the risk discussion.": "激进风格分析师更新了风险观点。",
  "Safe analyst updated the risk discussion.": "稳健风格分析师更新了风险观点。",
  "Neutral analyst updated the risk discussion.": "中性风格分析师更新了风险观点。",
  "Risk review started.": "风险评审已开始。",
  "Portfolio manager completed the risk review.": "组合经理已完成风险评审。",
  "Final trade decision is ready.": "最终交易结论已生成。",
  "Scan created and waiting to start.": "隔夜扫描已创建，等待启动。",
  "Scan queued. Runner process started.": "隔夜扫描已进入队列，后台执行器已启动。",
  "Initializing overnight market scanner.": "正在初始化隔夜扫描器。",
  "Overnight scan completed successfully.": "隔夜扫描已完成。",
  "Overnight scan failed during execution.": "隔夜扫描执行失败。",
  "Failed to start overnight scan runner.": "隔夜扫描执行器启动失败。",
  "Review created and waiting to start.": "历史验证已创建，等待启动。",
  "Review queued. Runner process started.": "历史验证已进入队列，后台执行器已启动。",
  "Initializing overnight history review.": "正在初始化隔夜历史验证。",
  "Overnight review completed successfully.": "隔夜历史验证已完成。",
  "Overnight review failed during execution.": "隔夜历史验证执行失败。",
  "Failed to start overnight review runner.": "隔夜历史验证执行器启动失败。",
  "Loading replay universe from the current active A-share pool.":
    "正在从当前活跃的 A 股股票池构建回放宇宙。",
  "Replaying historical overnight scans.": "正在回放历史隔夜扫描。"
};

export function getBadgeLabel(value: string | null, variant: BadgeVariant): string {
  if (!value) {
    if (variant === "decision") {
      return "待生成";
    }
    if (variant === "stage") {
      return "待开始";
    }
    return "未知";
  }

  if (variant === "status") {
    return STATUS_LABELS[value as TaskStatus] ?? value;
  }
  if (variant === "stage") {
    return STAGE_LABELS[value as TaskStage] ?? value;
  }
  return DECISION_LABELS[value] ?? value;
}

export function getProgressMessageLabel(message: string | null | undefined): string {
  if (!message) {
    return "";
  }
  if (message.startsWith("Loading historical bars for replay universe")) {
    return "\u6b63\u5728\u52a0\u8f7d\u56de\u653e\u80a1\u7968\u6c60\u7684\u5386\u53f2K\u7ebf\u6570\u636e\u3002";
  }
  if (message.startsWith("Replaying overnight scan for ")) {
    return "\u6b63\u5728\u9010\u65e5\u56de\u653e\u5386\u53f2\u9694\u591c\u626b\u63cf\u3002";
  }
  return PROGRESS_MESSAGE_LABELS[message] ?? message;
}

export function getProviderLabel(value: string): string {
  return PROVIDER_LABELS[value] ?? value;
}

export function getMarketLabel(value: "cn_a" | "us"): string {
  return value === "cn_a" ? "A股" : "美股";
}

export function getOvernightModeLabel(value: OvernightMode): string {
  return MODE_LABELS[value] ?? value;
}

export function getOvernightQualityLabel(value: OvernightQuality): string {
  return QUALITY_LABELS[value] ?? value;
}
