import { getProgressMessageLabel } from "./labels";
import type { TaskStatus } from "./types";

export type OvernightExecutionKind = "scan" | "review";

interface OvernightExecutionStep {
  key: string;
  label: string;
}

export interface OvernightExecutionState {
  steps: readonly OvernightExecutionStep[];
  activeStepIndex: number;
  currentStepLabel: string;
  detailLabel: string;
}

const SCAN_STEPS = [
  { key: "queued", label: "\u7b49\u5f85\u542f\u52a8" },
  { key: "market", label: "\u8bc4\u4f30\u5e02\u573a" },
  { key: "universe", label: "\u6784\u5efa\u80a1\u7968\u6c60" },
  { key: "risk", label: "\u98ce\u9669\u8fc7\u6ee4" },
  { key: "enrichment", label: "\u8865\u5145\u884c\u60c5" },
  { key: "scoring", label: "\u8bc4\u5206\u5206\u5c42" },
  { key: "completed", label: "\u626b\u63cf\u5b8c\u6210" }
] as const satisfies readonly OvernightExecutionStep[];

const REVIEW_STEPS = [
  { key: "queued", label: "\u7b49\u5f85\u542f\u52a8" },
  { key: "initializing", label: "\u51c6\u5907\u56de\u653e" },
  { key: "universe", label: "\u52a0\u8f7d\u80a1\u6c60\u5feb\u7167" },
  { key: "history", label: "\u52a0\u8f7d\u5386\u53f2K\u7ebf" },
  { key: "replay", label: "\u9010\u65e5\u56de\u653e" },
  { key: "completed", label: "\u9a8c\u8bc1\u5b8c\u6210" }
] as const satisfies readonly OvernightExecutionStep[];

function normalizeMessage(message: string | null | undefined): string {
  return message?.trim() ?? "";
}

function deriveScanStepIndex(status: TaskStatus, message: string): number {
  if (status === "queued") {
    return 0;
  }
  if (status === "succeeded") {
    return SCAN_STEPS.length - 1;
  }

  if (
    message.includes("\u8bc4\u4f30\u5e02\u573a") ||
    message === "Initializing overnight market scanner."
  ) {
    return 1;
  }
  if (message.includes("A \u80a1\u52a8\u6001\u80a1\u7968\u6c60")) {
    return 2;
  }
  if (message.includes("\u98ce\u9669\u4e8b\u4ef6\u8fc7\u6ee4")) {
    return 3;
  }
  if (message.includes("\u65e5\u7ebf") || message.includes("\u5c3e\u76d8\u5206\u65f6")) {
    return 4;
  }
  if (
    message.includes("\u7efc\u5408\u8bc4\u5206") ||
    message.includes("\u7ed3\u679c\u5206\u5c42")
  ) {
    return 5;
  }

  return status === "running" || status === "failed" ? 1 : 0;
}

function deriveReviewStepIndex(status: TaskStatus, message: string): number {
  if (status === "queued") {
    return 0;
  }
  if (status === "succeeded") {
    return REVIEW_STEPS.length - 1;
  }

  if (message === "Initializing overnight history review.") {
    return 1;
  }
  if (
    message === "Loading replay universe snapshots." ||
    message === "Loading replay universe from the current active A-share pool."
  ) {
    return 2;
  }
  if (message.startsWith("Loading historical bars for replay universe")) {
    return 3;
  }
  if (
    message === "Replaying historical overnight scans." ||
    message.startsWith("Replaying overnight scan for ")
  ) {
    return 4;
  }

  return status === "running" || status === "failed" ? 1 : 0;
}

export function getOvernightExecutionState(
  kind: OvernightExecutionKind,
  status: TaskStatus | null | undefined,
  progressMessage: string | null | undefined,
): OvernightExecutionState {
  const resolvedStatus = status ?? "queued";
  const message = normalizeMessage(progressMessage);
  const detailLabel =
    getProgressMessageLabel(message) ||
    (resolvedStatus === "failed"
      ? "\u9694\u591c\u4efb\u52a1\u6267\u884c\u5931\u8d25\u3002"
      : "\u6b63\u5728\u540c\u6b65\u9694\u591c\u4efb\u52a1\u72b6\u6001\u3002");
  const steps = kind === "scan" ? SCAN_STEPS : REVIEW_STEPS;
  const activeStepIndex =
    kind === "scan"
      ? deriveScanStepIndex(resolvedStatus, message)
      : deriveReviewStepIndex(resolvedStatus, message);

  if (resolvedStatus === "failed") {
    return {
      steps,
      activeStepIndex,
      currentStepLabel:
        kind === "scan" ? "\u626b\u63cf\u5931\u8d25" : "\u9a8c\u8bc1\u5931\u8d25",
      detailLabel,
    };
  }

  return {
    steps,
    activeStepIndex,
    currentStepLabel: steps[activeStepIndex]?.label ?? steps[0].label,
    detailLabel,
  };
}
