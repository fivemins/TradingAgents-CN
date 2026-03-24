import type { TaskStage, TaskStatus } from "../types";
import { getBadgeLabel } from "../labels";

interface StatusBadgeProps {
  value: TaskStatus | TaskStage | string | null;
  variant?: "status" | "decision" | "stage";
}

const CLASS_BY_VALUE: Record<string, string> = {
  queued: "badge badge-queued",
  running: "badge badge-running",
  succeeded: "badge badge-succeeded",
  failed: "badge badge-failed",
  BUY: "badge badge-buy",
  HOLD: "badge badge-hold",
  SELL: "badge badge-sell"
};

export function StatusBadge({ value, variant = "status" }: StatusBadgeProps) {
  const rawValue = value ?? null;
  const text = getBadgeLabel(rawValue, variant);
  const fallbackClass =
    variant === "decision" ? "badge badge-decision" : "badge badge-neutral";
  const className = CLASS_BY_VALUE[rawValue ?? ""] ?? fallbackClass;

  return (
    <span className={className} title={rawValue ?? undefined}>
      {text}
    </span>
  );
}
