import { StatusBadge } from "./StatusBadge";
import { getOvernightExecutionState, type OvernightExecutionKind } from "../overnightProgress";
import type { TaskStatus } from "../types";

interface OvernightExecutionStatusProps {
  title: string;
  kind: OvernightExecutionKind;
  status: TaskStatus | null | undefined;
  progressMessage: string | null | undefined;
  startedAt?: string | null;
  finishedAt?: string | null;
  errorMessage?: string | null;
  compact?: boolean;
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) {
    return "--";
  }
  return value.replace("T", " ").replace("Z", "");
}

export function OvernightExecutionStatus({
  title,
  kind,
  status,
  progressMessage,
  startedAt,
  finishedAt,
  errorMessage,
  compact = false,
}: OvernightExecutionStatusProps) {
  const progress = getOvernightExecutionState(kind, status, progressMessage);

  return (
    <>
      <div className="decision-head">
        <strong>{title}</strong>
        <StatusBadge value={status ?? null} />
      </div>
      <div className="overnight-progress-meta">
        <span className="analysis-badge">{progress.currentStepLabel}</span>
        {status === "running" ? (
          <span className="overnight-live-pill">
            {"\u81ea\u52a8\u5237\u65b0\u4e2d"}
          </span>
        ) : null}
      </div>
      <p className="card-note overnight-progress-copy">{progress.detailLabel}</p>

      {!compact ? (
        <>
          <div className="stage-strip overnight-stage-strip">
            {progress.steps.map((step, index) => (
              <div
                key={`${kind}:${step.key}`}
                className={
                  index <= progress.activeStepIndex
                    ? "stage-chip stage-chip-active"
                    : "stage-chip"
                }
              >
                {step.label}
              </div>
            ))}
          </div>

          <div className="analysis-estimate-list overnight-progress-times">
            <div className="analysis-estimate-row">
              <span>{"\u5f00\u59cb\u65f6\u95f4"}</span>
              <strong>{formatTimestamp(startedAt)}</strong>
            </div>
            <div className="analysis-estimate-row">
              <span>{"\u7ed3\u675f\u65f6\u95f4"}</span>
              <strong>{formatTimestamp(finishedAt)}</strong>
            </div>
          </div>
        </>
      ) : null}

      {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}
    </>
  );
}
