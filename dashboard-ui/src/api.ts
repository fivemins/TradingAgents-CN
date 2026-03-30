import type {
  CreateOvernightTrackedTradeRequest,
  CreateOvernightReviewRequest,
  CreateOvernightScanRequest,
  CreateTaskRequest,
  CreateTaskSource,
  OvernightTrackedTrade,
  OvernightTrackedTradeListResponse,
  OvernightReviewArtifactsResponse,
  OvernightReviewDetail,
  OvernightReviewListResponse,
  OvernightScanArtifactsResponse,
  OvernightScanDetail,
  OvernightScanListResponse,
  SystemReadinessResponse,
  TaskArtifactsResponse,
  TaskDetail,
  TaskListResponse,
  TaskOptionsResponse
} from "./types";
import {
  normalizeOvernightTrackedTrade,
  normalizeOvernightTrackedTradeListResponse,
  normalizeOvernightReviewArtifacts,
  normalizeOvernightReviewDetail,
  normalizeOvernightReviewListResponse,
  normalizeOvernightScanArtifacts,
  normalizeOvernightScanDetail,
  normalizeOvernightScanListResponse,
  normalizeSystemReadinessResponse,
  normalizeTaskArtifacts,
  normalizeTaskDetail,
  normalizeTaskListResponse
} from "./utils/normalize";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!response.ok) {
    const message = await response.text();
    if (message) {
      try {
        const parsed = JSON.parse(message) as { detail?: string };
        if (parsed.detail) {
          throw new Error(parsed.detail);
        }
      } catch {
        // Fall through to the raw message when the payload is not JSON.
      }
    }
    throw new Error(message || `Request failed with status ${response.status}`);
  }

  return (await response.json()) as T;
}

function buildTaskQuery(source?: CreateTaskSource): string {
  if (!source) {
    return "";
  }
  const search = new URLSearchParams({
    source_type: source.type,
    source_scan_id: source.scanId,
    source_trade_date: source.tradeDate,
    source_mode: source.mode
  });
  if (source.name) {
    search.set("source_name", source.name);
  }
  return `?${search.toString()}`;
}

export const dashboardApi = {
  getSystemReadiness(refresh = false): Promise<SystemReadinessResponse> {
    const suffix = refresh ? "?refresh=true" : "";
    return apiFetch<SystemReadinessResponse>(`/api/system/readiness${suffix}`).then(
      normalizeSystemReadinessResponse
    );
  },
  getOptions(): Promise<TaskOptionsResponse> {
    return apiFetch<TaskOptionsResponse>("/api/options");
  },
  getTasks(sourceType?: string): Promise<TaskListResponse> {
    const search = new URLSearchParams();
    if (sourceType) {
      search.set("source_type", sourceType);
    }
    const suffix = search.size ? `?${search.toString()}` : "";
    return apiFetch<TaskListResponse>(`/api/tasks${suffix}`).then(normalizeTaskListResponse);
  },
  getTask(taskId: string): Promise<TaskDetail> {
    return apiFetch<TaskDetail>(`/api/tasks/${taskId}`).then(normalizeTaskDetail);
  },
  getArtifacts(taskId: string): Promise<TaskArtifactsResponse> {
    return apiFetch<TaskArtifactsResponse>(`/api/tasks/${taskId}/artifacts`).then(
      normalizeTaskArtifacts
    );
  },
  terminateTask(taskId: string): Promise<TaskDetail> {
    return apiFetch<TaskDetail>(`/api/tasks/${taskId}/terminate`, {
      method: "POST"
    }).then(normalizeTaskDetail);
  },
  deleteTask(taskId: string): Promise<{ ok: boolean }> {
    return apiFetch<{ ok: boolean }>(`/api/tasks/${taskId}`, {
      method: "DELETE"
    });
  },
  createTask(payload: CreateTaskRequest, source?: CreateTaskSource): Promise<TaskDetail> {
    return apiFetch<TaskDetail>(`/api/tasks${buildTaskQuery(source)}`, {
      method: "POST",
      body: JSON.stringify(payload)
    }).then(normalizeTaskDetail);
  },
  getOvernightScans(): Promise<OvernightScanListResponse> {
    return apiFetch<OvernightScanListResponse>("/api/overnight/scans").then(
      normalizeOvernightScanListResponse
    );
  },
  getOvernightScan(scanId: string): Promise<OvernightScanDetail> {
    return apiFetch<OvernightScanDetail>(`/api/overnight/scans/${scanId}`).then(
      normalizeOvernightScanDetail
    );
  },
  deleteOvernightScan(scanId: string): Promise<{ ok: boolean }> {
    return apiFetch<{ ok: boolean }>(`/api/overnight/scans/${scanId}`, {
      method: "DELETE"
    });
  },
  getOvernightArtifacts(scanId: string): Promise<OvernightScanArtifactsResponse> {
    return apiFetch<OvernightScanArtifactsResponse>(
      `/api/overnight/scans/${scanId}/artifacts`
    ).then(normalizeOvernightScanArtifacts);
  },
  createOvernightScan(payload: CreateOvernightScanRequest): Promise<OvernightScanDetail> {
    return apiFetch<OvernightScanDetail>("/api/overnight/scans", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then(normalizeOvernightScanDetail);
  },
  validateOvernightScan(scanId: string): Promise<OvernightScanDetail> {
    return apiFetch<OvernightScanDetail>(`/api/overnight/scans/${scanId}/validate`, {
      method: "POST"
    }).then(normalizeOvernightScanDetail);
  },
  getOvernightTrackedTrades(): Promise<OvernightTrackedTradeListResponse> {
    return apiFetch<OvernightTrackedTradeListResponse>("/api/overnight/trades").then(
      normalizeOvernightTrackedTradeListResponse
    );
  },
  getOvernightTrackedTrade(tradeId: string): Promise<OvernightTrackedTrade> {
    return apiFetch<OvernightTrackedTrade>(`/api/overnight/trades/${tradeId}`).then(
      normalizeOvernightTrackedTrade
    );
  },
  createOvernightTrackedTrade(payload: CreateOvernightTrackedTradeRequest): Promise<OvernightTrackedTrade> {
    return apiFetch<OvernightTrackedTrade>("/api/overnight/trades", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then(normalizeOvernightTrackedTrade);
  },
  refreshPendingOvernightTrackedTrades(): Promise<OvernightTrackedTradeListResponse> {
    return apiFetch<OvernightTrackedTradeListResponse>("/api/overnight/trades/refresh-pending", {
      method: "POST"
    }).then(normalizeOvernightTrackedTradeListResponse);
  },
  deleteOvernightTrackedTrade(tradeId: string): Promise<{ ok: boolean }> {
    return apiFetch<{ ok: boolean }>(`/api/overnight/trades/${tradeId}`, {
      method: "DELETE"
    });
  },
  getOvernightReviews(): Promise<OvernightReviewListResponse> {
    return apiFetch<OvernightReviewListResponse>("/api/overnight/reviews").then(
      normalizeOvernightReviewListResponse
    );
  },
  getOvernightReview(reviewId: string): Promise<OvernightReviewDetail> {
    return apiFetch<OvernightReviewDetail>(`/api/overnight/reviews/${reviewId}`).then(
      normalizeOvernightReviewDetail
    );
  },
  deleteOvernightReview(reviewId: string): Promise<{ ok: boolean }> {
    return apiFetch<{ ok: boolean }>(`/api/overnight/reviews/${reviewId}`, {
      method: "DELETE"
    });
  },
  getOvernightReviewArtifacts(reviewId: string): Promise<OvernightReviewArtifactsResponse> {
    return apiFetch<OvernightReviewArtifactsResponse>(
      `/api/overnight/reviews/${reviewId}/artifacts`
    ).then(normalizeOvernightReviewArtifacts);
  },
  createOvernightReview(payload: CreateOvernightReviewRequest): Promise<OvernightReviewDetail> {
    return apiFetch<OvernightReviewDetail>("/api/overnight/reviews", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then(normalizeOvernightReviewDetail);
  }
};

export function resolveDownloadUrl(path: string): string {
  return `${API_BASE}${path}`;
}
