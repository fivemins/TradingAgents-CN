from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests

from .auth import get_qveris_api_keys


DEFAULT_BASE_URL = "https://qveris.ai/api/v1"
ROTATABLE_STATUS_CODES = {401, 403, 429}


class QVerisClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_text: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class QVerisConfigurationError(QVerisClientError):
    pass


class QVerisAuthError(QVerisClientError):
    pass


class QVerisRateLimitError(QVerisClientError):
    pass


class QVerisToolUnavailableError(QVerisClientError):
    pass


@dataclass(frozen=True)
class QVerisResponseMeta:
    attempts: int
    active_key_index: int


class QVerisClient:
    def __init__(
        self,
        *,
        api_keys: list[str] | None = None,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
    ) -> None:
        self.api_keys = api_keys or get_qveris_api_keys()
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self._last_meta = QVerisResponseMeta(attempts=0, active_key_index=0)

    @property
    def last_meta(self) -> QVerisResponseMeta:
        return self._last_meta

    def discover_tools(
        self,
        query: str,
        *,
        limit: int = 10,
        timeout_ms: int = 30_000,
    ) -> dict[str, Any]:
        return self._request_json(
            "/search",
            body={"query": query, "limit": limit},
            timeout_ms=timeout_ms,
        )

    def inspect_tools(
        self,
        tool_ids: list[str],
        *,
        discovery_id: str | None = None,
        timeout_ms: int = 30_000,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"tool_ids": tool_ids}
        if discovery_id:
            body["search_id"] = discovery_id
        return self._request_json(
            "/tools/by-ids",
            body=body,
            timeout_ms=timeout_ms,
        )

    def call_tool(
        self,
        tool_id: str,
        discovery_id: str,
        parameters: dict[str, Any],
        *,
        max_response_size: int = 20_480,
        timeout_ms: int = 120_000,
    ) -> dict[str, Any]:
        return self._request_json(
            "/tools/execute",
            query={"tool_id": tool_id},
            body={
                "search_id": discovery_id,
                "parameters": parameters,
                "max_response_size": max_response_size,
            },
            timeout_ms=timeout_ms,
        )

    def _request_json(
        self,
        path: str,
        *,
        method: str = "POST",
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        timeout_ms: int = 30_000,
    ) -> dict[str, Any]:
        if not self.api_keys:
            raise QVerisConfigurationError("QVeris API key is not configured.")

        last_error: QVerisClientError | None = None
        for index, api_key in enumerate(self.api_keys):
            try:
                response = self.session.request(
                    method=method,
                    url=f"{self.base_url}{path}",
                    params=query,
                    json=body,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    timeout=max(timeout_ms / 1000.0, 1.0),
                )
            except requests.RequestException as exc:
                raise QVerisClientError(f"QVeris request failed: {exc}") from exc

            if response.ok:
                self._last_meta = QVerisResponseMeta(
                    attempts=index + 1,
                    active_key_index=index,
                )
                try:
                    return response.json()
                except json.JSONDecodeError as exc:
                    raise QVerisClientError(
                        "QVeris returned invalid JSON.",
                        status_code=response.status_code,
                        response_text=response.text,
                    ) from exc

            error = self._build_error(response.status_code, response.text)
            if (
                response.status_code in ROTATABLE_STATUS_CODES
                and index + 1 < len(self.api_keys)
            ):
                last_error = error
                continue
            raise error

        if last_error:
            raise last_error
        raise QVerisClientError("QVeris request failed without a response.")

    @staticmethod
    def _build_error(status_code: int, response_text: str) -> QVerisClientError:
        lowered = response_text.lower()
        if status_code in {401, 403}:
            return QVerisAuthError(
                f"QVeris auth failed: HTTP {status_code}",
                status_code=status_code,
                response_text=response_text,
            )
        if status_code == 429 or "credit" in lowered or "quota" in lowered:
            return QVerisRateLimitError(
                f"QVeris rate limit reached: HTTP {status_code}",
                status_code=status_code,
                response_text=response_text,
            )
        if "tool" in lowered and ("unavailable" in lowered or "not found" in lowered):
            return QVerisToolUnavailableError(
                f"QVeris tool is unavailable: HTTP {status_code}",
                status_code=status_code,
                response_text=response_text,
            )
        return QVerisClientError(
            f"QVeris request failed: HTTP {status_code}",
            status_code=status_code,
            response_text=response_text,
        )
