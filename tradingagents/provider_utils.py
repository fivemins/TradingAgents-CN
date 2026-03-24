from __future__ import annotations

import os
from typing import Any, Dict, List
from urllib.parse import urlparse


OPENAI_COMPATIBLE_PROVIDERS = {"openai", "ollama", "openrouter", "ark"}


def is_local_base_url(base_url: str | None) -> bool:
    if not base_url:
        return False
    try:
        host = urlparse(base_url).hostname
    except ValueError:
        return False
    return host in {"127.0.0.1", "localhost", "0.0.0.0"}


def get_llm_base_url(config: Dict[str, Any]) -> str | None:
    return config.get("llm_base_url") or config.get("backend_url")


def get_embedding_base_url(config: Dict[str, Any]) -> str | None:
    return config.get("embedding_base_url") or ""


def resolve_api_key(
    base_url: str | None, configured_key: str | None, *env_names: str
) -> str | None:
    if configured_key:
        return configured_key
    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value
    if is_local_base_url(base_url):
        return "local-placeholder-key"
    return None


def get_llm_api_key(config: Dict[str, Any]) -> str | None:
    return resolve_api_key(
        get_llm_base_url(config),
        config.get("llm_api_key"),
        "TRADINGAGENTS_LLM_API_KEY",
        "ARK_API_KEY",
        "OPENAI_API_KEY",
    )


def get_embedding_api_key(config: Dict[str, Any]) -> str | None:
    return resolve_api_key(
        get_embedding_base_url(config),
        config.get("embedding_api_key"),
        "TRADINGAGENTS_EMBEDDING_API_KEY",
        "OPENAI_API_KEY",
    )


def get_web_search_tools(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    provider = (config.get("llm_provider") or "").lower()
    if provider == "ark":
        return [{"type": "web_search"}]

    return [
        {
            "type": "web_search_preview",
            "user_location": {"type": "approximate"},
            "search_context_size": "low",
        }
    ]


def extract_responses_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if output_text:
        return output_text

    chunks: List[str] = []
    for item in getattr(response, "output", []) or []:
        content = getattr(item, "content", None)
        if not content:
            continue
        for part in content:
            text = getattr(part, "text", None)
            if text:
                chunks.append(text)
            elif isinstance(part, dict) and part.get("text"):
                chunks.append(part["text"])

    return "\n\n".join(chunks).strip()
