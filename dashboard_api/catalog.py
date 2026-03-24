from __future__ import annotations

from typing import Any

from tradingagents.default_config import DEFAULT_CONFIG


ANALYST_OPTIONS = [
    {"value": "market", "label": "Market Analyst"},
    {"value": "social", "label": "Social Media Analyst"},
    {"value": "news", "label": "News Analyst"},
    {"value": "fundamentals", "label": "Fundamentals Analyst"},
]

MARKET_REGION_OPTIONS = [
    {
        "value": "cn_a",
        "label": "CN A-share",
        "description": "Prioritize Akshare-friendly A-share data, company events, and broker research.",
        "example": "000001, 600519, 300750",
    },
    {
        "value": "us",
        "label": "US stock",
        "description": "Use Yahoo Finance-first market data and the existing US-oriented research flow.",
        "example": "NVDA, AAPL, TSLA",
    },
]

RESEARCH_DEPTH_OPTIONS = [
    {
        "value": 1,
        "label": "Shallow",
        "description": "Quick research with one debate and one risk discussion round.",
    },
    {
        "value": 3,
        "label": "Medium",
        "description": "Balanced research depth with moderate discussion rounds.",
    },
    {
        "value": 5,
        "label": "Deep",
        "description": "Comprehensive research with more debate and synthesis.",
    },
]

PROVIDER_OPTIONS = [
    {"value": "openai", "label": "OpenAI", "base_url": "https://api.openai.com/v1"},
    {
        "value": "anthropic",
        "label": "Anthropic",
        "base_url": "https://api.anthropic.com/",
    },
    {
        "value": "google",
        "label": "Google",
        "base_url": "https://generativelanguage.googleapis.com/v1",
    },
    {
        "value": "ark",
        "label": "Ark",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
    },
    {
        "value": "openrouter",
        "label": "OpenRouter",
        "base_url": "https://openrouter.ai/api/v1",
    },
    {
        "value": "ollama",
        "label": "Ollama",
        "base_url": "http://localhost:11434/v1",
    },
]

QUICK_MODEL_OPTIONS = {
    "openai": [
        {"label": "GPT-4o Mini", "value": "gpt-4o-mini"},
        {"label": "GPT-4.1 Nano", "value": "gpt-4.1-nano"},
        {"label": "GPT-4.1 Mini", "value": "gpt-4.1-mini"},
        {"label": "GPT-4o", "value": "gpt-4o"},
    ],
    "anthropic": [
        {"label": "Claude 3.5 Haiku", "value": "claude-3-5-haiku-latest"},
        {"label": "Claude 3.7 Sonnet", "value": "claude-3-7-sonnet-latest"},
    ],
    "google": [
        {"label": "Gemini 2.0 Flash", "value": "gemini-2.0-flash"},
        {"label": "Gemini 2.5 Flash", "value": "gemini-2.5-flash-preview-05-20"},
    ],
    "ark": [
        {"label": "Doubao Seed 2.0 Lite", "value": "doubao-seed-2.0-lite"},
        {"label": "Doubao Seed Code", "value": "doubao-seed-code"},
        {"label": "Doubao Seed 2.0 Code", "value": "doubao-seed-2.0-code"},
        {"label": "Doubao Seed 2.0 Pro", "value": "doubao-seed-2.0-pro"},
        {"label": "Kimi K2.5", "value": "kimi-k2.5"},
        {"label": "DeepSeek V3.2", "value": "deepseek-v3.2"},
        {"label": "GLM 4.7", "value": "glm-4.7"},
        {"label": "MiniMax M2.5", "value": "minimax-m2.5"},
    ],
    "openrouter": [
        {"label": "Llama 4 Scout", "value": "meta-llama/llama-4-scout:free"},
        {
            "label": "Llama 3.3 8B Instruct",
            "value": "meta-llama/llama-3.3-8b-instruct:free",
        },
        {
            "label": "Gemini 2.0 Flash Experimental",
            "value": "google/gemini-2.0-flash-exp:free",
        },
    ],
    "ollama": [
        {"label": "llama3.1", "value": "llama3.1"},
        {"label": "qwen3", "value": "qwen3"},
    ],
}

DEEP_MODEL_OPTIONS = {
    "openai": [
        {"label": "o4-mini", "value": "o4-mini"},
        {"label": "GPT-4.1 Mini", "value": "gpt-4.1-mini"},
        {"label": "GPT-4.1", "value": "gpt-4.1"},
        {"label": "GPT-4o", "value": "gpt-4o"},
    ],
    "anthropic": [
        {"label": "Claude 3.7 Sonnet", "value": "claude-3-7-sonnet-latest"},
        {"label": "Claude 3.5 Sonnet", "value": "claude-3-5-sonnet-latest"},
    ],
    "google": [
        {"label": "Gemini 2.5 Flash", "value": "gemini-2.5-flash-preview-05-20"},
        {"label": "Gemini 2.5 Pro", "value": "gemini-2.5-pro-preview-06-05"},
    ],
    "ark": [
        {"label": "Doubao Seed 2.0 Pro", "value": "doubao-seed-2.0-pro"},
        {"label": "Kimi K2.5", "value": "kimi-k2.5"},
        {"label": "DeepSeek V3.2", "value": "deepseek-v3.2"},
        {"label": "Doubao Seed Code", "value": "doubao-seed-code"},
        {"label": "Doubao Seed 2.0 Code", "value": "doubao-seed-2.0-code"},
        {"label": "GLM 4.7", "value": "glm-4.7"},
        {"label": "Doubao Seed 2.0 Lite", "value": "doubao-seed-2.0-lite"},
        {"label": "MiniMax M2.5", "value": "minimax-m2.5"},
    ],
    "openrouter": [
        {
            "label": "DeepSeek Chat V3",
            "value": "deepseek/deepseek-chat-v3-0324:free",
        },
        {"label": "Llama 4 Maverick", "value": "meta-llama/llama-4-maverick:free"},
    ],
    "ollama": [
        {"label": "qwen3", "value": "qwen3"},
        {"label": "llama3.1", "value": "llama3.1"},
    ],
}


def get_provider_base_url(provider: str) -> str:
    normalized = provider.lower()
    for option in PROVIDER_OPTIONS:
        if option["value"] == normalized:
            return option["base_url"]
    raise KeyError(f"Unsupported provider: {provider}")


def is_valid_model(provider: str, quick_model: str, deep_model: str) -> bool:
    provider = provider.lower()
    quick_values = {item["value"] for item in QUICK_MODEL_OPTIONS.get(provider, [])}
    deep_values = {item["value"] for item in DEEP_MODEL_OPTIONS.get(provider, [])}
    return quick_model in quick_values and deep_model in deep_values


def build_options_payload() -> dict[str, Any]:
    provider = DEFAULT_CONFIG["llm_provider"].lower()
    return {
        "analysts": ANALYST_OPTIONS,
        "market_regions": MARKET_REGION_OPTIONS,
        "research_depths": RESEARCH_DEPTH_OPTIONS,
        "providers": PROVIDER_OPTIONS,
        "model_options": {
            key: {
                "quick": value,
                "deep": DEEP_MODEL_OPTIONS[key],
            }
            for key, value in QUICK_MODEL_OPTIONS.items()
        },
        "defaults": {
            "ticker": "600519",
            "analysis_date": "",
            "market_region": DEFAULT_CONFIG.get("market_region", "cn_a"),
            "analysts": [item["value"] for item in ANALYST_OPTIONS],
            "research_depth": int(DEFAULT_CONFIG["max_debate_rounds"]),
            "llm_provider": provider,
            "quick_think_llm": str(DEFAULT_CONFIG["quick_think_llm"]),
            "deep_think_llm": str(DEFAULT_CONFIG["deep_think_llm"]),
            "online_tools": bool(DEFAULT_CONFIG["online_tools"]),
        },
    }
