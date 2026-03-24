import os
from urllib.parse import urlparse


def _is_local_base_url(base_url: str) -> bool:
    try:
        host = urlparse(base_url).hostname
    except ValueError:
        return False
    return host in {"127.0.0.1", "localhost", "0.0.0.0"}


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


DEFAULT_LLM_BASE_URL = os.getenv(
    "TRADINGAGENTS_LLM_BASE_URL",
    os.getenv("TRADINGAGENTS_BACKEND_URL", "https://api.openai.com/v1"),
)

DEFAULT_LLM_API_KEY = (
    os.getenv("TRADINGAGENTS_LLM_API_KEY")
    or os.getenv("ARK_API_KEY")
    or os.getenv("OPENAI_API_KEY")
    or ("local-placeholder-key" if _is_local_base_url(DEFAULT_LLM_BASE_URL) else "")
)

DEFAULT_EMBEDDING_BASE_URL = os.getenv("TRADINGAGENTS_EMBEDDING_BASE_URL", "")
if not DEFAULT_EMBEDDING_BASE_URL and (
    DEFAULT_LLM_BASE_URL == "https://api.openai.com/v1"
    or _is_local_base_url(DEFAULT_LLM_BASE_URL)
):
    DEFAULT_EMBEDDING_BASE_URL = DEFAULT_LLM_BASE_URL

DEFAULT_EMBEDDING_API_KEY = (
    os.getenv("TRADINGAGENTS_EMBEDDING_API_KEY")
    or (
        DEFAULT_LLM_API_KEY
        if DEFAULT_EMBEDDING_BASE_URL == DEFAULT_LLM_BASE_URL
        else ""
    )
    or (
        "local-placeholder-key"
        if DEFAULT_EMBEDDING_BASE_URL
        and _is_local_base_url(DEFAULT_EMBEDDING_BASE_URL)
        else ""
    )
)

DEFAULT_EMBEDDING_MODEL = os.getenv("TRADINGAGENTS_EMBEDDING_MODEL", "")
if not DEFAULT_EMBEDDING_MODEL:
    if DEFAULT_EMBEDDING_BASE_URL == "https://api.openai.com/v1":
        DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
    elif DEFAULT_EMBEDDING_BASE_URL and _is_local_base_url(
        DEFAULT_EMBEDDING_BASE_URL
    ):
        DEFAULT_EMBEDDING_MODEL = "nomic-embed-text"

DEFAULT_CONFIG = {
    "project_dir": os.path.abspath(os.path.join(os.path.dirname(__file__), ".")),
    "results_dir": os.getenv("TRADINGAGENTS_RESULTS_DIR", "./results"),
    "data_dir": os.getenv(
        "TRADINGAGENTS_DATA_DIR", "/Users/yluo/Documents/Code/ScAI/FR1-data"
    ),
    "data_cache_dir": os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), ".")),
        "dataflows/data_cache",
    ),
    # LLM settings
    "llm_provider": os.getenv("TRADINGAGENTS_LLM_PROVIDER", "openai"),
    "deep_think_llm": os.getenv("TRADINGAGENTS_DEEP_LLM", "o4-mini"),
    "quick_think_llm": os.getenv("TRADINGAGENTS_QUICK_LLM", "gpt-4o-mini"),
    # Keep backend_url for backward compatibility with older call sites.
    "backend_url": DEFAULT_LLM_BASE_URL,
    "llm_base_url": DEFAULT_LLM_BASE_URL,
    "llm_api_key": DEFAULT_LLM_API_KEY,
    "embedding_base_url": DEFAULT_EMBEDDING_BASE_URL,
    "embedding_api_key": DEFAULT_EMBEDDING_API_KEY,
    "embedding_model": DEFAULT_EMBEDDING_MODEL,
    "enable_memory": _env_bool("TRADINGAGENTS_ENABLE_MEMORY", True),
    "market_region": os.getenv("TRADINGAGENTS_MARKET_REGION", "cn_a").strip().lower(),
    # Debate and discussion settings
    "max_debate_rounds": 1,
    "max_risk_discuss_rounds": 1,
    "max_recur_limit": 100,
    # Tool settings
    "online_tools": _env_bool("TRADINGAGENTS_ONLINE_TOOLS", True),
}
