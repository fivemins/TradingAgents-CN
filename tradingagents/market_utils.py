from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import os
import re
from typing import Any, Callable, Literal


MarketRegion = Literal["cn_a", "us"]

_PROXY_ENV_KEYS = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
]


@dataclass(frozen=True)
class SecurityProfile:
    user_ticker: str
    market_region: MarketRegion
    normalized_ticker: str
    exchange: str
    yfinance_symbol: str
    akshare_symbol: str
    eastmoney_symbol: str
    display_symbol: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def infer_market_region(ticker: str, market_region: str | None = None) -> MarketRegion:
    normalized_region = (market_region or "").strip().lower()
    if normalized_region in {"cn_a", "us"}:
        return normalized_region  # type: ignore[return-value]

    normalized_ticker = ticker.strip().upper()
    if re.fullmatch(r"\d{6}(\.(SZ|SS|SH))?", normalized_ticker):
        return "cn_a"
    return "us"


def normalize_ticker(ticker: str, market_region: str | None = None) -> str:
    normalized = ticker.strip().upper()
    region = infer_market_region(normalized, market_region)
    if region != "cn_a":
        return normalized

    if re.fullmatch(r"\d{6}\.(SZ|SS|SH)", normalized):
        if normalized.endswith(".SH"):
            return normalized[:-3] + ".SS"
        return normalized

    if not re.fullmatch(r"\d{6}", normalized):
        return normalized

    if normalized.startswith(("5", "6", "9")):
        return f"{normalized}.SS"
    return f"{normalized}.SZ"


def build_security_profile(
    ticker: str,
    market_region: str | None = None,
) -> SecurityProfile:
    normalized_ticker = normalize_ticker(ticker, market_region)
    resolved_region = infer_market_region(normalized_ticker, market_region)

    if resolved_region == "cn_a":
        code = normalized_ticker.split(".", 1)[0]
        suffix = normalized_ticker.split(".", 1)[1] if "." in normalized_ticker else ""
        exchange = "SSE" if suffix == "SS" or code.startswith(("5", "6", "9")) else "SZSE"
        eastmoney_suffix = "SH" if exchange == "SSE" else "SZ"
        return SecurityProfile(
            user_ticker=ticker.strip().upper(),
            market_region="cn_a",
            normalized_ticker=normalized_ticker,
            exchange=exchange,
            yfinance_symbol=normalized_ticker,
            akshare_symbol=code,
            eastmoney_symbol=f"{code}.{eastmoney_suffix}",
            display_symbol=code,
        )

    return SecurityProfile(
        user_ticker=ticker.strip().upper(),
        market_region="us",
        normalized_ticker=normalized_ticker,
        exchange="US",
        yfinance_symbol=normalized_ticker,
        akshare_symbol="",
        eastmoney_symbol="",
        display_symbol=normalized_ticker,
    )


@contextmanager
def cleared_http_proxies():
    saved: dict[str, str] = {}
    for key in _PROXY_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ.pop(key)

    previous_no_proxy = os.environ.get("NO_PROXY")
    local_hosts = "127.0.0.1,localhost"
    os.environ["NO_PROXY"] = (
        f"{previous_no_proxy},{local_hosts}" if previous_no_proxy else local_hosts
    )

    try:
        yield
    finally:
        if previous_no_proxy is None:
            os.environ.pop("NO_PROXY", None)
        else:
            os.environ["NO_PROXY"] = previous_no_proxy
        for key, value in saved.items():
            os.environ[key] = value


def call_with_proxy_fallback(
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    try:
        return func(*args, **kwargs)
    except Exception as first_error:
        with cleared_http_proxies():
            try:
                return func(*args, **kwargs)
            except Exception:
                raise first_error


def call_with_proxy_or_empty_fallback(
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    try:
        result = func(*args, **kwargs)
        if _result_has_data(result):
            return result
        with cleared_http_proxies():
            retry_result = func(*args, **kwargs)
        return retry_result if _result_has_data(retry_result) else result
    except Exception as first_error:
        with cleared_http_proxies():
            try:
                return func(*args, **kwargs)
            except Exception:
                raise first_error


def _result_has_data(value: Any) -> bool:
    if value is None:
        return False
    empty = getattr(value, "empty", None)
    if isinstance(empty, bool):
        return not empty
    return True
