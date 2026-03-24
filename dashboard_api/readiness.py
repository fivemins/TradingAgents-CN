from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen

import akshare as ak

from dashboard_api.settings import DashboardSettings
from tradingagents.market_utils import (
    build_security_profile,
    call_with_proxy_fallback,
    call_with_proxy_or_empty_fallback,
)
from tradingagents.overnight.tail_data import _load_intraday_minute_df
from tradingagents.qveris import (
    CAPABILITY_QUERIES,
    QVerisClient,
    QVerisToolRegistry,
)
from tradingagents.qveris.auth import build_qveris_auth_summary


_CACHE_TTL_SECONDS = 30.0
_READINESS_CACHE: dict[str, Any] = {"timestamp": 0.0, "payload": None}
_LOCAL_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_component(
    name: str,
    ok: bool,
    message: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "name": name,
        "ok": ok,
        "status": "ok" if ok else "error",
        "message": message,
        **extra,
    }


def _probe_http(
    url: str,
    timeout: float = 4.0,
    *,
    bypass_proxy: bool = False,
) -> tuple[bool, str]:
    try:
        request = Request(url, method="GET")
        if bypass_proxy:
            opener = build_opener(ProxyHandler({}))
            response_context = opener.open(request, timeout=timeout)
        else:
            response_context = urlopen(request, timeout=timeout)
        with response_context as response:
            return True, f"HTTP {getattr(response, 'status', 200)}"
    except HTTPError as exc:
        if exc.code < 500:
            return True, f"HTTP {exc.code}"
        return False, f"HTTP {exc.code}"
    except (URLError, OSError, ValueError) as exc:
        return False, str(exc)


def _base_host_probe(base_url: str) -> tuple[bool, str]:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return False, "Invalid base URL."
    probe_url = f"{parsed.scheme}://{parsed.netloc}"
    return _probe_http(probe_url, bypass_proxy=parsed.hostname in _LOCAL_HOSTS)


def _check_ark() -> dict[str, Any]:
    provider = (os.getenv("TRADINGAGENTS_LLM_PROVIDER") or "").lower()
    base_url = os.getenv("TRADINGAGENTS_LLM_BASE_URL") or ""
    api_key = (
        os.getenv("TRADINGAGENTS_LLM_API_KEY")
        or os.getenv("ARK_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or ""
    )
    configured = bool(base_url and api_key) and (provider == "ark" or "volces.com" in base_url)
    if not configured:
        return _build_component(
            "ark",
            False,
            "Ark 配置不完整，或当前未作为默认 LLM provider。",
            configured=False,
            base_url=base_url or None,
            provider=provider or None,
        )

    reachable, detail = _base_host_probe(base_url)
    return _build_component(
        "ark",
        reachable,
        "Ark 网关可访问。" if reachable else f"Ark 网关不可访问：{detail}",
        configured=True,
        base_url=base_url,
        provider=provider,
        probe_detail=detail,
    )


def _check_embedding() -> dict[str, Any]:
    base_url = os.getenv("TRADINGAGENTS_EMBEDDING_BASE_URL") or ""
    model = os.getenv("TRADINGAGENTS_EMBEDDING_MODEL") or ""
    configured = bool(base_url and model)
    if not configured:
        return _build_component(
            "embedding",
            False,
            "Embedding 配置不完整。",
            configured=False,
            base_url=base_url or None,
            model=model or None,
        )

    parsed = urlparse(base_url)
    if parsed.hostname in _LOCAL_HOSTS:
        probe_url = f"{parsed.scheme}://{parsed.netloc}/api/tags"
        reachable, detail = _probe_http(probe_url, bypass_proxy=True)
    else:
        reachable, detail = _base_host_probe(base_url)

    return _build_component(
        "embedding",
        reachable,
        "Embedding 服务可访问。" if reachable else f"Embedding 服务不可访问：{detail}",
        configured=True,
        base_url=base_url,
        model=model,
        probe_detail=detail,
    )


def _check_akshare_loader(name: str, func: Any) -> dict[str, Any]:
    try:
        frame = call_with_proxy_fallback(func)
    except Exception as exc:
        return _build_component(name, False, f"{name} 探测失败：{exc}")
    ok = frame is not None and not getattr(frame, "empty", True)
    return _build_component(
        name,
        ok,
        f"{name} 数据可用。" if ok else f"{name} 数据为空。",
        rows=(0 if frame is None else int(getattr(frame, "shape", [0])[0])),
    )


def _check_akshare_spot() -> dict[str, Any]:
    for loader in (ak.stock_zh_a_spot_em, ak.stock_zh_a_spot):
        try:
            frame = call_with_proxy_or_empty_fallback(loader)
        except Exception:
            continue
        ok = frame is not None and not getattr(frame, "empty", True)
        if ok:
            return _build_component(
                "spot",
                True,
                "spot 数据可用。",
                rows=int(getattr(frame, "shape", [0])[0]),
                provider_route=f"akshare:{loader.__name__}",
            )
    return _build_component("spot", False, "spot 数据为空。", rows=0, provider_route="akshare_unavailable")


def _check_akshare_minute() -> dict[str, Any]:
    try:
        profile = build_security_profile("000001", "cn_a")
        frame, route = _load_intraday_minute_df(profile, datetime.now().strftime("%Y-%m-%d"))
    except Exception as exc:
        return _build_component("minute", False, f"minute 探测失败：{exc}")
    ok = frame is not None and not getattr(frame, "empty", True)
    return _build_component(
        "minute",
        ok,
        "minute 数据可用。" if ok else "minute 数据为空。",
        rows=(0 if frame is None else int(getattr(frame, "shape", [0])[0])),
        provider_route=route,
    )


def _check_akshare() -> dict[str, Any]:
    spot = _check_akshare_spot()
    index_daily = _check_akshare_loader(
        "index",
        lambda: ak.stock_zh_index_daily(symbol="sh000300"),
    )
    minute = _check_akshare_minute()
    ok = spot["ok"] and index_daily["ok"] and minute["ok"]
    return _build_component(
        "akshare",
        ok,
        "Akshare spot/index/minute 全部可用。"
        if ok
        else "Akshare 至少一项关键数据源不可用。",
        checks={"spot": spot, "index": index_daily, "minute": minute},
    )


def _check_qveris() -> dict[str, Any]:
    auth = build_qveris_auth_summary()
    if not auth["configured"]:
        return _build_component(
            "qveris",
            False,
            "QVeris 未配置，将不会作为隔夜扫描补充源。",
            configured=False,
            active_keys=0,
            rotation_enabled=False,
        )

    registry = QVerisToolRegistry()
    client = QVerisClient()
    cached_records = [
        record
        for record in registry.load().values()
        if isinstance(record, dict) and record.get("tool_id")
    ]

    if cached_records:
        for record in cached_records:
            tool_id = str(record.get("tool_id") or "")
            discovery_id = str(record.get("discovery_id") or "") or None
            if not tool_id:
                continue
            try:
                inspected = client.inspect_tools([tool_id], discovery_id=discovery_id, timeout_ms=12_000)
                results = inspected.get("results") or []
                if any(str(item.get("tool_id") or "") == tool_id for item in results if isinstance(item, dict)):
                    return _build_component(
                        "qveris",
                        True,
                        "QVeris 已配置，缓存工具可用。",
                        configured=True,
                        active_keys=auth["active_keys"],
                        rotation_enabled=auth["rotation_enabled"],
                    )
            except Exception:
                continue

    try:
        response = client.discover_tools(CAPABILITY_QUERIES["cn_a_index_snapshot"], limit=1, timeout_ms=12_000)
        results = response.get("results") or []
        if isinstance(results, list) and results:
            return _build_component(
                "qveris",
                True,
                "QVeris 已配置，可发现实时市场工具。",
                configured=True,
                active_keys=auth["active_keys"],
                rotation_enabled=auth["rotation_enabled"],
            )
    except Exception as exc:
        return _build_component(
            "qveris",
            False,
            f"QVeris 探测失败：{exc}",
            configured=True,
            active_keys=auth["active_keys"],
            rotation_enabled=auth["rotation_enabled"],
        )

    return _build_component(
        "qveris",
        False,
        "QVeris 已配置，但未发现可用工具。",
        configured=True,
        active_keys=auth["active_keys"],
        rotation_enabled=auth["rotation_enabled"],
    )


def _check_database(settings: DashboardSettings) -> dict[str, Any]:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    db_parent = settings.db_path.parent
    db_parent.mkdir(parents=True, exist_ok=True)
    probe_path = db_parent / ".write_probe"
    try:
        probe_path.write_text("ok", encoding="utf-8")
        probe_path.unlink(missing_ok=True)
    except OSError as exc:
        return _build_component(
            "database",
            False,
            f"数据库目录不可写：{exc}",
            path=str(settings.db_path),
        )
    return _build_component(
        "database",
        True,
        "数据库目录可写。",
        path=str(settings.db_path),
    )


def _check_frontend(settings: DashboardSettings) -> dict[str, Any]:
    dist_exists = settings.frontend_dist.exists()
    index_exists = (settings.frontend_dist / "index.html").exists()
    ok = dist_exists and index_exists
    return _build_component(
        "frontend",
        ok,
        "前端构建产物可用。"
        if ok
        else "前端 dist 缺失，请先构建 dashboard-ui。",
        dist=str(settings.frontend_dist),
        dist_exists=dist_exists,
        index_exists=index_exists,
    )


def collect_readiness(settings: DashboardSettings, *, refresh: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    if (
        not refresh
        and _READINESS_CACHE["payload"] is not None
        and now - float(_READINESS_CACHE["timestamp"]) < _CACHE_TTL_SECONDS
    ):
        return _READINESS_CACHE["payload"]

    components = {
        "ark": _check_ark(),
        "embedding": _check_embedding(),
        "akshare": _check_akshare(),
        "qveris": _check_qveris(),
        "database": _check_database(settings),
        "frontend": _check_frontend(settings),
    }
    akshare_checks = components["akshare"].get("checks") or {}
    akshare_spot_ok = bool((akshare_checks.get("spot") or {}).get("ok"))
    akshare_index_ok = bool((akshare_checks.get("index") or {}).get("ok"))
    akshare_minute_ok = bool((akshare_checks.get("minute") or {}).get("ok"))
    qveris_ok = bool(components["qveris"].get("ok"))
    market_data_ready = akshare_index_ok and akshare_minute_ok and (akshare_spot_ok or qveris_ok)
    payload = {
        "checked_at": _utc_now(),
        "ready": all(
            (
                components["ark"]["ok"],
                components["embedding"]["ok"],
                market_data_ready,
                components["database"]["ok"],
                components["frontend"]["ok"],
            )
        ),
        "components": components,
    }
    _READINESS_CACHE["timestamp"] = now
    _READINESS_CACHE["payload"] = payload
    return payload
