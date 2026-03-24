from __future__ import annotations

from typing import Any


SUSPICIOUS_TOKENS = (
    "锛",
    "銆",
    "鍙",
    "鏃",
    "甯",
    "娆",
    "璇",
    "鍚",
    "绛",
    "鏈",
    "鐨",
    "浠",
    "鎴",
    "闃",
    "璁",
)


def _cjk_count(value: str) -> int:
    return sum(1 for char in value if "\u4e00" <= char <= "\u9fff")


def _suspicious_count(value: str) -> int:
    return sum(value.count(token) for token in SUSPICIOUS_TOKENS) + value.count("�")


def looks_like_mojibake(value: str | None) -> bool:
    if not value:
        return False
    suspicious = _suspicious_count(value)
    if suspicious <= 0:
        return False
    return suspicious >= 2 or ("?" in value and _cjk_count(value) > 0)


def _score_candidate(value: str) -> int:
    cjk = _cjk_count(value)
    suspicious = _suspicious_count(value)
    ascii_letters = sum(1 for char in value if char.isascii() and char.isalnum())
    return cjk * 3 + ascii_letters - suspicious * 6 - value.count("?") * 4


def repair_mojibake(value: str) -> str:
    if not looks_like_mojibake(value):
        return value

    best = value
    best_score = _score_candidate(value)
    for source_encoding in ("gbk", "gb18030", "cp936"):
        try:
            candidate = value.encode(source_encoding, errors="ignore").decode(
                "utf-8", errors="ignore"
            )
        except UnicodeError:
            continue
        if not candidate:
            continue
        score = _score_candidate(candidate)
        if score > best_score:
            best = candidate
            best_score = score
    return best


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    return repair_mojibake(value)


def clean_structure(value: Any) -> Any:
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, list):
        return [clean_structure(item) for item in value]
    if isinstance(value, tuple):
        return [clean_structure(item) for item in value]
    if isinstance(value, dict):
        return {
            clean_text(str(key)) if isinstance(key, str) else key: clean_structure(item)
            for key, item in value.items()
        }
    return value


def clean_source_name(name: str | None, fallback: str | None = None) -> str | None:
    cleaned = clean_text(name)
    if cleaned and cleaned.strip() and "?" not in cleaned:
        return cleaned.strip()
    if fallback:
        return fallback
    return cleaned.strip() if cleaned else None
