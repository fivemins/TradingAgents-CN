from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Annotated, Callable

import pandas as pd
import yfinance as yf

from .config import get_config


STANDARD_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]
NUMERIC_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = [
            "_".join(str(part) for part in column if part not in ("", None)).strip("_")
            for column in normalized.columns
        ]
    else:
        normalized.columns = [str(column).strip() for column in normalized.columns]
    normalized = normalized.loc[:, ~normalized.columns.duplicated()]
    return normalized


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _flatten_columns(frame)

    if "Date" not in normalized.columns:
        index_name = getattr(normalized.index, "name", None)
        if index_name and str(index_name).strip().lower() == "date":
            normalized = normalized.reset_index()
        elif "date" in [str(column).strip().lower() for column in normalized.columns]:
            rename_map = {
                column: "Date"
                for column in normalized.columns
                if str(column).strip().lower() == "date"
            }
            normalized = normalized.rename(columns=rename_map)
        else:
            normalized = normalized.reset_index()
            first_column = str(normalized.columns[0]).strip()
            if first_column.lower() in {"index", "date", "datetime"}:
                normalized = normalized.rename(columns={first_column: "Date"})

    column_lookup = {str(column).strip().lower(): str(column) for column in normalized.columns}
    rename_map: dict[str, str] = {}
    for canonical in STANDARD_COLUMNS:
        lower = canonical.lower()
        source = column_lookup.get(lower)
        if source is None and lower == "volume":
            source = column_lookup.get("vol")
        if source:
            rename_map[source] = canonical

    normalized = normalized.rename(columns=rename_map)
    missing = [column for column in STANDARD_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"Price frame is missing required columns: {missing}")

    normalized = normalized[STANDARD_COLUMNS].copy()
    normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce")
    if getattr(normalized["Date"].dt, "tz", None) is not None:
        normalized["Date"] = normalized["Date"].dt.tz_localize(None)

    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["Date", "Open", "High", "Low", "Close"])
    normalized["Volume"] = normalized["Volume"].fillna(0.0)
    normalized = normalized.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    normalized = normalized.reset_index(drop=True)

    if normalized.empty:
        raise ValueError("Price frame is empty after normalization.")

    return normalized


def _atomic_write_csv(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(f"{target.suffix}.{uuid.uuid4().hex}.tmp")
    frame.to_csv(temp_path, index=False)
    os.replace(temp_path, target)


def _download_online_frame(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    data = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        multi_level_index=False,
        progress=False,
        auto_adjust=True,
        threads=False,
    )
    if data is None or data.empty:
        raise ValueError(f"No online price data returned for {symbol}.")
    return _normalize_price_frame(data.reset_index())


def _load_cached_frame_or_rebuild(
    data_file: Path,
    rebuild: Callable[[], pd.DataFrame],
) -> pd.DataFrame:
    if data_file.exists():
        try:
            return _normalize_price_frame(pd.read_csv(data_file))
        except Exception:
            try:
                data_file.unlink()
            except FileNotFoundError:
                # Another repair attempt may already have removed the broken cache file.
                pass

    rebuilt = rebuild()
    _atomic_write_csv(rebuilt, data_file)
    return rebuilt


def _wilder_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)

    avg_gain = _wilder_ema(gains, period)
    avg_loss = _wilder_ema(losses, period)
    safe_avg_loss = avg_loss.where(avg_loss != 0)
    rs = avg_gain / safe_avg_loss
    result = pd.to_numeric(100 - (100 / (1 + rs)), errors="coerce")
    result = result.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    result = result.mask((avg_loss == 0) & (avg_gain == 0), 50.0)
    return result


def _atr(frame: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = frame["Close"].shift(1)
    tr = pd.concat(
        [
            frame["High"] - frame["Low"],
            (frame["High"] - prev_close).abs(),
            (frame["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return _wilder_ema(tr, period)


def _bollinger(close: pd.Series, period: int = 20, std_factor: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    middle = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std(ddof=0)
    upper = middle + std_factor * std
    lower = middle - std_factor * std
    return middle, upper, lower


def _vwma(close: pd.Series, volume: pd.Series, period: int = 20) -> pd.Series:
    weighted_price = close * volume
    rolling_volume = volume.rolling(window=period, min_periods=period).sum()
    rolling_weighted = weighted_price.rolling(window=period, min_periods=period).sum()
    safe_rolling_volume = rolling_volume.where(rolling_volume != 0)
    return pd.to_numeric(rolling_weighted / safe_rolling_volume, errors="coerce")


def _mfi(frame: pd.DataFrame, period: int = 14) -> pd.Series:
    typical_price = (frame["High"] + frame["Low"] + frame["Close"]) / 3.0
    money_flow = typical_price * frame["Volume"]
    direction = typical_price.diff()
    positive_flow = money_flow.where(direction > 0, 0.0)
    negative_flow = money_flow.where(direction < 0, 0.0)
    positive_sum = positive_flow.rolling(window=period, min_periods=period).sum()
    negative_sum = negative_flow.rolling(window=period, min_periods=period).sum()
    safe_negative_sum = negative_sum.where(negative_sum != 0)
    ratio = positive_sum / safe_negative_sum
    result = pd.to_numeric(100 - (100 / (1 + ratio)), errors="coerce")
    result = result.mask((negative_sum == 0) & (positive_sum > 0), 100.0)
    result = result.mask((negative_sum == 0) & (positive_sum == 0), 50.0)
    return result


def _macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _compute_indicator(frame: pd.DataFrame, indicator: str) -> pd.Series:
    close = frame["Close"]
    volume = frame["Volume"]

    sma_match = re.fullmatch(r"close_(\d+)_sma", indicator)
    if sma_match:
        window = int(sma_match.group(1))
        return close.rolling(window=window, min_periods=window).mean()

    ema_match = re.fullmatch(r"close_(\d+)_ema", indicator)
    if ema_match:
        span = int(ema_match.group(1))
        return close.ewm(span=span, adjust=False, min_periods=span).mean()

    if indicator in {"macd", "macds", "macdh"}:
        macd_line, signal_line, histogram = _macd(close)
        return {
            "macd": macd_line,
            "macds": signal_line,
            "macdh": histogram,
        }[indicator]

    if indicator == "rsi":
        return _rsi(close, period=14)

    if indicator in {"boll", "boll_ub", "boll_lb"}:
        middle, upper, lower = _bollinger(close, period=20, std_factor=2.0)
        return {
            "boll": middle,
            "boll_ub": upper,
            "boll_lb": lower,
        }[indicator]

    if indicator == "atr":
        return _atr(frame, period=14)

    if indicator == "vwma":
        return _vwma(close, volume, period=20)

    if indicator == "mfi":
        return _mfi(frame, period=14)

    raise ValueError(f"Unsupported indicator: {indicator}")


class TechnicalIndicatorsUtils:
    @staticmethod
    def _load_price_frame(
        symbol: str,
        curr_date: str,
        data_dir: str,
        online: bool,
    ) -> pd.DataFrame:
        if not online:
            data_file = Path(data_dir) / f"{symbol}-YFin-data-2015-01-01-2025-03-25.csv"
            if not data_file.exists():
                raise FileNotFoundError("Indicator data not fetched yet.")
            return _normalize_price_frame(pd.read_csv(data_file))

        today_date = pd.Timestamp.today()
        curr_ts = pd.to_datetime(curr_date)

        end_date = max(today_date.normalize(), curr_ts.normalize()).strftime("%Y-%m-%d")
        start_date = (today_date - pd.DateOffset(years=15)).strftime("%Y-%m-%d")

        config = get_config()
        cache_dir = Path(config["data_cache_dir"])
        cache_dir.mkdir(parents=True, exist_ok=True)
        data_file = cache_dir / f"{symbol}-YFin-data-{start_date}-{end_date}.csv"

        return _load_cached_frame_or_rebuild(
            data_file,
            lambda: _download_online_frame(symbol, start_date, end_date),
        )

    @staticmethod
    def get_indicator_series(
        symbol: Annotated[str, "ticker symbol for the company"],
        indicator: Annotated[
            str, "quantitative indicators based on the stock data for the company"
        ],
        curr_date: Annotated[
            str, "curr date for retrieving stock price data, YYYY-mm-dd"
        ],
        data_dir: Annotated[
            str,
            "directory where the stock data is stored.",
        ],
        online: Annotated[
            bool,
            "whether to use online tools to fetch data or offline tools. If True, will use online tools.",
        ] = False,
    ) -> pd.DataFrame:
        data = TechnicalIndicatorsUtils._load_price_frame(symbol, curr_date, data_dir, online)
        try:
            indicator_series = _compute_indicator(data, indicator)
        except Exception as exc:
            raise RuntimeError(f"Unable to calculate indicator '{indicator}': {exc}") from exc

        result = pd.DataFrame(
            {
                "Date": pd.to_datetime(data["Date"], errors="coerce").dt.strftime("%Y-%m-%d"),
                indicator: indicator_series,
            }
        )
        return result.dropna(subset=["Date"])

    @staticmethod
    def get_indicator_value(
        symbol: Annotated[str, "ticker symbol for the company"],
        indicator: Annotated[
            str, "quantitative indicators based on the stock data for the company"
        ],
        curr_date: Annotated[
            str, "curr date for retrieving stock price data, YYYY-mm-dd"
        ],
        data_dir: Annotated[
            str,
            "directory where the stock data is stored.",
        ],
        online: Annotated[
            bool,
            "whether to use online tools to fetch data or offline tools. If True, will use online tools.",
        ] = False,
    ):
        series = TechnicalIndicatorsUtils.get_indicator_series(
            symbol=symbol,
            indicator=indicator,
            curr_date=curr_date,
            data_dir=data_dir,
            online=online,
        )
        matching_rows = series[series["Date"] == pd.to_datetime(curr_date).strftime("%Y-%m-%d")]
        if matching_rows.empty:
            return "N/A: Not a trading day (weekend or holiday)"
        return matching_rows[indicator].values[0]

__all__ = ["TechnicalIndicatorsUtils"]
