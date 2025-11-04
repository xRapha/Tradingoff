#!/usr/bin/env python3
"""Backtest the 4-hour range scalping strategy on minute CSV data."""
from __future__ import annotations

import argparse
import csv
import io
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Sequence, Tuple
import zipfile

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAVE_MATPLOTLIB = True
except Exception:  # pragma: no cover - optional dependency
    matplotlib = None
    plt = None
    HAVE_MATPLOTLIB = False

from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
NY_TZ = ZoneInfo("America/New_York")
PARIS_TZ = ZoneInfo("Europe/Paris")

FRENCH_MONTH_NAMES = [
    "",
    "Janvier",
    "Février",
    "Mars",
    "Avril",
    "Mai",
    "Juin",
    "Juillet",
    "Août",
    "Septembre",
    "Octobre",
    "Novembre",
    "Décembre",
]


# ==============================================
# CONFIGURATION
# ==============================================
DATA_DIR = Path(__file__).resolve().parent
DATA_FILE_NAME = "MNQ.csv"
DATA_CACHE_FILENAME: Optional[str] = None  # Defaults to <stem>_days.pkl when None
USE_DATA_CACHE = True

# Trading session constraints
FORCED_SESSION_CLOSE_TIME = time(16, 45)  # 16:45 New York time

# RR sweep + minimum performance requirements
RR_START = 1.0
RR_END = 3.0
RR_STEP = 0.1
MIN_AVG_R = 0.12
MIN_TRADES = 1000

# Trade limits (None = disabled)
MAX_TRADES_PER_DAY: Optional[int] = None
MAX_TRADES_PER_DIRECTION: Optional[int] = None
MIN_REENTRY_MINUTES: Optional[int] = None
MAX_REENTRY_MINUTES: Optional[int] = None
PREVENT_OVERLAP: bool = False

# Time allowed after the 4h box close to enter a trade (minutes)
MAX_MINUTES_AFTER_BOX_CLOSE: Optional[int] = 540

# Entry handling
ENTRY_MODE: Literal["open", "close"] = "open"

# Breakout filters (None/0 = disabled)
MIN_BREAKOUT_PCT: Optional[float] = None  # fraction of the box height required outside on the close
MIN_BREAKOUT_WICK_PCT: Optional[float] = None  # fraction of the breakout-side wick vs. the full candle range
MAX_BREAKOUT_PCT: Optional[float] = None

# 4H box size filters (points)
MIN_BOX_SIZE: Optional[float] = None
MAX_BOX_SIZE: Optional[float] = None

# Retest candle filters (fractions relative to box / candle body)
MIN_RETEST_SIZE_PCT: Optional[float] = None
MAX_RETEST_SIZE_PCT: Optional[float] = None
MIN_RETEST_BODY_INSIDE_PCT: Optional[float] = None

# Stop distance filters (ticks)
TICK_SIZE: float = 0.25
MIN_TICKS_TO_STOP: Optional[float] = None
MAX_TICKS_TO_STOP: Optional[float] = None

# Parameter sweep configuration
RUN_SWEEP_BY_DEFAULT = True
SWEEP_TOP_N = 10
SWEEP_RR_VALUES = [1.85]
SWEEP_MAX_MINUTES_AFTER_BOX_CLOSE: Sequence[Optional[int]] = [540]
SWEEP_MIN_BREAKOUT_PCTS: Sequence[Optional[float]] = [None]
SWEEP_MIN_BREAKOUT_WICK_PCTS: Sequence[Optional[float]] = [None, 0.1, 0.2, 0.3, 0.4]
SWEEP_MAX_BREAKOUT_PCTS: Sequence[Optional[float]] = [0.2]
SWEEP_MAX_REENTRY_MINUTES: Sequence[Optional[int]] = [155]
SWEEP_MIN_BOX_SIZES: Sequence[Optional[float]] = [30]
SWEEP_MAX_BOX_SIZES: Sequence[Optional[float]] = [180]
SWEEP_MIN_RETEST_SIZE_PCTS: Sequence[Optional[float]] = [None]
SWEEP_MAX_RETEST_SIZE_PCTS: Sequence[Optional[float]] = [0.3]
SWEEP_MIN_RETEST_BODY_INSIDE_PCTS: Sequence[Optional[float]] = [0.2]
SWEEP_MIN_TICKS_TO_STOP: Sequence[Optional[float]] = [42]
SWEEP_MAX_TICKS_TO_STOP: Sequence[Optional[float]] = [400]

@dataclass
class MinuteBar:
    time: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class Bar:
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class DayData:
    day_start: datetime
    range_high: float
    range_low: float
    bars: List[Bar]


@dataclass
class TradeResult:
    day: datetime
    direction: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    target_price: float
    exit_time: datetime
    exit_price: float
    rr: float
    pnl_r: float
    exit_reason: str
    range_high: float
    range_low: float
    breakout_time: Optional[datetime]
    breakout_price: Optional[float]


@dataclass
class CandidateTrade:
    day: datetime
    direction: str
    range_high: float
    range_low: float
    range_size: float
    range_end_time: datetime
    breakout_time: datetime
    breakout_price: float
    breakout_close_pct: float
    breakout_wick_pct: float
    entry_bar_start: datetime
    entry_bar_end: datetime
    entry_bar_open: float
    entry_bar_close: float
    entry_bar_high: float
    entry_bar_low: float
    next_bar_start: Optional[datetime]
    next_bar_end: Optional[datetime]
    next_bar_open: Optional[float]
    next_bar_high: Optional[float]
    next_bar_low: Optional[float]
    next_bar_close: Optional[float]
    retest_size_pct: Optional[float]
    retest_body_inside_pct: Optional[float]
    wait_minutes: float
    stop_price: float
    post_times: List[datetime]
    post_highs: List[float]
    post_lows: List[float]
    session_close_time: datetime
    session_close_price: float


@dataclass
class ComboSummary:
    rr: float
    min_breakout_pct: Optional[float]
    min_breakout_wick_pct: Optional[float]
    max_breakout_pct: Optional[float]
    max_reentry_minutes: Optional[int]
    min_box_size: Optional[float]
    max_box_size: Optional[float]
    min_retest_size_pct: Optional[float]
    max_retest_size_pct: Optional[float]
    min_retest_body_inside_pct: Optional[float]
    max_entry_minutes_from_box_close: Optional[int]
    min_ticks_to_stop: Optional[float]
    max_ticks_to_stop: Optional[float]
    trades: int
    wins: int
    losses: int
    avg_r: float
    total_r: float
    win_rate: float


@dataclass
class PrecomputedOutcome:
    exit_time: datetime
    trade: Optional[TradeResult]
    status: str


def format_optional(value: Optional[float], precision: str = ".2f") -> str:
    if value is None:
        return "None"
    return f"{value:{precision}}"


def count_backtest_days(
    days: Sequence[DayData], *, min_year: int = 2010
) -> int:
    """Return the number of trading days considered by the backtest.

    Days prior to ``min_year`` are ignored so the count matches the
    generation of candidates and trades, and days without trades still
    contribute to the denominator as requested.
    """

    return sum(1 for day in days if day.day_start.year >= min_year)


def print_monthly_summary(trades: Sequence[TradeResult], year: int) -> None:
    """Print monthly performance metrics for the requested year."""

    monthly_stats = {
        month: {"trades": 0, "wins": 0, "losses": 0, "total_r": 0.0}
        for month in range(1, 13)
    }

    for trade in trades:
        local_time = trade.entry_time.astimezone(NY_TZ)
        if local_time.year != year:
            continue
        bucket = monthly_stats[local_time.month]
        bucket["trades"] += 1
        if trade.pnl_r > 0:
            bucket["wins"] += 1
        elif trade.pnl_r < 0:
            bucket["losses"] += 1
        bucket["total_r"] += trade.pnl_r

    any_activity = any(stats["trades"] > 0 for stats in monthly_stats.values())
    print(f"\nBilan mensuel {year}:")
    if not any_activity:
        print(f"  Aucun trade enregistré en {year}.")
        return

    for month in range(1, 13):
        stats = monthly_stats[month]
        trades_count = stats["trades"]
        wins = stats["wins"]
        losses = stats["losses"]
        total_r = stats["total_r"]
        avg_r = total_r / trades_count if trades_count else 0.0
        win_rate = (wins / trades_count * 100.0) if trades_count else 0.0
        month_name = FRENCH_MONTH_NAMES[month]
        print(
            "  "
            f"{month_name:<9} | trades={trades_count:>4} | wins={wins:>3} | losses={losses:>3} | "
            f"win%={win_rate:6.2f} | totalR={total_r:7.2f} | avgR={avg_r:6.4f}"
        )


def format_trade_line(index: int, trade: TradeResult, cumulative_r: float) -> str:
    entry_local = trade.entry_time.astimezone(PARIS_TZ)
    exit_local = trade.exit_time.astimezone(PARIS_TZ)
    entry_str = entry_local.strftime("%Y-%m-%d %H:%M")
    exit_str = exit_local.strftime("%Y-%m-%d %H:%M")
    breakout_str = "N/A"
    breakout_price_str = "N/A"
    if trade.breakout_time is not None:
        breakout_local = trade.breakout_time.astimezone(PARIS_TZ)
        breakout_str = breakout_local.strftime("%Y-%m-%d %H:%M")
    if trade.breakout_price is not None:
        breakout_price_str = f"{trade.breakout_price:.2f}"

    base = (
        f"#{index:>4} | {entry_str} -> {exit_str} | {trade.direction:<5} | "
        f"Entry={trade.entry_price:.2f} | Exit={trade.exit_price:.2f} | SL={trade.stop_price:.2f} | "
        f"Box=[{trade.range_low:.2f}-{trade.range_high:.2f}] | "
        f"Cassure={breakout_str} @ {breakout_price_str} | "
        f"R={trade.pnl_r:+.4f} | R cumulé={cumulative_r:+.4f} | sortie={trade.exit_reason}"
    )
    return base


class FiveMinuteBuilder:
    def __init__(self) -> None:
        self.bars: List[Bar] = []
        self.current_start: Optional[datetime] = None
        self.current_end: Optional[datetime] = None
        self.open_price: Optional[float] = None
        self.high: Optional[float] = None
        self.low: Optional[float] = None
        self.close: Optional[float] = None
        self.last_time: Optional[datetime] = None

    def add(self, bar: MinuteBar) -> None:
        if self.current_start is None:
            self.current_start = bar.time.replace(
                minute=(bar.time.minute // 5) * 5, second=0, microsecond=0
            )
            self.current_end = self.current_start + timedelta(minutes=5)

        while self.current_end is not None and bar.time >= self.current_end:
            self._flush()
            self.current_start = self.current_end
            self.current_end = self.current_start + timedelta(minutes=5)

        if self.open_price is None:
            self.open_price = bar.open
            self.high = bar.high
            self.low = bar.low
        else:
            self.high = max(self.high, bar.high)  # type: ignore[arg-type]
            self.low = min(self.low, bar.low)  # type: ignore[arg-type]
        self.close = bar.close
        self.last_time = bar.time

    def finalize(self) -> List[Bar]:
        self._flush()
        return self.bars

    def _flush(self) -> None:
        if self.open_price is None or self.last_time is None:
            return
        start = self.current_start
        end = self.current_end
        if start is None:
            start = self.last_time.replace(second=0, microsecond=0)
        if end is None:
            end = start + timedelta(minutes=5)
        self.bars.append(
            Bar(
                start=start,
                end=end,
                open=self.open_price,
                high=self.high if self.high is not None else self.open_price,
                low=self.low if self.low is not None else self.open_price,
                close=self.close if self.close is not None else self.open_price,
            )
        )
        self.open_price = None
        self.high = None
        self.low = None
        self.close = None


def _trading_day_start(ts: datetime) -> datetime:
    return datetime.combine(ts.date(), time(0), tzinfo=NY_TZ)


def _truncate_bars_at_forced_close(bars: List[Bar], day_start: datetime) -> List[Bar]:
    if not bars:
        return bars

    forced_close_dt = datetime.combine(
        day_start.date(), FORCED_SESSION_CLOSE_TIME, tzinfo=NY_TZ
    )

    truncated: List[Bar] = []
    reached_forced_close = False
    for bar in bars:
        truncated.append(bar)
        if forced_close_dt <= bar.end:
            reached_forced_close = True
            break

    if reached_forced_close:
        return truncated

    return bars


def _load_cached_days(cache_path: Path, sources: Sequence[Path]) -> Optional[List[DayData]]:
    if not cache_path.exists():
        return None

    cache_mtime = cache_path.stat().st_mtime
    for source in sources:
        if source.exists() and source.stat().st_mtime > cache_mtime:
            return None

    with cache_path.open("rb") as handle:
        return pickle.load(handle)


def _store_cached_days(cache_path: Path, days: List[DayData]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("wb") as handle:
        pickle.dump(days, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_day_data(
    data_dir: Path,
    *,
    use_cache: bool = USE_DATA_CACHE,
    cache_path: Optional[Path] = None,
    data_file: str = DATA_FILE_NAME,
) -> List[DayData]:
    csv_path = data_dir / data_file
    stem = Path(data_file).stem
    zip_name = stem + ".zip"
    zip_path = data_dir / zip_name
    part_pattern = stem + ".zip.part*"
    parts = sorted(data_dir.glob(part_pattern)) if not csv_path.exists() else []
    sources: List[Path] = []
    if csv_path.exists():
        sources.append(csv_path)
    elif zip_path.exists():
        sources.append(zip_path)
    else:
        sources.extend(parts)

    if use_cache:
        cache_name = DATA_CACHE_FILENAME or f"{stem}_days.pkl"
        resolved_cache = cache_path or data_dir / cache_name
        cached = _load_cached_days(resolved_cache, sources)
        if cached is not None:
            return cached

    if csv_path.exists():
        with csv_path.open("r", newline="") as handle:
            days = _read_day_data(handle)
    elif zip_path.exists():
        with zipfile.ZipFile(zip_path) as archive:
            with archive.open(Path(data_file).name) as handle:
                text = io.TextIOWrapper(handle, encoding="utf-8")
                days = _read_day_data(text)
    else:
        if not parts:
            raise FileNotFoundError(
                f"No {data_file} or {stem}.zip.part* files found in {data_dir}"
            )

        combined = io.BytesIO()
        for part in parts:
            combined.write(part.read_bytes())
        combined.seek(0)

        with zipfile.ZipFile(combined) as archive:
            with archive.open(Path(data_file).name) as handle:
                text = io.TextIOWrapper(handle, encoding="utf-8")
                days = _read_day_data(text)

    if use_cache:
        cache_name = DATA_CACHE_FILENAME or f"{stem}_days.pkl"
        resolved_cache = cache_path or data_dir / cache_name
        _store_cached_days(resolved_cache, days)

    return days


def _read_day_data(handle: io.TextIOBase) -> List[DayData]:
    reader = csv.reader(handle, delimiter=";")
    header = next(reader, None)
    if header is None:
        return []

    days: List[DayData] = []
    current_day_start: Optional[datetime] = None
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    range_deadline: Optional[datetime] = None
    builder = FiveMinuteBuilder()

    for row in reader:
        if len(row) < 6:
            continue
        try:
            close = float(row[0])
            high = float(row[1])
            low = float(row[2])
            open_price = float(row[4])
            time_str = row[5]
        except ValueError:
            continue

        dt_utc = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
        dt_ny = dt_utc.astimezone(NY_TZ)
        bar = MinuteBar(time=dt_ny, open=open_price, high=high, low=low, close=close)
        day_start = _trading_day_start(bar.time)

        if current_day_start is None:
            current_day_start = day_start
            range_high = float("-inf")
            range_low = float("inf")
            range_deadline = current_day_start + timedelta(hours=4)
        elif day_start != current_day_start:
            day_bars = builder.finalize()
            day_bars = _truncate_bars_at_forced_close(day_bars, current_day_start)
            if range_high is not None and range_high != float("-inf") and day_bars:
                days.append(
                    DayData(
                        day_start=current_day_start,
                        range_high=range_high,
                        range_low=range_low if range_low is not None else range_high,
                        bars=day_bars,
                    )
                )
            builder = FiveMinuteBuilder()
            current_day_start = day_start
            range_high = float("-inf")
            range_low = float("inf")
            range_deadline = current_day_start + timedelta(hours=4)

        if range_deadline is not None and bar.time < range_deadline:
            range_high = max(range_high, bar.high) if range_high is not None else bar.high
            range_low = min(range_low, bar.low) if range_low is not None else bar.low

        builder.add(bar)

    if current_day_start is not None:
        day_bars = builder.finalize()
        day_bars = _truncate_bars_at_forced_close(day_bars, current_day_start)
        if range_high is not None and range_high != float("-inf") and day_bars:
            days.append(
                DayData(
                    day_start=current_day_start,
                    range_high=range_high,
                    range_low=range_low if range_low is not None else range_high,
                    bars=day_bars,
                )
            )

    return days


def generate_trade_log(
    days: List[DayData],
    rr: float,
    min_year: int = 2010,
    max_trades_per_day: Optional[int] = None,
    max_trades_per_direction: Optional[int] = None,
    max_reentry_minutes: Optional[int] = None,
    min_breakout_pct: Optional[float] = None,
    min_breakout_wick_pct: Optional[float] = None,
    min_box_size: Optional[float] = None,
    max_box_size: Optional[float] = None,
    min_retest_size_pct: Optional[float] = None,
    max_retest_size_pct: Optional[float] = None,
    min_retest_body_inside_pct: Optional[float] = None,
    entry_mode: Literal["open", "close"] = ENTRY_MODE,
) -> List[TradeResult]:
    trades: List[TradeResult] = []

    for day in days:
        if day.day_start.year < min_year:
            continue

        range_high = day.range_high
        range_low = day.range_low
        pending_breakout: Optional[dict] = None
        active_trade: Optional[dict] = None
        range_end = day.day_start + timedelta(hours=4)
        range_size = range_high - range_low
        if min_box_size is not None and range_size < min_box_size:
            continue
        if max_box_size is not None and range_size > max_box_size:
            continue
        trades_taken = 0
        direction_counts = {"long": 0, "short": 0}

        for bar in day.bars:
            if bar.end <= range_end:
                continue

            if active_trade is not None:
                if bar.end > active_trade["entry_bar_end"]:
                    action, info = _check_trade_exit(bar, active_trade)
                    if action == "exit":
                        trades.append(
                            TradeResult(
                                day=day.day_start,
                                direction=active_trade["direction"],
                                entry_time=active_trade["entry_time"],
                                entry_price=active_trade["entry_price"],
                                stop_price=active_trade["stop_price"],
                                target_price=active_trade["target_price"],
                                exit_time=bar.end,
                                exit_price=info["price"],
                                rr=rr,
                                pnl_r=info["pnl_r"],
                                exit_reason=info["reason"],
                                range_high=active_trade["range_high"],
                                range_low=active_trade["range_low"],
                                breakout_time=active_trade.get("breakout_time"),
                                breakout_price=active_trade.get("breakout_price"),
                            )
                        )
                        active_trade = None
                    elif action == "cancel":
                        direction_counts[active_trade["direction"]] = max(
                            0, direction_counts[active_trade["direction"]] - 1
                        )
                        trades_taken = max(0, trades_taken - 1)
                        active_trade = None
                if active_trade is not None:
                    continue

            if pending_breakout is not None:
                if range_low <= bar.close <= range_high:
                    if not _retest_filters_ok(
                        bar,
                        range_high=range_high,
                        range_low=range_low,
                        range_size=range_size,
                        min_size_pct=min_retest_size_pct,
                        max_size_pct=max_retest_size_pct,
                        min_body_inside_pct=min_retest_body_inside_pct,
                    ):
                        pending_breakout = None
                        continue
                    if (
                        max_reentry_minutes is not None
                        and pending_breakout.get("time") is not None
                        and (bar.end - pending_breakout["time"]).total_seconds() / 60.0
                        > max_reentry_minutes
                    ):
                        pending_breakout = None
                        continue
                    entry_time = bar.start if entry_mode == "open" else bar.end
                    entry_price = bar.open if entry_mode == "open" else bar.close
                    stop_price = pending_breakout["stop"]
                    risk = abs(entry_price - stop_price)
                    if risk == 0:
                        pending_breakout = None
                        continue
                    direction = "short" if pending_breakout["direction"] == "above" else "long"
                    if (
                        max_trades_per_day is not None and trades_taken >= max_trades_per_day
                    ) or (
                        max_trades_per_direction is not None
                        and direction_counts[direction] >= max_trades_per_direction
                    ):
                        pending_breakout = None
                        continue
                    target_price = entry_price - rr * risk if direction == "short" else entry_price + rr * risk
                    active_trade = {
                        "direction": direction,
                        "entry_time": entry_time,
                        "entry_bar_end": bar.end,
                        "entry_price": entry_price,
                        "stop_price": stop_price,
                        "target_price": target_price,
                        "risk": risk,
                        "rr": rr,
                        "entry_mode": entry_mode,
                        "range_high": range_high,
                        "range_low": range_low,
                        "breakout_time": pending_breakout.get("time"),
                        "breakout_price": pending_breakout.get("breakout_price"),
                    }
                    trades_taken += 1
                    direction_counts[direction] += 1
                    action, info = _check_trade_exit(bar, active_trade, is_entry_bar=True)
                    if action == "exit":
                        trades.append(
                            TradeResult(
                                day=day.day_start,
                                direction=active_trade["direction"],
                                entry_time=active_trade["entry_time"],
                                entry_price=active_trade["entry_price"],
                                stop_price=active_trade["stop_price"],
                                target_price=active_trade["target_price"],
                                exit_time=bar.end,
                                exit_price=info["price"],
                                rr=rr,
                                pnl_r=info["pnl_r"],
                                exit_reason=info["reason"],
                                range_high=active_trade["range_high"],
                                range_low=active_trade["range_low"],
                                breakout_time=active_trade.get("breakout_time"),
                                breakout_price=active_trade.get("breakout_price"),
                            )
                        )
                        active_trade = None
                    elif action == "cancel":
                        direction_counts[direction] = max(0, direction_counts[direction] - 1)
                        trades_taken = max(0, trades_taken - 1)
                        active_trade = None
                    pending_breakout = None
                    continue

                if pending_breakout["direction"] == "above":
                    if bar.close > range_high:
                        pending_breakout["stop"] = max(pending_breakout["stop"], bar.high)
                    elif bar.close < range_low:
                        if (
                            range_size == 0
                            or min_breakout_pct is None
                            or (range_low - bar.close) / range_size >= min_breakout_pct
                        ):
                            if _breakout_wick_ok(
                                bar,
                                direction="below",
                                min_pct=min_breakout_wick_pct,
                            ):
                                pending_breakout = {
                                    "direction": "below",
                                    "stop": bar.low,
                                    "time": bar.end,
                                    "breakout_bar": bar,
                                    "breakout_price": bar.close,
                                }
                            else:
                                pending_breakout = None
                        else:
                            pending_breakout = None
                else:
                    if bar.close < range_low:
                        pending_breakout["stop"] = min(pending_breakout["stop"], bar.low)
                    elif bar.close > range_high:
                        if (
                            range_size == 0
                            or min_breakout_pct is None
                            or (bar.close - range_high) / range_size >= min_breakout_pct
                        ):
                            if _breakout_wick_ok(
                                bar,
                                direction="above",
                                min_pct=min_breakout_wick_pct,
                            ):
                                pending_breakout = {
                                    "direction": "above",
                                    "stop": bar.high,
                                    "time": bar.end,
                                    "breakout_bar": bar,
                                    "breakout_price": bar.close,
                                }
                            else:
                                pending_breakout = None
                        else:
                            pending_breakout = None
                continue

            if bar.close > range_high:
                if (
                    range_size == 0
                    or min_breakout_pct is None
                    or (bar.close - range_high) / range_size >= min_breakout_pct
                ):
                    if _breakout_wick_ok(
                        bar,
                        direction="above",
                        min_pct=min_breakout_wick_pct,
                    ):
                        pending_breakout = {
                            "direction": "above",
                            "stop": bar.high,
                            "time": bar.end,
                            "breakout_bar": bar,
                            "breakout_price": bar.close,
                        }
            elif bar.close < range_low:
                if (
                    range_size == 0
                    or min_breakout_pct is None
                    or (range_low - bar.close) / range_size >= min_breakout_pct
                ):
                    if _breakout_wick_ok(
                        bar,
                        direction="below",
                        min_pct=min_breakout_wick_pct,
                    ):
                        pending_breakout = {
                            "direction": "below",
                            "stop": bar.low,
                            "time": bar.end,
                            "breakout_bar": bar,
                            "breakout_price": bar.close,
                        }

        if active_trade is not None and day.bars:
            last_bar = day.bars[-1]
            exit_price = last_bar.close
            pnl_r = _pnl_from_exit(exit_price, active_trade)
            trades.append(
                TradeResult(
                    day=day.day_start,
                    direction=active_trade["direction"],
                    entry_time=active_trade["entry_time"],
                    entry_price=active_trade["entry_price"],
                    stop_price=active_trade["stop_price"],
                    target_price=active_trade["target_price"],
                    exit_time=last_bar.end,
                    exit_price=exit_price,
                    rr=rr,
                    pnl_r=pnl_r,
                    exit_reason="session_close",
                    range_high=active_trade["range_high"],
                    range_low=active_trade["range_low"],
                    breakout_time=active_trade.get("breakout_time"),
                    breakout_price=active_trade.get("breakout_price"),
                )
            )

    return trades


def collect_candidate_trades(
    days: List[DayData],
    *,
    min_year: int = 2010,
) -> Dict[datetime, List[CandidateTrade]]:
    candidates: Dict[datetime, List[CandidateTrade]] = {}
    for day in days:
        if day.day_start.year < min_year:
            continue
        day_candidates = _build_day_candidates(day)
        if day_candidates:
            candidates[day.day_start] = day_candidates
    return candidates


def _build_day_candidates(day: DayData) -> List[CandidateTrade]:
    range_high = day.range_high
    range_low = day.range_low
    range_size = range_high - range_low
    range_end = day.day_start + timedelta(hours=4)
    forced_close_dt = datetime.combine(
        day.day_start.date(), FORCED_SESSION_CLOSE_TIME, tzinfo=NY_TZ
    )

    session_close_bar = day.bars[-1] if day.bars else None
    if session_close_bar is None:
        return []

    session_close_time = session_close_bar.end
    if forced_close_dt <= session_close_time:
        session_close_time = forced_close_dt
    session_close_price = session_close_bar.close

    candidates: List[CandidateTrade] = []
    pending_breakout: Optional[dict] = None

    for idx, bar in enumerate(day.bars):
        if bar.end <= range_end:
            continue

        if pending_breakout is not None:
            if range_low <= bar.close <= range_high:
                stop_price = pending_breakout["stop"]
                direction = "short" if pending_breakout["direction"] == "above" else "long"
                breakout_bar: Bar = pending_breakout["breakout_bar"]
                breakout_close_pct = _compute_breakout_close_pct(
                    breakout_bar,
                    direction=direction,
                    range_high=range_high,
                    range_low=range_low,
                    range_size=range_size,
                )
                breakout_wick_pct = _breakout_wick_ratio(
                    breakout_bar,
                    direction="above" if direction == "short" else "below",
                )
                retest_size_pct = _compute_retest_size_pct(bar, range_size)
                retest_body_inside_pct = _retest_body_inside_ratio(
                    bar,
                    range_high=range_high,
                    range_low=range_low,
                )
                wait_minutes = (
                    (bar.end - pending_breakout["time"]).total_seconds() / 60.0
                    if pending_breakout.get("time") is not None
                    else 0.0
                )
                next_bar = day.bars[idx + 1] if idx + 1 < len(day.bars) else None
                post_bars = day.bars[idx + 1 :]
                post_times = [b.end for b in post_bars]
                post_highs = [b.high for b in post_bars]
                post_lows = [b.low for b in post_bars]
                candidates.append(
                    CandidateTrade(
                        day=day.day_start,
                        direction=direction,
                        range_high=range_high,
                        range_low=range_low,
                        range_size=range_size,
                        range_end_time=range_end,
                        breakout_time=pending_breakout["time"],
                        breakout_price=pending_breakout.get(
                            "breakout_price", breakout_bar.close
                        ),
                        breakout_close_pct=breakout_close_pct,
                        breakout_wick_pct=breakout_wick_pct,
                        entry_bar_start=bar.start,
                        entry_bar_end=bar.end,
                        entry_bar_open=bar.open,
                        entry_bar_close=bar.close,
                        entry_bar_high=bar.high,
                        entry_bar_low=bar.low,
                        next_bar_start=next_bar.start if next_bar else None,
                        next_bar_end=next_bar.end if next_bar else None,
                        next_bar_open=next_bar.open if next_bar else None,
                        next_bar_high=next_bar.high if next_bar else None,
                        next_bar_low=next_bar.low if next_bar else None,
                        next_bar_close=next_bar.close if next_bar else None,
                        retest_size_pct=retest_size_pct,
                        retest_body_inside_pct=retest_body_inside_pct,
                        wait_minutes=wait_minutes,
                        stop_price=stop_price,
                        post_times=post_times,
                        post_highs=post_highs,
                        post_lows=post_lows,
                        session_close_time=session_close_time,
                        session_close_price=session_close_price,
                    )
                )
                pending_breakout = None
                continue

            if pending_breakout["direction"] == "above":
                if bar.close > range_high:
                    pending_breakout["stop"] = max(pending_breakout["stop"], bar.high)
                elif bar.close < range_low:
                    pending_breakout = {
                        "direction": "below",
                        "stop": bar.low,
                        "time": bar.end,
                        "breakout_bar": bar,
                    }
                continue
            else:
                if bar.close < range_low:
                    pending_breakout["stop"] = min(pending_breakout["stop"], bar.low)
                elif bar.close > range_high:
                    pending_breakout = {
                        "direction": "above",
                        "stop": bar.high,
                        "time": bar.end,
                        "breakout_bar": bar,
                    }
                continue

        if bar.close > range_high:
            pending_breakout = {
                "direction": "above",
                "stop": bar.high,
                "time": bar.end,
                "breakout_bar": bar,
            }
        elif bar.close < range_low:
            pending_breakout = {
                "direction": "below",
                "stop": bar.low,
                "time": bar.end,
                "breakout_bar": bar,
            }

    return candidates


def _compute_breakout_close_pct(
    bar: Bar,
    *,
    direction: str,
    range_high: float,
    range_low: float,
    range_size: float,
) -> float:
    if range_size <= 0:
        return 0.0
    if direction == "short":
        return max(0.0, (bar.close - range_high) / range_size)
    return max(0.0, (range_low - bar.close) / range_size)


def _compute_retest_size_pct(bar: Bar, range_size: float) -> Optional[float]:
    candle_range = bar.high - bar.low
    if range_size <= 0:
        return None
    if candle_range < 0:
        return None
    return candle_range / range_size


def precompute_outcomes(
    candidates_by_day: Dict[datetime, List[CandidateTrade]],
    rr_values: Sequence[float],
    *,
    entry_mode: Literal["open", "close"],
) -> Dict[Tuple[datetime, int, float], PrecomputedOutcome]:
    outcomes: Dict[Tuple[datetime, int, float], PrecomputedOutcome] = {}
    unique_rrs = sorted(set(rr_values))
    for day_start, candidates in candidates_by_day.items():
        for idx, candidate in enumerate(candidates):
            for rr in unique_rrs:
                exit_time, trade, status = _simulate_trade_from_candidate(
                    candidate, rr, entry_mode=entry_mode
                )
                outcomes[(day_start, idx, rr)] = PrecomputedOutcome(
                    exit_time=exit_time,
                    trade=trade,
                    status=status,
                )
    return outcomes


def _simulate_trade_from_candidate(
    candidate: CandidateTrade,
    rr: float,
    *,
    entry_mode: Literal["open", "close"],
) -> Tuple[datetime, Optional[TradeResult], str]:
    post_times = candidate.post_times
    post_highs = candidate.post_highs
    post_lows = candidate.post_lows

    if entry_mode == "open":
        if (
            candidate.next_bar_start is None
            or candidate.next_bar_open is None
            or candidate.next_bar_high is None
            or candidate.next_bar_low is None
        ):
            fallback_time = candidate.entry_bar_end
            return fallback_time, None, "invalid"
        entry_time = candidate.next_bar_start
        entry_price = candidate.next_bar_open
        entry_high = candidate.next_bar_high
        entry_low = candidate.next_bar_low
        post_times_iter = post_times[1:]
        post_highs_iter = post_highs[1:]
        post_lows_iter = post_lows[1:]
    else:
        entry_time = candidate.entry_bar_end
        entry_price = candidate.entry_bar_close
        entry_high = candidate.entry_bar_high
        entry_low = candidate.entry_bar_low
        post_times_iter = post_times
        post_highs_iter = post_highs
        post_lows_iter = post_lows

    stop_price = candidate.stop_price
    risk = abs(entry_price - stop_price)
    if risk <= 0:
        return entry_time, None, "invalid"

    if candidate.direction == "short":
        target_price = entry_price - rr * risk
    else:
        target_price = entry_price + rr * risk

    if candidate.direction == "long":
        stop_hit = entry_low <= stop_price
        target_hit = entry_high >= target_price
    else:
        stop_hit = entry_high >= stop_price
        target_hit = entry_low <= target_price

    if entry_mode == "open":
        if stop_hit:
            trade = TradeResult(
                day=candidate.day,
                direction=candidate.direction,
                entry_time=entry_time,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                exit_time=entry_time,
                exit_price=stop_price,
                rr=rr,
                pnl_r=-1.0,
                exit_reason="stop",
                range_high=candidate.range_high,
                range_low=candidate.range_low,
                breakout_time=candidate.breakout_time,
                breakout_price=candidate.breakout_price,
            )
            return entry_time, trade, "stop"
        if target_hit:
            trade = TradeResult(
                day=candidate.day,
                direction=candidate.direction,
                entry_time=entry_time,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                exit_time=entry_time,
                exit_price=target_price,
                rr=rr,
                pnl_r=rr,
                exit_reason="target",
                range_high=candidate.range_high,
                range_low=candidate.range_low,
                breakout_time=candidate.breakout_time,
                breakout_price=candidate.breakout_price,
            )
            return entry_time, trade, "target"
    else:
        if stop_hit:
            trade = TradeResult(
                day=candidate.day,
                direction=candidate.direction,
                entry_time=entry_time,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                exit_time=entry_time,
                exit_price=stop_price,
                rr=rr,
                pnl_r=-1.0,
                exit_reason="stop",
                range_high=candidate.range_high,
                range_low=candidate.range_low,
                breakout_time=candidate.breakout_time,
                breakout_price=candidate.breakout_price,
            )
            return entry_time, trade, "stop"
        if target_hit:
            # Target on the entry bar ignored when entering at close
            pass

    for time, high, low in zip(post_times_iter, post_highs_iter, post_lows_iter):
        if candidate.direction == "long":
            stop_hit = low <= stop_price
            target_hit = high >= target_price
        else:
            stop_hit = high >= stop_price
            target_hit = low <= target_price

        if stop_hit and target_hit:
            return time, None, "cancel"
        if stop_hit:
            trade = TradeResult(
                day=candidate.day,
                direction=candidate.direction,
                entry_time=entry_time,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                exit_time=time,
                exit_price=stop_price,
                rr=rr,
                pnl_r=-1.0,
                exit_reason="stop",
                range_high=candidate.range_high,
                range_low=candidate.range_low,
                breakout_time=candidate.breakout_time,
                breakout_price=candidate.breakout_price,
            )
            return time, trade, "stop"
        if target_hit:
            trade = TradeResult(
                day=candidate.day,
                direction=candidate.direction,
                entry_time=entry_time,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                exit_time=time,
                exit_price=target_price,
                rr=rr,
                pnl_r=rr,
                exit_reason="target",
                range_high=candidate.range_high,
                range_low=candidate.range_low,
                breakout_time=candidate.breakout_time,
                breakout_price=candidate.breakout_price,
            )
            return time, trade, "target"

    exit_time = candidate.session_close_time
    exit_price = candidate.session_close_price
    pnl_r = (
        (exit_price - entry_price) / risk
        if candidate.direction == "long"
        else (entry_price - exit_price) / risk
    )
    trade = TradeResult(
        day=candidate.day,
        direction=candidate.direction,
        entry_time=entry_time,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        exit_time=exit_time,
        exit_price=exit_price,
        rr=rr,
        pnl_r=pnl_r,
        exit_reason="session_close",
        range_high=candidate.range_high,
        range_low=candidate.range_low,
        breakout_time=candidate.breakout_time,
        breakout_price=candidate.breakout_price,
    )
    return exit_time, trade, "session_close"


def _effective_value(
    base: Optional[float], override: Optional[float]
) -> Optional[float]:
    return override if override is not None else base


def _range_size_allowed(
    range_size: float,
    min_size: Optional[float],
    max_size: Optional[float],
) -> bool:
    if min_size is not None and range_size < min_size:
        return False
    if max_size is not None and range_size > max_size:
        return False
    return True


def _candidate_filter_check(
    candidate: CandidateTrade,
    *,
    entry_time: datetime,
    session_close_time: Optional[datetime],
    min_breakout_pct: Optional[float],
    max_breakout_pct: Optional[float],
    min_breakout_wick_pct: Optional[float],
    max_breakout_wick_pct: Optional[float],
    min_wait_minutes: Optional[float],
    max_wait_minutes: Optional[float],
    min_retest_size_pct: Optional[float],
    max_retest_size_pct: Optional[float],
    min_retest_body_inside_pct: Optional[float],
    max_entry_minutes_from_box_close: Optional[float],
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if session_close_time is not None and entry_time >= session_close_time:
        reasons.append(
            "entrée après la clôture forcée de 16h45 NY"
        )
    if max_entry_minutes_from_box_close is not None:
        delta_minutes = (entry_time - candidate.range_end_time).total_seconds() / 60.0
        if delta_minutes > max_entry_minutes_from_box_close:
            reasons.append(
                f"entrée {delta_minutes:.1f}m > max_minutes_box {max_entry_minutes_from_box_close:.1f}m"
            )
    if min_breakout_pct is not None and candidate.breakout_close_pct < min_breakout_pct:
        reasons.append(
            f"close% {candidate.breakout_close_pct:.3f} < min_breakout_pct {min_breakout_pct:.3f}"
        )
    if max_breakout_pct is not None and candidate.breakout_close_pct > max_breakout_pct:
        reasons.append(
            f"close% {candidate.breakout_close_pct:.3f} > max_breakout_pct {max_breakout_pct:.3f}"
        )
    if min_breakout_wick_pct is not None:
        wick_val = candidate.breakout_wick_pct
        if wick_val is None or wick_val < min_breakout_wick_pct:
            measured = "N/A" if wick_val is None else f"{wick_val:.3f}"
            reasons.append(
                f"wick% {measured} < min_breakout_wick_pct {min_breakout_wick_pct:.3f}"
            )
    if max_breakout_wick_pct is not None:
        wick_val = candidate.breakout_wick_pct
        if wick_val is None or wick_val > max_breakout_wick_pct:
            measured = "N/A" if wick_val is None else f"{wick_val:.3f}"
            reasons.append(
                f"wick% {measured} > max_breakout_wick_pct {max_breakout_wick_pct:.3f}"
            )
    if min_wait_minutes is not None and candidate.wait_minutes < min_wait_minutes:
        reasons.append(
            f"attente {candidate.wait_minutes:.1f}m < min_reentry_minutes {min_wait_minutes:.1f}m"
        )
    if max_wait_minutes is not None and candidate.wait_minutes > max_wait_minutes:
        reasons.append(
            f"attente {candidate.wait_minutes:.1f}m > max_reentry_minutes {max_wait_minutes:.1f}m"
        )
    if min_retest_size_pct is not None:
        size_val = candidate.retest_size_pct
        if size_val is None or size_val < min_retest_size_pct:
            measured = "N/A" if size_val is None else f"{size_val:.3f}"
            reasons.append(
                f"taille_retest {measured} < min_retest_size_pct {min_retest_size_pct:.3f}"
            )
    if max_retest_size_pct is not None:
        size_val = candidate.retest_size_pct
        if size_val is None or size_val > max_retest_size_pct:
            measured = "N/A" if size_val is None else f"{size_val:.3f}"
            reasons.append(
                f"taille_retest {measured} > max_retest_size_pct {max_retest_size_pct:.3f}"
            )
    if min_retest_body_inside_pct is not None:
        body_val = candidate.retest_body_inside_pct
        if body_val is None or body_val < min_retest_body_inside_pct:
            measured = "N/A" if body_val is None else f"{body_val:.3f}"
            reasons.append(
                f"corps_dans_box {measured} < min_retest_body_inside_pct {min_retest_body_inside_pct:.3f}"
            )
    return len(reasons) == 0, reasons


def _run_parameter_combo(
    candidates_by_day: Dict[datetime, List[CandidateTrade]],
    precomputed: Dict[Tuple[datetime, int, float], PrecomputedOutcome],
    *,
    rr: float,
    min_breakout_pct: Optional[float],
    min_breakout_wick_pct: Optional[float],
    max_breakout_pct: Optional[float],
    max_reentry_minutes: Optional[int],
    min_reentry_minutes: Optional[int],
    min_box_size: Optional[float],
    max_box_size: Optional[float],
    min_retest_size_pct: Optional[float],
    max_retest_size_pct: Optional[float],
    min_retest_body_inside_pct: Optional[float],
    max_entry_minutes_from_box_close: Optional[int],
    min_ticks_to_stop: Optional[float],
    max_ticks_to_stop: Optional[float],
    tick_size: float,
    max_trades_per_day: Optional[int],
    max_trades_per_direction: Optional[int],
    prevent_overlap: bool,
    entry_mode: Literal["open", "close"],
    trade_collector: Optional[List[TradeResult]] = None,
) -> ComboSummary:
    total_trades = 0
    wins = 0
    losses = 0
    total_r = 0.0

    for day_start, day_candidates in sorted(candidates_by_day.items()):
        if not day_candidates:
            continue

        range_size = day_candidates[0].range_size
        if not _range_size_allowed(range_size, min_box_size, max_box_size):
            continue

        trades_taken = 0
        direction_counts = {"long": 0, "short": 0}
        active_until: Optional[datetime] = None

        for idx, candidate in enumerate(day_candidates):
            candidate_entry_time = (
                candidate.next_bar_start if entry_mode == "open" else candidate.entry_bar_end
            )
            if candidate_entry_time is None:
                continue
            entry_price = (
                candidate.next_bar_open if entry_mode == "open" else candidate.entry_bar_close
            )
            if entry_price is None:
                continue
            if prevent_overlap and active_until is not None and candidate_entry_time < active_until:
                continue
            if prevent_overlap:
                active_until = None

            if max_trades_per_day is not None and trades_taken >= max_trades_per_day:
                break

            direction = candidate.direction
            if (
                max_trades_per_direction is not None
                and direction_counts[direction] >= max_trades_per_direction
            ):
                continue

            passes_filters, reasons = _candidate_filter_check(
                candidate,
                entry_time=candidate_entry_time,
                session_close_time=candidate.session_close_time,
                min_breakout_pct=min_breakout_pct,
                max_breakout_pct=max_breakout_pct,
                min_breakout_wick_pct=min_breakout_wick_pct,
                max_breakout_wick_pct=None,
                min_wait_minutes=min_reentry_minutes,
                max_wait_minutes=max_reentry_minutes,
                min_retest_size_pct=min_retest_size_pct,
                max_retest_size_pct=max_retest_size_pct,
                min_retest_body_inside_pct=min_retest_body_inside_pct,
                max_entry_minutes_from_box_close=max_entry_minutes_from_box_close,
            )
            reasons_list = list(reasons)
            failed = not passes_filters

            ticks_to_stop: Optional[float] = None
            if tick_size > 0:
                ticks_to_stop = abs(entry_price - candidate.stop_price) / tick_size
            if min_ticks_to_stop is not None:
                if ticks_to_stop is None or ticks_to_stop < min_ticks_to_stop:
                    measured = "N/A" if ticks_to_stop is None else f"{ticks_to_stop:.2f}"
                    reasons_list.append(
                        f"ticks_stop {measured} < min_ticks_to_stop {min_ticks_to_stop:.2f}"
                    )
                    failed = True
            if max_ticks_to_stop is not None:
                if ticks_to_stop is None or ticks_to_stop > max_ticks_to_stop:
                    measured = "N/A" if ticks_to_stop is None else f"{ticks_to_stop:.2f}"
                    reasons_list.append(
                        f"ticks_stop {measured} > max_ticks_to_stop {max_ticks_to_stop:.2f}"
                    )
                    failed = True

            if failed:
                continue

            outcome_key = (day_start, idx, rr)
            outcome = precomputed.get(outcome_key)
            if outcome is None:
                outcome = PrecomputedOutcome(
                    exit_time=candidate_entry_time,
                    trade=None,
                    status="missing",
                )
            resolution_time = outcome.exit_time
            trade = outcome.trade
            if prevent_overlap:
                active_until = resolution_time

            if trade is None:
                continue

            trades_taken += 1
            direction_counts[direction] += 1
            total_trades += 1
            total_r += trade.pnl_r
            if trade_collector is not None:
                trade_collector.append(trade)
            if trade.pnl_r > 0:
                wins += 1
            elif trade.pnl_r < 0:
                losses += 1

    avg_r = total_r / total_trades if total_trades else 0.0
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0
    return ComboSummary(
        rr=rr,
        min_breakout_pct=min_breakout_pct,
        min_breakout_wick_pct=min_breakout_wick_pct,
        max_breakout_pct=max_breakout_pct,
        max_reentry_minutes=max_reentry_minutes,
        min_box_size=min_box_size,
        max_box_size=max_box_size,
        min_retest_size_pct=min_retest_size_pct,
        max_retest_size_pct=max_retest_size_pct,
        min_retest_body_inside_pct=min_retest_body_inside_pct,
        max_entry_minutes_from_box_close=max_entry_minutes_from_box_close,
        min_ticks_to_stop=min_ticks_to_stop,
        max_ticks_to_stop=max_ticks_to_stop,
        trades=total_trades,
        wins=wins,
        losses=losses,
        avg_r=avg_r,
        total_r=total_r,
        win_rate=win_rate,
    )


def collect_trades_for_summary(
    candidates_by_day: Dict[datetime, List[CandidateTrade]],
    precomputed: Dict[Tuple[datetime, int, float], PrecomputedOutcome],
    summary: ComboSummary,
    *,
    max_trades_per_day: Optional[int],
    max_trades_per_direction: Optional[int],
    min_reentry_minutes: Optional[int],
    prevent_overlap: bool,
    entry_mode: Literal["open", "close"],
    tick_size: float,
) -> List[TradeResult]:
    trades: List[TradeResult] = []
    _run_parameter_combo(
        candidates_by_day,
        precomputed,
        rr=summary.rr,
        min_breakout_pct=summary.min_breakout_pct,
        min_breakout_wick_pct=summary.min_breakout_wick_pct,
        max_breakout_pct=summary.max_breakout_pct,
        max_reentry_minutes=summary.max_reentry_minutes,
        min_reentry_minutes=min_reentry_minutes,
        min_box_size=summary.min_box_size,
        max_box_size=summary.max_box_size,
        min_retest_size_pct=summary.min_retest_size_pct,
        max_retest_size_pct=summary.max_retest_size_pct,
        min_retest_body_inside_pct=summary.min_retest_body_inside_pct,
        max_entry_minutes_from_box_close=summary.max_entry_minutes_from_box_close,
        min_ticks_to_stop=summary.min_ticks_to_stop,
        max_ticks_to_stop=summary.max_ticks_to_stop,
        tick_size=tick_size,
        max_trades_per_day=max_trades_per_day,
        max_trades_per_direction=max_trades_per_direction,
        prevent_overlap=prevent_overlap,
        entry_mode=entry_mode,
        trade_collector=trades,
    )
    trades.sort(key=lambda trade: trade.entry_time)
    return trades


def sweep_parameter_grid(
    candidates_by_day: Dict[datetime, List[CandidateTrade]],
    *,
    rr_values: Sequence[float],
    min_breakout_pcts: Sequence[Optional[float]],
    min_breakout_wick_pcts: Sequence[Optional[float]],
    max_breakout_pcts: Sequence[Optional[float]],
    max_reentry_minutes: Sequence[Optional[int]],
    min_box_sizes: Sequence[Optional[float]],
    max_box_sizes: Sequence[Optional[float]],
    min_retest_size_pcts: Sequence[Optional[float]],
    max_retest_size_pcts: Sequence[Optional[float]],
    min_retest_body_inside_pcts: Sequence[Optional[float]],
    max_minutes_after_box_close: Sequence[Optional[int]],
    min_ticks_to_stop: Sequence[Optional[float]],
    max_ticks_to_stop: Sequence[Optional[float]],
    max_trades_per_day: Optional[int],
    max_trades_per_direction: Optional[int],
    min_reentry_minutes: Optional[int],
    prevent_overlap: bool,
    entry_mode: Literal["open", "close"],
    tick_size: float,
    precomputed: Optional[
        Dict[Tuple[datetime, int, float], PrecomputedOutcome]
    ] = None,
) -> List[ComboSummary]:
    results: List[ComboSummary] = []
    if precomputed is None:
        precomputed = precompute_outcomes(
            candidates_by_day, rr_values, entry_mode=entry_mode
        )
    for rr in rr_values:
        for min_breakout_pct in min_breakout_pcts:
            for min_breakout_wick_pct in min_breakout_wick_pcts:
                for max_breakout_pct in max_breakout_pcts:
                    for reentry_minutes in max_reentry_minutes:
                        for min_box in min_box_sizes:
                            for max_box in max_box_sizes:
                                for min_retest_size in min_retest_size_pcts:
                                    for max_retest_size in max_retest_size_pcts:
                                        for min_body_inside in min_retest_body_inside_pcts:
                                            for max_entry_delay in max_minutes_after_box_close:
                                                for min_ticks in min_ticks_to_stop:
                                                    for max_ticks in max_ticks_to_stop:
                                                        summary = _run_parameter_combo(
                                                            candidates_by_day,
                                                            precomputed,
                                                            rr=rr,
                                                            min_breakout_pct=min_breakout_pct,
                                                            min_breakout_wick_pct=min_breakout_wick_pct,
                                                            max_breakout_pct=max_breakout_pct,
                                                            max_reentry_minutes=reentry_minutes,
                                                            min_reentry_minutes=min_reentry_minutes,
                                                            min_box_size=min_box,
                                                            max_box_size=max_box,
                                                            min_retest_size_pct=min_retest_size,
                                                            max_retest_size_pct=max_retest_size,
                                                            min_retest_body_inside_pct=min_body_inside,
                                                            max_entry_minutes_from_box_close=max_entry_delay,
                                                            min_ticks_to_stop=min_ticks,
                                                            max_ticks_to_stop=max_ticks,
                                                            tick_size=tick_size,
                                                            max_trades_per_day=max_trades_per_day,
                                                            max_trades_per_direction=max_trades_per_direction,
                                                            prevent_overlap=prevent_overlap,
                                                            entry_mode=entry_mode,
                                                        )
                                                        results.append(summary)
    return results


def _check_trade_exit(
    bar: Bar, trade: dict, *, is_entry_bar: bool = False
) -> Tuple[str, Optional[dict]]:
    direction = trade["direction"]
    stop_price = trade["stop_price"]
    target_price = trade["target_price"]
    entry_mode: Literal["open", "close"] = trade.get("entry_mode", "close")

    if direction == "long":
        stop_hit = bar.low <= stop_price
        target_hit = bar.high >= target_price
    else:
        stop_hit = bar.high >= stop_price
        target_hit = bar.low <= target_price

    if is_entry_bar:
        if entry_mode == "open":
            if stop_hit:
                return "exit", {"price": stop_price, "pnl_r": -1.0, "reason": "stop"}
            if target_hit:
                return "exit", {
                    "price": target_price,
                    "pnl_r": trade["rr"],
                    "reason": "target",
                }
            return "hold", None
        else:
            if stop_hit:
                return "exit", {"price": stop_price, "pnl_r": -1.0, "reason": "stop"}
            if target_hit:
                return "hold", None
            return "hold", None

    if stop_hit and target_hit:
        return "cancel", None
    if stop_hit:
        return "exit", {"price": stop_price, "pnl_r": -1.0, "reason": "stop"}
    if target_hit:
        return "exit", {"price": target_price, "pnl_r": trade["rr"], "reason": "target"}
    return "hold", None


def _breakout_wick_ok(
    bar: Bar,
    *,
    direction: str,
    min_pct: Optional[float],
) -> bool:
    if min_pct is None:
        return True
    ratio = _breakout_wick_ratio(
        bar,
        direction=direction,
    )
    return ratio >= min_pct


def _breakout_wick_ratio(
    bar: Bar,
    *,
    direction: str,
) -> float:
    candle_range = bar.high - bar.low
    if candle_range <= 0:
        return 0.0
    if direction == "above":
        wick = max(0.0, bar.high - max(bar.open, bar.close))
    else:
        wick = max(0.0, min(bar.open, bar.close) - bar.low)
    return wick / candle_range


def _retest_filters_ok(
    bar: Bar,
    *,
    range_high: float,
    range_low: float,
    range_size: float,
    min_size_pct: Optional[float],
    max_size_pct: Optional[float],
    min_body_inside_pct: Optional[float],
) -> bool:
    candle_range = bar.high - bar.low
    if range_size <= 0:
        size_ratio = None
    else:
        size_ratio = candle_range / range_size if candle_range >= 0 else None

    if min_size_pct is not None:
        if size_ratio is None or size_ratio < min_size_pct:
            return False
    if max_size_pct is not None:
        if size_ratio is None or size_ratio > max_size_pct:
            return False

    if min_body_inside_pct is not None:
        inside_ratio = _retest_body_inside_ratio(
            bar,
            range_high=range_high,
            range_low=range_low,
        )
        if inside_ratio is None or inside_ratio < min_body_inside_pct:
            return False

    return True


def _retest_body_inside_ratio(bar: Bar, *, range_high: float, range_low: float) -> Optional[float]:
    body_top = max(bar.open, bar.close)
    body_bottom = min(bar.open, bar.close)
    body_size = body_top - body_bottom
    if body_size <= 0:
        return None
    inside_high = min(body_top, range_high)
    inside_low = max(body_bottom, range_low)
    inside = max(0.0, inside_high - inside_low)
    return inside / body_size


def _pnl_from_exit(exit_price: float, trade: dict) -> float:
    if trade["direction"] == "long":
        return (exit_price - trade["entry_price"]) / trade["risk"]
    return (trade["entry_price"] - exit_price) / trade["risk"]


def evaluate_rrs(
    days: List[DayData],
    rr_values: Sequence[float],
    min_avg_r: float,
    min_trades: int,
    max_trades_per_day: Optional[int],
    max_trades_per_direction: Optional[int],
    max_reentry_minutes: Optional[int],
    min_reentry_minutes: Optional[int],
    min_breakout_pct: float,
    min_breakout_wick_pct: Optional[float],
    max_breakout_pct: Optional[float],
    min_box_size: Optional[float],
    max_box_size: Optional[float],
    min_retest_size_pct: Optional[float],
    max_retest_size_pct: Optional[float],
    min_retest_body_inside_pct: Optional[float],
    max_entry_minutes_from_box_close: Optional[int],
    min_ticks_to_stop: Optional[float],
    max_ticks_to_stop: Optional[float],
    tick_size: float,
    prevent_overlap: bool,
    entry_mode: Literal["open", "close"],
) -> Tuple[float, List[TradeResult]]:
    candidates_by_day = collect_candidate_trades(days)
    if not candidates_by_day:
        raise RuntimeError("No candidate trades generated for evaluation.")
    precomputed = precompute_outcomes(
        candidates_by_day, rr_values, entry_mode=entry_mode
    )
    best_summary: Optional[ComboSummary] = None
    for rr in rr_values:
        collected: List[TradeResult] = []
        summary = _run_parameter_combo(
            candidates_by_day,
            precomputed,
            rr=rr,
            min_breakout_pct=min_breakout_pct,
            min_breakout_wick_pct=min_breakout_wick_pct,
            max_breakout_pct=max_breakout_pct,
            max_reentry_minutes=max_reentry_minutes,
            min_reentry_minutes=min_reentry_minutes,
            min_box_size=min_box_size,
            max_box_size=max_box_size,
            min_retest_size_pct=min_retest_size_pct,
            max_retest_size_pct=max_retest_size_pct,
            min_retest_body_inside_pct=min_retest_body_inside_pct,
            max_entry_minutes_from_box_close=max_entry_minutes_from_box_close,
            min_ticks_to_stop=min_ticks_to_stop,
            max_ticks_to_stop=max_ticks_to_stop,
            tick_size=tick_size,
            max_trades_per_day=max_trades_per_day,
            max_trades_per_direction=max_trades_per_direction,
            prevent_overlap=prevent_overlap,
            entry_mode=entry_mode,
            trade_collector=collected,
        )
        if not collected:
            continue
        if (
            best_summary is None
            or summary.avg_r > best_summary.avg_r
            or (
                summary.avg_r == best_summary.avg_r
                and summary.total_r > best_summary.total_r
            )
        ):
            best_summary = summary
        if summary.trades >= min_trades and summary.avg_r >= min_avg_r:
            trades = collect_trades_for_summary(
                candidates_by_day,
                precomputed,
                summary,
                max_trades_per_day=max_trades_per_day,
                max_trades_per_direction=max_trades_per_direction,
                min_reentry_minutes=min_reentry_minutes,
                prevent_overlap=prevent_overlap,
                entry_mode=entry_mode,
                tick_size=tick_size,
            )
            return summary.rr, trades
    if best_summary is not None:
        print(
            "Warning: no RR met the constraints; returning the best available setup",
            f"(RR={best_summary.rr}, average R={best_summary.avg_r:.4f}).",
        )
        trades = collect_trades_for_summary(
            candidates_by_day,
            precomputed,
            best_summary,
            max_trades_per_day=max_trades_per_day,
            max_trades_per_direction=max_trades_per_direction,
            min_reentry_minutes=min_reentry_minutes,
            prevent_overlap=prevent_overlap,
            entry_mode=entry_mode,
            tick_size=tick_size,
        )
        return best_summary.rr, trades
    raise RuntimeError("No trades generated for the provided parameters.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="Directory containing the CSV file (or zip parts) to load.",
    )
    parser.add_argument(
        "--data-file",
        type=str,
        default=DATA_FILE_NAME,
        help="Name of the CSV file to load inside data-dir (e.g. NQ10.csv).",
    )
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=None,
        help="Optional path for the parsed data cache (defaults to data-dir / <stem>_days.pkl).",
    )
    parser.set_defaults(use_cache=USE_DATA_CACHE)
    parser.add_argument(
        "--use-cache",
        dest="use_cache",
        action="store_true",
        help="Load/store the parsed data cache (enabled by default).",
    )
    parser.add_argument(
        "--no-cache",
        dest="use_cache",
        action="store_false",
        help="Disable the parsed data cache and re-read the raw CSV/zip parts.",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        dest="sweep",
        default=RUN_SWEEP_BY_DEFAULT,
        help="Run the fast parameter sweep instead of a single RR evaluation.",
    )
    parser.add_argument(
        "--no-sweep",
        action="store_false",
        dest="sweep",
        help="Disable the parameter sweep and evaluate only the requested RR range.",
    )
    parser.add_argument("--sweep-top-n", type=int, default=SWEEP_TOP_N)
    parser.add_argument("--min-avg-r", type=float, default=MIN_AVG_R)
    parser.add_argument("--min-trades", type=int, default=MIN_TRADES)
    parser.add_argument("--rr-start", type=float, default=RR_START)
    parser.add_argument("--rr-end", type=float, default=RR_END)
    parser.add_argument("--rr-step", type=float, default=RR_STEP)
    parser.add_argument(
        "--export-trades",
        type=Path,
        help="Optional path to export the trade log as CSV.",
    )
    parser.add_argument(
        "--max-trades-per-day",
        type=int,
        default=MAX_TRADES_PER_DAY,
        help="Maximum number of trades to take per day (optional).",
    )
    parser.add_argument(
        "--max-trades-per-direction",
        type=int,
        default=MAX_TRADES_PER_DIRECTION,
        help="Maximum number of trades per day for the same direction (optional).",
    )
    parser.add_argument(
        "--max-reentry-minutes",
        type=int,
        default=MAX_REENTRY_MINUTES,
        help="Discard setups where the retest happens after this many minutes (optional).",
    )
    parser.add_argument(
        "--min-reentry-minutes",
        type=int,
        default=MIN_REENTRY_MINUTES,
        help="Ignore setups when the retest happens faster than this many minutes (optional).",
    )
    parser.add_argument(
        "--max-minutes-after-box",
        dest="max_minutes_after_box_close",
        type=int,
        default=MAX_MINUTES_AFTER_BOX_CLOSE,
        help=(
            "Ignore setups whose entry would occur more than this many minutes after the "
            "4h box closes (optional)."
        ),
    )
    parser.add_argument(
        "--min-breakout-pct",
        type=float,
        default=MIN_BREAKOUT_PCT,
        help=(
            "Require the breakout close to extend this fraction of the 4h range outside "
            "before tracking a retest (e.g. 0.2 = 20%%)."
        ),
    )
    parser.add_argument(
        "--min-breakout-wick-pct",
        type=float,
        default=MIN_BREAKOUT_WICK_PCT,
        help=(
            "Require the breakout candle's wick (on the breakout side) to be at least this "
            "fraction of the candle's full high-low range (e.g. 0.2 = 20%%)."
        ),
    )
    parser.add_argument(
        "--max-breakout-pct",
        type=float,
        default=MAX_BREAKOUT_PCT,
        help="Require the breakout close to remain within this fraction of the range outside (optional).",
    )
    parser.add_argument(
        "--min-box-size",
        type=float,
        default=MIN_BOX_SIZE,
        help="Skip days where the 4h range is smaller than this size (points).",
    )
    parser.add_argument(
        "--max-box-size",
        type=float,
        default=MAX_BOX_SIZE,
        help="Skip days where the 4h range exceeds this size (points).",
    )
    parser.add_argument(
        "--min-retest-size-pct",
        type=float,
        default=MIN_RETEST_SIZE_PCT,
        help=(
            "Require the retest candle's high-low range to be at least this fraction of the "
            "box height (e.g. 0.2 = 20%%)."
        ),
    )
    parser.add_argument(
        "--max-retest-size-pct",
        type=float,
        default=MAX_RETEST_SIZE_PCT,
        help=(
            "Require the retest candle's high-low range to be at most this fraction of the "
            "box height (e.g. 0.8 = 80%%)."
        ),
    )
    parser.add_argument(
        "--min-retest-body-inside-pct",
        type=float,
        default=MIN_RETEST_BODY_INSIDE_PCT,
        help=(
            "Require at least this fraction of the retest candle's body to close back "
            "inside the box (e.g. 0.5 = 50%%)."
        ),
    )
    parser.add_argument(
        "--tick-size",
        type=float,
        default=TICK_SIZE,
        help="Tick size used to convert stop distance into ticks (e.g. 0.25 for NQ).",
    )
    parser.add_argument(
        "--min-ticks-to-stop",
        type=float,
        default=MIN_TICKS_TO_STOP,
        help="Minimum number of ticks between entry and stop (optional).",
    )
    parser.add_argument(
        "--max-ticks-to-stop",
        type=float,
        default=MAX_TICKS_TO_STOP,
        help="Maximum number of ticks between entry and stop (optional).",
    )
    parser.add_argument(
        "--entry-mode",
        choices=["open", "close"],
        default=ENTRY_MODE,
        help=(
            "Choose whether entries are filled at the retest bar's open (target honoured on the "
            "entry bar) or close (target ignored on the entry bar)."
        ),
    )
    parser.set_defaults(prevent_overlap=PREVENT_OVERLAP)
    parser.add_argument(
        "--prevent-overlap",
        dest="prevent_overlap",
        action="store_true",
        help="Skip setups that would overlap an open trade.",
    )
    parser.add_argument(
        "--allow-overlap",
        dest="prevent_overlap",
        action="store_false",
        help="Allow overlapping trades (default).",
    )
    return parser.parse_args()


def frange(start: float, end: float, step: float) -> List[float]:
    values: List[float] = []
    current = start
    while current <= end + 1e-9:
        values.append(round(current, 10))
        current += step
    return values


def export_trades(trades: Iterable[TradeResult], path: Path) -> None:
    rows = [asdict(trade) for trade in trades]
    rows.sort(key=lambda row: row["entry_time"])
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def save_cumulative_chart(counts: Sequence[int], totals: Sequence[float], stem: str) -> Path:
    if HAVE_MATPLOTLIB and plt is not None:
        path = Path.cwd() / f"cumulative_r_{stem}.png"
        plt.figure(figsize=(10, 6))
        plt.plot(counts, totals, color="tab:blue")
        plt.xlabel("Nombre de trades")
        plt.ylabel("R cumulé")
        plt.title("Evolution du R total par trade")
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.tight_layout()
        plt.savefig(path)
        plt.close()
        return path

    path = Path.cwd() / f"cumulative_r_{stem}.svg"
    _write_svg_line_chart(path, counts, totals)
    return path


def _write_svg_line_chart(path: Path, counts: Sequence[int], totals: Sequence[float]) -> None:
    width, height = 960, 540
    margin = 60
    if not counts:
        path.write_text("<svg xmlns='http://www.w3.org/2000/svg' width='960' height='540'></svg>")
        return

    x_min, x_max = min(counts), max(counts)
    y_min, y_max = min(totals), max(totals)
    if x_min == x_max:
        x_min -= 1
        x_max += 1
    if y_min == y_max:
        y_min -= 1
        y_max += 1

    x_span = x_max - x_min
    y_span = y_max - y_min
    plot_width = width - 2 * margin
    plot_height = height - 2 * margin
    points = []
    for x, y in zip(counts, totals):
        norm_x = (x - x_min) / x_span
        norm_y = (y - y_min) / y_span
        px = margin + norm_x * plot_width
        py = height - margin - norm_y * plot_height
        points.append(f"{px:.2f},{py:.2f}")

    axis_y_zero = None
    if y_min <= 0 <= y_max:
        zero_norm = (0 - y_min) / y_span
        axis_y_zero = height - margin - zero_norm * plot_height

    svg_lines = [
        "<?xml version='1.0' encoding='UTF-8'?>",
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>",
        "  <style>text{font-family:Arial,sans-serif;font-size:16px;fill:#333}</style>",
        "  <rect x='0' y='0' width='100%' height='100%' fill='white' stroke='none' />",
        f"  <line x1='{margin}' y1='{height - margin}' x2='{width - margin}' y2='{height - margin}' stroke='#444' stroke-width='2' />",
        f"  <line x1='{margin}' y1='{margin}' x2='{margin}' y2='{height - margin}' stroke='#444' stroke-width='2' />",
    ]

    if axis_y_zero is not None:
        svg_lines.append(
            f"  <line x1='{margin}' y1='{axis_y_zero:.2f}' x2='{width - margin}' y2='{axis_y_zero:.2f}' stroke='#bbb' stroke-dasharray='6 6' />"
        )

    svg_lines.extend(
        [
            f"  <polyline points='{ ' '.join(points) }' fill='none' stroke='#1f77b4' stroke-width='2.5' />",
            f"  <text x='{width/2:.2f}' y='{margin/2:.2f}' text-anchor='middle'>Evolution du R total par trade</text>",
            f"  <text x='{width/2:.2f}' y='{height - margin/4:.2f}' text-anchor='middle'>Nombre de trades</text>",
            f"  <text transform='translate({margin/3:.2f},{height/2:.2f}) rotate(-90)' text-anchor='middle'>R cumulé</text>",
        ]
    )

    svg_lines.append("</svg>")
    path.write_text("\n".join(svg_lines))


def main() -> None:
    args = parse_args()
    days = load_day_data(
        args.data_dir,
        use_cache=args.use_cache,
        cache_path=args.cache_path,
        data_file=args.data_file,
    )
    if not days:
        raise SystemExit("No data loaded.")

    if args.sweep:
        candidates_by_day = collect_candidate_trades(days)
        if not candidates_by_day:
            raise SystemExit("No candidate trades generated for the sweep.")
        precomputed = precompute_outcomes(
            candidates_by_day, SWEEP_RR_VALUES, entry_mode=args.entry_mode
        )
        all_results = sweep_parameter_grid(
            candidates_by_day,
            rr_values=SWEEP_RR_VALUES,
            min_breakout_pcts=SWEEP_MIN_BREAKOUT_PCTS,
            min_breakout_wick_pcts=SWEEP_MIN_BREAKOUT_WICK_PCTS,
            max_breakout_pcts=SWEEP_MAX_BREAKOUT_PCTS,
            max_reentry_minutes=SWEEP_MAX_REENTRY_MINUTES,
            min_box_sizes=SWEEP_MIN_BOX_SIZES,
            max_box_sizes=SWEEP_MAX_BOX_SIZES,
            min_retest_size_pcts=SWEEP_MIN_RETEST_SIZE_PCTS,
            max_retest_size_pcts=SWEEP_MAX_RETEST_SIZE_PCTS,
            min_retest_body_inside_pcts=SWEEP_MIN_RETEST_BODY_INSIDE_PCTS,
            max_minutes_after_box_close=SWEEP_MAX_MINUTES_AFTER_BOX_CLOSE,
            min_ticks_to_stop=SWEEP_MIN_TICKS_TO_STOP,
            max_ticks_to_stop=SWEEP_MAX_TICKS_TO_STOP,
            max_trades_per_day=args.max_trades_per_day,
            max_trades_per_direction=args.max_trades_per_direction,
            min_reentry_minutes=args.min_reentry_minutes,
            prevent_overlap=args.prevent_overlap,
            entry_mode=args.entry_mode,
            tick_size=args.tick_size,
            precomputed=precomputed,
        )
        qualified = [
            result
            for result in all_results
            if result.trades >= args.min_trades and result.avg_r >= args.min_avg_r
        ]
        qualified.sort(
            key=lambda res: (res.avg_r, res.total_r, res.trades),
            reverse=True,
        )
        print(f"Parameter combinations tested: {len(all_results)}")
        print(
            f"Combinations meeting >= {args.min_trades} trades and avg R >= {args.min_avg_r}:"
            f" {len(qualified)}"
        )
        top_n = qualified[: args.sweep_top_n]
        best_combo: Optional[ComboSummary] = None
        if not top_n:
            print("No parameter sets satisfied the requested constraints.")
            fallback = sorted(
                all_results,
                key=lambda res: (res.avg_r, res.total_r, res.trades),
                reverse=True,
            )[: args.sweep_top_n]
            if fallback:
                print("Best parameter sets overall (constraints ignored):")
                for rank, result in enumerate(fallback, start=1):
                    print(
                        f"#{rank}: RR={result.rr:.2f}, trades={result.trades}, avgR={result.avg_r:.4f}, "
                        f"totalR={result.total_r:.2f}, win%={result.win_rate:.2f}, "
                        f"minBreak={format_optional(result.min_breakout_pct)}, "
                        f"maxBreak={format_optional(result.max_breakout_pct)}, "
                        f"minWick={format_optional(result.min_breakout_wick_pct)}, "
                        f"maxRetestMin={result.max_reentry_minutes}, minBox={result.min_box_size}, "
                        f"maxBox={result.max_box_size}, minRetestSize={result.min_retest_size_pct}, "
                        f"maxRetestSize={result.max_retest_size_pct}, minBodyInside={result.min_retest_body_inside_pct}, "
                        f"maxEntryDelay={format_optional(result.max_entry_minutes_from_box_close, '.0f')}, "
                        f"minTicks={format_optional(result.min_ticks_to_stop, '.2f')}, "
                        f"maxTicks={format_optional(result.max_ticks_to_stop, '.2f')}"
                    )
                best_combo = fallback[0]
        else:
            print("Top parameter sets:")
            for rank, result in enumerate(top_n, start=1):
                print(
                    f"#{rank}: RR={result.rr:.2f}, trades={result.trades}, avgR={result.avg_r:.4f}, "
                    f"totalR={result.total_r:.2f}, win%={result.win_rate:.2f}, "
                    f"minBreak={format_optional(result.min_breakout_pct)}, "
                    f"maxBreak={format_optional(result.max_breakout_pct)}, "
                    f"minWick={format_optional(result.min_breakout_wick_pct)}, "
                    f"maxRetestMin={result.max_reentry_minutes}, minBox={result.min_box_size}, "
                    f"maxBox={result.max_box_size}, minRetestSize={result.min_retest_size_pct}, "
                    f"maxRetestSize={result.max_retest_size_pct}, minBodyInside={result.min_retest_body_inside_pct}, "
                    f"maxEntryDelay={format_optional(result.max_entry_minutes_from_box_close, '.0f')}, "
                    f"minTicks={format_optional(result.min_ticks_to_stop, '.2f')}, "
                    f"maxTicks={format_optional(result.max_ticks_to_stop, '.2f')}"
                )
            best_combo = top_n[0]

        if best_combo is None:
            return

        print("\nDétails pour le meilleur ensemble de paramètres:")
        trades = collect_trades_for_summary(
            candidates_by_day,
            precomputed,
            best_combo,
            max_trades_per_day=args.max_trades_per_day,
            max_trades_per_direction=args.max_trades_per_direction,
            min_reentry_minutes=args.min_reentry_minutes,
            prevent_overlap=args.prevent_overlap,
            entry_mode=args.entry_mode,
            tick_size=args.tick_size,
        )
        if not trades:
            print("Aucun trade n'a été généré pour cet ensemble.")
            return

        total_trades = len(trades)
        wins = sum(1 for trade in trades if trade.pnl_r > 0)
        losses = sum(1 for trade in trades if trade.pnl_r < 0)
        total_r = sum(trade.pnl_r for trade in trades)
        avg_r = total_r / total_trades if total_trades else 0.0
        win_rate = (wins / total_trades * 100.0) if total_trades else 0.0

        print(f"RR sélectionné: {best_combo.rr:.2f}")
        print(f"Nombre de trades: {total_trades}")
        print(f"Wins: {wins}, Losses: {losses}")
        print(f"Win rate: {win_rate:.2f}%")
        print(f"Average R per trade: {avg_r:.4f}")
        print(f"Total R: {total_r:.2f}")

        day_count = count_backtest_days(days)
        avg_trades_per_day = total_trades / day_count if day_count else 0.0
        print(
            "Trades moyens par jour (jours sans trade inclus): "
            f"{avg_trades_per_day:.4f} (sur {day_count} jours)"
        )

        print_monthly_summary(trades, 2025)

        trade_progress: List[Tuple[TradeResult, int, float]] = []
        cumulative = 0.0
        counts: List[int] = []
        totals: List[float] = []
        for idx, trade in enumerate(trades, start=1):
            cumulative += trade.pnl_r
            trade_progress.append((trade, idx, cumulative))
            counts.append(idx)
            totals.append(cumulative)

        plot_path = save_cumulative_chart(counts, totals, Path(args.data_file).stem)
        print(f"Graphique cumulatif sauvegardé: {plot_path}")

        print("Derniers 10 trades:")
        for trade, idx, cum_r in trade_progress[-10:]:
            print(format_trade_line(idx, trade, cum_r))

        if args.export_trades:
            export_trades(trades, args.export_trades)
            print(f"Trade log exported to {args.export_trades}")

        return

    rr_values = frange(args.rr_start, args.rr_end, args.rr_step)
    selected_rr, trades = evaluate_rrs(
        days=days,
        rr_values=rr_values,
        min_avg_r=args.min_avg_r,
        min_trades=args.min_trades,
        max_trades_per_day=args.max_trades_per_day,
        max_trades_per_direction=args.max_trades_per_direction,
        max_reentry_minutes=args.max_reentry_minutes,
        min_reentry_minutes=args.min_reentry_minutes,
        min_breakout_pct=args.min_breakout_pct,
        min_breakout_wick_pct=args.min_breakout_wick_pct,
        max_breakout_pct=args.max_breakout_pct,
        min_box_size=args.min_box_size,
        max_box_size=args.max_box_size,
        min_retest_size_pct=args.min_retest_size_pct,
        max_retest_size_pct=args.max_retest_size_pct,
        min_retest_body_inside_pct=args.min_retest_body_inside_pct,
        max_entry_minutes_from_box_close=args.max_minutes_after_box_close,
        min_ticks_to_stop=args.min_ticks_to_stop,
        max_ticks_to_stop=args.max_ticks_to_stop,
        tick_size=args.tick_size,
        prevent_overlap=args.prevent_overlap,
        entry_mode=args.entry_mode,
    )

    total_trades = len(trades)
    wins = sum(1 for trade in trades if trade.pnl_r > 0)
    losses = sum(1 for trade in trades if trade.pnl_r < 0)
    avg_r = sum(trade.pnl_r for trade in trades) / total_trades if total_trades else 0.0
    total_r = sum(trade.pnl_r for trade in trades)
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0

    print(f"Selected RR multiple: {selected_rr:.2f}")
    print(f"Number of trades: {total_trades}")
    print(f"Wins: {wins}, Losses: {losses}")
    print(f"Win rate: {win_rate:.2f}%")
    print(f"Average R per trade: {avg_r:.4f}")
    print(f"Total R: {total_r:.2f}")

    day_count = count_backtest_days(days)
    avg_trades_per_day = total_trades / day_count if day_count else 0.0
    print(
        "Trades moyens par jour (jours sans trade inclus): "
        f"{avg_trades_per_day:.4f} (sur {day_count} jours)"
    )

    print_monthly_summary(trades, 2025)

    trades_sorted = sorted(trades, key=lambda trade: trade.entry_time)
    if trades_sorted:
        trade_progress: List[Tuple[TradeResult, int, float]] = []
        cumulative = 0.0
        counts: List[int] = []
        totals: List[float] = []
        for idx, trade in enumerate(trades_sorted, start=1):
            cumulative += trade.pnl_r
            trade_progress.append((trade, idx, cumulative))
            counts.append(idx)
            totals.append(cumulative)

        plot_path = save_cumulative_chart(counts, totals, Path(args.data_file).stem)
        print(f"Graphique cumulatif sauvegardé: {plot_path}")

        print("Derniers 10 trades:")
        for trade, idx, cum_r in trade_progress[-10:]:
            print(format_trade_line(idx, trade, cum_r))

    if args.export_trades:
        export_trades(trades, args.export_trades)
        print(f"Trade log exported to {args.export_trades}")


if __name__ == "__main__":
    main()