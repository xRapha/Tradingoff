"""Backtest the 4-hour range scalping strategy on the NQ10 dataset."""
from __future__ import annotations

import argparse
import csv
import io
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
import zipfile

from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
NY_TZ = ZoneInfo("America/New_York")


# ==============================================
# CONFIGURATION
# ==============================================
DATA_DIR = Path(__file__).resolve().parent

# RR sweep + minimum performance requirements
RR_START = 1.0
RR_END = 3.0
RR_STEP = 0.1
MIN_AVG_R = 0.12
MIN_TRADES = 100

# Trade limits (None = disabled)
MAX_TRADES_PER_DAY: Optional[int] = None
MAX_TRADES_PER_DIRECTION: Optional[int] = None
MAX_REENTRY_MINUTES: Optional[int] = 5

# Breakout filters (None/0 = disabled)
MIN_BREAKOUT_PCT = 0.0  # fraction of the box height required outside on the close
MIN_BREAKOUT_WICK_PCT: Optional[float] = 0.0  # fraction of breakout candle wick outside the box

# 4H box size filters (points)
MIN_BOX_SIZE: Optional[float] = None
MAX_BOX_SIZE: Optional[float] = None

# Retest candle filters (fractions relative to box / candle body)
MIN_RETEST_SIZE_PCT: Optional[float] = None
MAX_RETEST_SIZE_PCT: Optional[float] = None
MIN_RETEST_BODY_INSIDE_PCT: Optional[float] = 0.3


@dataclass
class MinuteBar:
    time: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class Bar:
    time: datetime
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
        self.bars.append(
            Bar(
                time=self.last_time,
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


def load_day_data(data_dir: Path) -> List[DayData]:
    csv_path = data_dir / "NQ10.csv"
    if csv_path.exists():
        with csv_path.open("r", newline="") as handle:
            return _read_day_data(handle)

    parts = sorted(data_dir.glob("NQ10.zip.part*"))
    if not parts:
        raise FileNotFoundError("No NQ10.csv or NQ10.zip.part* files found")

    combined = io.BytesIO()
    for part in parts:
        combined.write(part.read_bytes())
    combined.seek(0)

    with zipfile.ZipFile(combined) as archive:
        with archive.open("NQ10.csv") as handle:
            text = io.TextIOWrapper(handle, encoding="utf-8")
            return _read_day_data(text)


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
    min_breakout_pct: float = 0.0,
    min_breakout_wick_pct: Optional[float] = None,
    min_box_size: Optional[float] = None,
    max_box_size: Optional[float] = None,
    min_retest_size_pct: Optional[float] = None,
    max_retest_size_pct: Optional[float] = None,
    min_retest_body_inside_pct: Optional[float] = None,
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
            if bar.time < range_end:
                continue

            if active_trade is not None:
                if bar.time > active_trade["entry_time"]:
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
                                exit_time=bar.time,
                                exit_price=info["price"],
                                rr=rr,
                                pnl_r=info["pnl_r"],
                                exit_reason=info["reason"],
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
                        and (bar.time - pending_breakout["time"]).total_seconds() / 60.0
                        > max_reentry_minutes
                    ):
                        pending_breakout = None
                        continue
                    entry_price = bar.close
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
                        "entry_time": bar.time,
                        "entry_price": entry_price,
                        "stop_price": stop_price,
                        "target_price": target_price,
                        "risk": risk,
                        "rr": rr,
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
                                exit_time=bar.time,
                                exit_price=info["price"],
                                rr=rr,
                                pnl_r=info["pnl_r"],
                                exit_reason=info["reason"],
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
                        if range_size == 0 or (range_low - bar.close) / range_size >= min_breakout_pct:
                            if _breakout_wick_ok(
                                bar,
                                direction="below",
                                range_high=range_high,
                                range_low=range_low,
                                min_pct=min_breakout_wick_pct,
                            ):
                                pending_breakout = {
                                    "direction": "below",
                                    "stop": bar.low,
                                    "time": bar.time,
                                    "breakout_bar": bar,
                                }
                            else:
                                pending_breakout = None
                        else:
                            pending_breakout = None
                else:
                    if bar.close < range_low:
                        pending_breakout["stop"] = min(pending_breakout["stop"], bar.low)
                    elif bar.close > range_high:
                        if range_size == 0 or (bar.close - range_high) / range_size >= min_breakout_pct:
                            if _breakout_wick_ok(
                                bar,
                                direction="above",
                                range_high=range_high,
                                range_low=range_low,
                                min_pct=min_breakout_wick_pct,
                            ):
                                pending_breakout = {
                                    "direction": "above",
                                    "stop": bar.high,
                                    "time": bar.time,
                                    "breakout_bar": bar,
                                }
                            else:
                                pending_breakout = None
                        else:
                            pending_breakout = None
                continue

            if bar.close > range_high:
                if range_size == 0 or (bar.close - range_high) / range_size >= min_breakout_pct:
                    if _breakout_wick_ok(
                        bar,
                        direction="above",
                        range_high=range_high,
                        range_low=range_low,
                        min_pct=min_breakout_wick_pct,
                    ):
                        pending_breakout = {
                            "direction": "above",
                            "stop": bar.high,
                            "time": bar.time,
                            "breakout_bar": bar,
                        }
            elif bar.close < range_low:
                if range_size == 0 or (range_low - bar.close) / range_size >= min_breakout_pct:
                    if _breakout_wick_ok(
                        bar,
                        direction="below",
                        range_high=range_high,
                        range_low=range_low,
                        min_pct=min_breakout_wick_pct,
                    ):
                        pending_breakout = {
                            "direction": "below",
                            "stop": bar.low,
                            "time": bar.time,
                            "breakout_bar": bar,
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
                    exit_time=last_bar.time,
                    exit_price=exit_price,
                    rr=rr,
                    pnl_r=pnl_r,
                    exit_reason="session_close",
                )
            )

    return trades


def _check_trade_exit(bar: Bar, trade: dict, *, is_entry_bar: bool = False) -> Tuple[str, Optional[dict]]:
    direction = trade["direction"]
    stop_price = trade["stop_price"]
    target_price = trade["target_price"]

    if direction == "long":
        stop_hit = bar.low <= stop_price
        target_hit = bar.high >= target_price
    else:
        stop_hit = bar.high >= stop_price
        target_hit = bar.low <= target_price

    if is_entry_bar:
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
    range_high: float,
    range_low: float,
    min_pct: Optional[float],
) -> bool:
    if min_pct is None:
        return True
    candle_range = bar.high - bar.low
    if candle_range <= 0:
        return False
    if direction == "above":
        body_top = max(bar.open, bar.close)
        outer = max(0.0, bar.high - max(range_high, body_top))
    else:
        body_bottom = min(bar.open, bar.close)
        outer = max(0.0, min(range_low, body_bottom) - bar.low)
    ratio = outer / candle_range
    return ratio >= min_pct


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
        body_top = max(bar.open, bar.close)
        body_bottom = min(bar.open, bar.close)
        body_size = body_top - body_bottom
        if body_size <= 0:
            return False
        inside_high = min(body_top, range_high)
        inside_low = max(body_bottom, range_low)
        inside = max(0.0, inside_high - inside_low)
        if inside / body_size < min_body_inside_pct:
            return False

    return True


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
    min_breakout_pct: float,
    min_breakout_wick_pct: Optional[float],
    min_box_size: Optional[float],
    max_box_size: Optional[float],
    min_retest_size_pct: Optional[float],
    max_retest_size_pct: Optional[float],
    min_retest_body_inside_pct: Optional[float],
) -> Tuple[float, List[TradeResult]]:
    best_result: Optional[Tuple[float, float, List[TradeResult]]] = None
    for rr in rr_values:
        trades = generate_trade_log(
            days,
            rr,
            max_trades_per_day=max_trades_per_day,
            max_trades_per_direction=max_trades_per_direction,
            max_reentry_minutes=max_reentry_minutes,
            min_breakout_pct=min_breakout_pct,
            min_breakout_wick_pct=min_breakout_wick_pct,
            min_box_size=min_box_size,
            max_box_size=max_box_size,
            min_retest_size_pct=min_retest_size_pct,
            max_retest_size_pct=max_retest_size_pct,
            min_retest_body_inside_pct=min_retest_body_inside_pct,
        )
        if not trades:
            continue
        avg_r = sum(trade.pnl_r for trade in trades) / len(trades)
        if best_result is None or avg_r > best_result[0]:
            best_result = (avg_r, rr, trades)
        if len(trades) >= min_trades and avg_r >= min_avg_r:
            return rr, trades
    if best_result is not None:
        best_avg, best_rr, best_trades = best_result
        print(
            "Warning: no RR met the constraints; returning the best available setup",
            f"(RR={best_rr}, average R={best_avg:.4f}).",
        )
        return best_rr, best_trades
    raise RuntimeError("No trades generated for the provided parameters.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="Directory containing NQ10.csv or the zip parts.",
    )
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
        "--min-breakout-pct",
        type=float,
        default=MIN_BREAKOUT_PCT,
        help=(
            "Require the breakout close to extend this fraction of the 4h range outside "
            "before tracking a retest (e.g. 0.2 = 20%)."
        ),
    )
    parser.add_argument(
        "--min-breakout-wick-pct",
        type=float,
        default=MIN_BREAKOUT_WICK_PCT,
        help=(
            "Require the breakout candle's outer wick to be at least this fraction of the "
            "candle range outside the box (e.g. 0.2 = 20%).",
        ),
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
            "box height (e.g. 0.2 = 20%).",
        ),
    )
    parser.add_argument(
        "--max-retest-size-pct",
        type=float,
        default=MAX_RETEST_SIZE_PCT,
        help=(
            "Require the retest candle's high-low range to be at most this fraction of the "
            "box height (e.g. 0.8 = 80%).",
        ),
    )
    parser.add_argument(
        "--min-retest-body-inside-pct",
        type=float,
        default=MIN_RETEST_BODY_INSIDE_PCT,
        help=(
            "Require at least this fraction of the retest candle's body to close back "
            "inside the box (e.g. 0.5 = 50%).",
        ),
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


def main() -> None:
    args = parse_args()
    days = load_day_data(args.data_dir)
    if not days:
        raise SystemExit("No data loaded.")

    rr_values = frange(args.rr_start, args.rr_end, args.rr_step)
    selected_rr, trades = evaluate_rrs(
        days=days,
        rr_values=rr_values,
        min_avg_r=args.min_avg_r,
        min_trades=args.min_trades,
        max_trades_per_day=args.max_trades_per_day,
        max_trades_per_direction=args.max_trades_per_direction,
        max_reentry_minutes=args.max_reentry_minutes,
        min_breakout_pct=args.min_breakout_pct,
        min_breakout_wick_pct=args.min_breakout_wick_pct,
        min_box_size=args.min_box_size,
        max_box_size=args.max_box_size,
        min_retest_size_pct=args.min_retest_size_pct,
        max_retest_size_pct=args.max_retest_size_pct,
        min_retest_body_inside_pct=args.min_retest_body_inside_pct,
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

    if args.export_trades:
        export_trades(trades, args.export_trades)
        print(f"Trade log exported to {args.export_trades}")


if __name__ == "__main__":
    main()