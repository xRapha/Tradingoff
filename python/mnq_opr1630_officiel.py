import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
from typing import List, Dict, Any, Optional, Tuple

# =====================================================
# CONFIG — Stratégie finale (Box 1h + Cassure BREAK_TF + Wick<=x%) avec SL paramétrable
# =====================================================
CSV_PATH = "NQ10.csv"

# Timezones
DATA_TZ = "UTC"
LOCAL_TZ = "Europe/Paris"
EXCHANGE_TZ = "America/New_York"

# Paramètres stratégie (final)
TP_R = 2.25                     # TP en multiples du risque (R)
RETEST_MINUTES = 35          # fenêtre de retest (strictement après la bougie de cassure)
ENABLE_BE = False            # BE désactivé
BE_AT_R = 2.25               # (ignoré si ENABLE_BE = False)

# Filtres bougie de cassure
BODY_OUTSIDE_FRAC_MIN = 0.07  # OFF (0 => pas de filtre)
RANGE_VS_BOX_MIN = 0.20      # OFF (0 => pas de filtre)
WICK_OUT_MAX_FRAC = 0.55     # mèche côté cassure ≤ 30% du range de la bougie (0..1)

# Filtre d’overextension (0 => OFF)
OVEREXT_MULT = 0.55

# Fenêtres horaires (New York)
OPEN_START_NY = "09:30"         # début box 1h
OPEN_END_NY   = "10:29"         # fin box 1h
BREAK_SCAN_START_NY = "10:30"   # cassure à partir de 10:30
BREAK_SCAN_END_NY   = "11:25"
MAX_TRADE_END_NY    = "15:59"

# Filtre de taille de box
BOX_FILTER_ENABLE = True
BOX_FILTER_TYPE = "band"       # "max" | "min" | "band"
BOX_MIN = 30.0
BOX_MAX = 1e12
BOX_BAND_MIN = 20
BOX_BAND_MAX = 260

# === NOUVEAUTÉS PARAMÉTRABLES ===
# Timeframe de la bougie de cassure (ex: "1min", "3min", "5min", "10min")
BREAK_TF = "5min"

# Stop-loss en % de la hauteur de la box (depuis l’extrémité côté cassure)
# ex: 0.22 (=22%), 0.25 (=25%), etc.
STOP_FRAC = 0.50

# Sortie / plots
SHOW_PLOTS = True
SAVE_PLOTS = True
PLOT_DPI = 120

# =====================================================
# HELPERS
# =====================================================
FiveMinAgg = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}

def _norm(s: Any) -> str:
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())

def passes_box_filter(box_high: float, box_low: float) -> bool:
    if not BOX_FILTER_ENABLE:
        return True
    width = float(box_high - box_low)
    if BOX_FILTER_TYPE == "max":
        return width <= float(BOX_MAX)
    elif BOX_FILTER_TYPE == "min":
        return width >= float(BOX_MIN)
    elif BOX_FILTER_TYPE == "band":
        return (width >= float(BOX_BAND_MIN)) and (width <= float(BOX_BAND_MAX))
    return True

def load_ohlc_paris(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=None, engine="python")
    cols = {c: _norm(c) for c in df.columns}

    time_col_candidates = [k for k, v in cols.items()
                           if ("timeleft" in v) or v.startswith("time") or ("datetime" in v) or v.endswith("time")]
    if not time_col_candidates:
        raise ValueError("Colonne temps introuvable.")
    time_col = time_col_candidates[0]

    open_col_list  = [k for k, v in cols.items() if v.startswith("open")]
    high_col_list  = [k for k, v in cols.items() if v.startswith("high")]
    low_col_list   = [k for k, v in cols.items() if v.startswith("low")]
    close_col_list = [k for k, v in cols.items() if v.startswith("close")]
    if not (open_col_list and high_col_list and low_col_list and close_col_list):
        raise ValueError("Colonnes OHLC introuvables.")

    open_col, high_col, low_col, close_col = (
        open_col_list[0], high_col_list[0], low_col_list[0], close_col_list[0]
    )

    df = df[[time_col, open_col, high_col, low_col, close_col]].copy()
    df.columns = ["Time", "Open", "High", "Low", "Close"]

    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Time"]).sort_values("Time")

    if df["Time"].dt.tz is None:
        df["Time"] = df["Time"].dt.tz_localize(DATA_TZ)
    else:
        df["Time"] = df["Time"].dt.tz_convert(DATA_TZ)

    df["Time_PARIS"] = df["Time"].dt.tz_convert(LOCAL_TZ)
    ohlc = df.set_index("Time_PARIS")[['Open', 'High', 'Low', 'Close']].copy()
    return ohlc

def first_break_close_out(df_brk_day: pd.DataFrame, bh: float, bl: float) -> Tuple[Optional[str], Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[float]]:
    """
    Première clôture (au TF BREAK_TF) qui sort de la box, entre BREAK_SCAN_START_NY et BREAK_SCAN_END_NY (index Paris).
    """
    df_brk_ny = df_brk_day.tz_convert(EXCHANGE_TZ).between_time(BREAK_SCAN_START_NY, BREAK_SCAN_END_NY)
    for ts_ny, row in df_brk_ny.iterrows():
        c = float(row["Close"])
        if c > bh:
            ts_paris = ts_ny.tz_convert(LOCAL_TZ)
            return "long", ts_paris, ts_paris + pd.Timedelta(BREAK_TF), c
        if c < bl:
            ts_paris = ts_ny.tz_convert(LOCAL_TZ)
            return "short", ts_paris, ts_paris + pd.Timedelta(BREAK_TF), c
    return None, None, None, None

def outside_wick_frac(side: str, bh: float, bl: float, o: float, h: float, l: float, c: float) -> float:
    """
    Mèche côté cassure / range bougie (0..1).
    - long: mèche haute (High - max(Open,Close))
    - short: mèche basse (min(Open,Close) - Low)
    """
    rng = max(h - l, 1e-12)
    body_hi = max(o, c)
    body_lo = min(o, c)
    if side == "long":
        wick_out = max(0.0, h - body_hi)
    else:
        wick_out = max(0.0, body_lo - l)
    return float(wick_out / rng)

# =====================================================
# SIMULATION — SL = STOP_FRAC * box, TP = TP_R × SL
# =====================================================
def simulate_trade(
    day_paris: pd.DataFrame,
    side: str,
    box_high: float,
    box_low: float,
    break_end_paris: pd.Timestamp,
    break_date_ny,
    break_px: float,
    tp_r: float,
    be_at_r: float = BE_AT_R,
    enable_be: bool = ENABLE_BE,
    overext_mult: float = OVEREXT_MULT,
    retest_minutes: Optional[int] = None,
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp, float, str, float]]:
    """
    - Retest STRICTEMENT après la bougie de cassure.
    - 1ère bougie d'entrée: SL touché => SL, TP-only ignoré.
    - Overextension avant l'entrée (si OVEREXT_MULT > 0).
    - SL = STOP_FRAC * hauteur de box depuis l’extrémité côté cassure (vers l’intérieur).
    - TP = entrée ± TP_R * |entrée − SL|.
    """
    entry = float(box_high if side == "long" else box_low)

    # ----- SL = STOP_FRAC de la hauteur de la box (vers l'intérieur) -----
    box_h = float(box_high - box_low)
    sl_dist = max(float(STOP_FRAC) * abs(box_h), 1e-12)
    stop = entry - sl_dist if side == "long" else entry + sl_dist
    # ---------------------------------------------------------------------

    risk_points = abs(entry - stop)
    if risk_points <= 0:
        return None

    # Fenêtre de retest strictement après la cassure
    rm = int(retest_minutes if retest_minutes is not None else RETEST_MINUTES)
    end = break_end_paris + pd.Timedelta(minutes=rm)
    w = day_paris[(day_paris.index > break_end_paris) & (day_paris.index < end)]
    if w.empty:
        return None

    # Premier toucher du niveau d'entrée
    hit = w[w["Low"] <= entry] if side == "long" else w[w["High"] >= entry]
    if hit.empty:
        return None

    # Overextension avant l'entrée
    if overext_mult and overext_mult > 0 and np.isfinite(overext_mult):
        box_mid_val = (box_high + box_low) / 2.0
        dist_from_mid = abs(float(break_px) - box_mid_val)
        if dist_from_mid > 0:
            entry_ts_candidate = hit.index[0]
            pre = w[w.index < entry_ts_candidate]
            if not pre.empty:
                if side == "long":
                    runup = float(pre["High"].max()) - float(break_px)
                    if runup > overext_mult * dist_from_mid:
                        return None
                else:
                    rundown = float(break_px) - float(pre["Low"].min())
                    if rundown > overext_mult * dist_from_mid:
                        return None

    entry_ts = hit.index[0]
    cutoff_paris = pd.Timestamp(f"{break_date_ny} {MAX_TRADE_END_NY}", tz=EXCHANGE_TZ).tz_convert(LOCAL_TZ)

    trail = day_paris[(day_paris.index >= entry_ts) & (day_paris.index <= cutoff_paris)]
    if trail.empty:
        return None

    # TP = TP_R × risk_points (R fixe)
    if side == "long":
        tp = entry + float(tp_r) * risk_points
        be_trig = entry + (be_at_r * risk_points if enable_be else np.inf)
    else:
        tp = entry - float(tp_r) * risk_points
        be_trig = entry - (be_at_r * risk_points if enable_be else np.inf)

    moved = False
    first_bar = True
    for ts, row in trail.iterrows():
        hi, lo = float(row["High"]), float(row["Low"])

        if first_bar:
            if side == "long":
                if lo <= stop:
                    return entry_ts, ts, stop, "SL", risk_points
            else:
                if hi >= stop:
                    return entry_ts, ts, stop, "SL", risk_points
            first_bar = False
            continue

        # (BE off par défaut — si on l’active, TP reste en multiples du risque initial)
        if enable_be and not moved:
            if (hi >= be_trig) if side == "long" else (lo <= be_trig):
                stop = entry
                moved = True

        if side == "long":
            eff_stop = stop
            if lo <= eff_stop:
                return entry_ts, ts, eff_stop, ("BE" if (moved and eff_stop == entry) else "SL"), risk_points
            if hi >= tp:
                return entry_ts, ts, tp, "TP", risk_points
        else:
            eff_stop = stop
            if hi >= eff_stop:
                return entry_ts, ts, eff_stop, ("BE" if (moved and eff_stop == entry) else "SL"), risk_points
            if lo <= tp:
                return entry_ts, ts, tp, "TP", risk_points

    return entry_ts, trail.index[-1], float(trail.iloc[-1]["Close"]), "TIMEOUT", risk_points

def body_and_range_pass(side: str, bh: float, bl: float,
                        o: float, h: float, l: float, c: float,
                        body_outside_frac_min: float,
                        range_vs_box_min: float) -> bool:
    # filtres à 0 => toujours True
    if body_outside_frac_min <= 0 and range_vs_box_min <= 0:
        return True
    lo_body, hi_body = min(o, c), max(o, c)
    rng = max(h - l, 1e-12)
    box_h = max(bh - bl, 1e-12)
    if side == "long":
        body_out = max(0.0, hi_body - max(bh, lo_body)) if hi_body > bh else 0.0
    else:
        body_out = (min(bl, hi_body) - lo_body) if lo_body < bl else 0.0
    body_frac = body_out / max(hi_body - lo_body, 1e-12)
    range_frac = rng / box_h
    return (body_frac >= body_outside_frac_min) and (range_frac >= range_vs_box_min)

def build_daily_setups(ohlc_paris: pd.DataFrame) -> List[Dict[str, Any]]:
    """Box 1h (09:30–10:29 NY) + 1ère cassure BREAK_TF à partir de 10:30 NY.
       Applique box_min + wick ≤ WICK_OUT_MAX_FRAC (si activés).
    """
    setups: List[Dict[str, Any]] = []
    df_brk = (
        ohlc_paris
        .resample(BREAK_TF, label="left", closed="left")
        .agg(FiveMinAgg)
        .dropna()
    )

    for date_paris, day_paris in ohlc_paris.resample("D"):
        if day_paris.empty:
            continue

        day_brk = df_brk.loc[day_paris.index.min(): day_paris.index.max()]
        if day_brk.empty:
            continue

        # Box 1h (09:30–10:29 NY)
        day_ny = day_paris.tz_convert(EXCHANGE_TZ)
        box_ny = day_ny.between_time(OPEN_START_NY, OPEN_END_NY)
        if box_ny.empty:
            continue

        bh, bl = float(box_ny["High"].max()), float(box_ny["Low"].min())
        if not passes_box_filter(bh, bl):
            continue

        # 1ère clôture BREAK_TF qui sort de la box à partir de 10:30 NY
        side, bstart_paris, bend_paris, break_px = first_break_close_out(day_brk, bh, bl)
        if side is None:
            continue

        # Bougie de cassure (au TF BREAK_TF) pour filtres
        r = day_brk.loc[bstart_paris]
        oC, hC, lC, cC = map(float, (r["Open"], r["High"], r["Low"], r["Close"]))

        # Filtres (body/range selon params) + wick
        if not body_and_range_pass(side, bh, bl, oC, hC, lC, cC,
                                   BODY_OUTSIDE_FRAC_MIN, RANGE_VS_BOX_MIN):
            continue
        wick_frac = outside_wick_frac(side, bh, bl, oC, hC, lC, cC)
        if wick_frac > float(WICK_OUT_MAX_FRAC):
            continue

        trade_date_ny = bstart_paris.tz_convert(EXCHANGE_TZ).date()
        setups.append(dict(
            date_paris=date_paris.date(),
            day_paris=day_paris,
            box_high=bh, box_low=bl,
            side=side,
            break_start=bstart_paris,
            break_end=bend_paris,
            break_px=break_px,
            trade_date_ny=trade_date_ny,
        ))
    return setups

# =====================================================
# RUN & REPORTS
# =====================================================
def run_strategy(ohlc_paris: pd.DataFrame) -> pd.DataFrame:
    setups = build_daily_setups(ohlc_paris)
    rows: List[List[Any]] = []

    for st in setups:
        sim = simulate_trade(
            day_paris=st["day_paris"],
            side=st["side"],
            box_high=st["box_high"],
            box_low=st["box_low"],
            break_end_paris=st["break_end"],
            break_date_ny=st["trade_date_ny"],
            break_px=st["break_px"],
            tp_r=float(TP_R),
            enable_be=ENABLE_BE,
            be_at_r=BE_AT_R,
            overext_mult=float(OVEREXT_MULT),
            retest_minutes=int(RETEST_MINUTES),
        )
        if not sim:
            continue

        entry_ts, exit_ts, exit_px, reason, risk_pts = sim
        entry = st["box_high"] if st["side"] == "long" else st["box_low"]
        R = ((exit_px - entry) / risk_pts) if st["side"] == "long" else ((entry - exit_px) / risk_pts)
        rows.append([
            st["date_paris"], st["side"], entry_ts, float(entry),
            float(st["box_high"]), float(st["box_low"]),
            st["break_end"], exit_ts, float(exit_px), reason, float(R)
        ])

    trades = pd.DataFrame(rows, columns=[
        "date", "side", "entry_ts", "entry", "box_high", "box_low",
        "break_end", "exit_ts", "exit_px", "reason", "R"
    ])
    if trades.empty:
        return trades

    trades = trades.sort_values("exit_ts").copy()
    trades["cum_R"] = trades["R"].cumsum()
    return trades

def summarize_global(trades: pd.DataFrame, ohlc_paris: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame([[0,0,0,0,0.0,0.0,0.0,0.0,0.0]], columns=[
            "Trades", "TP", "SL", "TIMEOUT", "Winrate_%",
            "R_mean", "R_total", "Max_DD_R", "Trades_per_day"
        ])
    t = trades.copy()
    n = len(t)
    tp_c = int((t["reason"] == "TP").sum())
    sl_c = int((t["reason"] == "SL").sum())
    to_c = int((t["reason"] == "TIMEOUT").sum())
    winrate = (tp_c / n) * 100.0
    r_mean = float(t["R"].mean())
    r_total = float(t["R"].sum())
    max_dd = float((t["cum_R"].cummax() - t["cum_R"]).max())

    nb_days = (ohlc_paris.index.date.max() - ohlc_paris.index.date.min()).days + 1
    trades_per_day = (n / nb_days) if nb_days > 0 else 0.0

    return pd.DataFrame([[n, tp_c, sl_c, to_c, winrate, r_mean, r_total, max_dd, trades_per_day]], columns=[
        "Trades", "TP", "SL", "TIMEOUT", "Winrate_%",
        "R_mean", "R_total", "Max_DD_R", "Trades_per_day"
    ])

def summarize_monthly_2025(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        t["month"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.to_period("M").astype(str)
    t["year"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.year
    t25 = t[t["year"] == 2025]
    if t25.empty:
        return pd.DataFrame(columns=["month", "Trades", "TP", "SL", "TIMEOUT", "Winrate_%", "R_mean", "R_total", "Max_DD_R"])

    rows = []
    for m, g in t25.groupby("month"):
        g = g.sort_values("exit_ts")
        cum = g["R"].cumsum()
        rows.append([
            m,
            len(g),
            int((g["reason"] == "TP").sum()),
            int((g["reason"] == "SL").sum()),
            int((g["reason"] == "TIMEOUT").sum()),
            float(g["reason"].eq("TP").mean() * 100.0),
            float(g["R"].mean()),
            float(g["R"].sum()),
            float((cum.cummax() - cum).max()),
        ])
    monthly = pd.DataFrame(rows, columns=["month", "Trades", "TP", "SL", "TIMEOUT", "Winrate_%", "R_mean", "R_total", "Max_DD_R"]).sort_values("month").reset_index(drop=True)
    return monthly

def make_last_10(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.sort_values("exit_ts").tail(10).copy()
    t["entry_time"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    t["exit_time"] = pd.to_datetime(t["exit_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    out = t[["date", "side", "entry_time", "entry", "exit_time", "exit_px", "reason", "R"]].copy()
    return out

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    ohlc = load_ohlc_paris(CSV_PATH)
    trades = run_strategy(ohlc)

    if trades.empty:
        print("Aucun trade trouvé avec les paramètres finaux.")
    else:
        # Sauvegardes
        trades.to_csv("opr_trades.csv", index=False)
        global_sum = summarize_global(trades, ohlc)
        global_sum.to_csv("opr_global_summary.csv", index=False)
        monthly25 = summarize_monthly_2025(trades)
        monthly25.to_csv("opr_monthly_2025.csv", index=False)
        last10 = make_last_10(trades)
        last10.to_csv("opr_last10.csv", index=False)

        # Console
        print("\n=== Bilan global — SL = STOP_FRAC × box, TP = TP_R × SL ===")
        print(global_sum.to_string(index=False))

        print("\n=== Bilan mensuel 2025 ===")
        if monthly25.empty:
            print("(Aucun trade en 2025 dans l'échantillon)")
        else:
            print(monthly25.to_string(index=False))

        print("\n=== 10 derniers trades ===")
        print(last10.to_string(index=False))

        # Courbe d'équité
        plt.figure(figsize=(9, 4))
        t_sorted = trades.sort_values("exit_ts")
        plt.plot(t_sorted["exit_ts"], t_sorted["cum_R"])
        plt.title(f"Équité cumulée (R) — Cassure {BREAK_TF} | SL={int(STOP_FRAC*100)}% box | TP={TP_R}R")
        plt.xlabel("Date")
        plt.ylabel("R cumulés")
        plt.grid(True)
        plt.tight_layout()
        if SAVE_PLOTS:
            plt.savefig("opr_equity_curve.png", dpi=PLOT_DPI)
        if SHOW_PLOTS:
            plt.show()
        else:
            plt.close()

        print("\nFichiers sauvegardés : opr_trades.csv, opr_global_summary.csv, opr_monthly_2025.csv, opr_last10.csv, opr_equity_curve.png")
