import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
from typing import List, Dict, Any, Optional, Tuple

# =====================================================
# CONFIG — STRATÉGIE FINALE
# =====================================================
CSV_PATH = "YM10.csv"  # <-- change si besoin

# Timezones
DATA_TZ = "UTC"
LOCAL_TZ = "Europe/Paris"
EXCHANGE_TZ = "America/New_York"

# Paramètres stratégie
TP_R = 2                    # Take profit en R
RETEST_MINUTES = 55           # Fenêtre de retest (strictement après la bougie de cassure)
ENABLE_BE = False             # Pas de break-even
BE_AT_R = 2.25                # (ignoré car BE off)

# Filtres sur la bougie de cassure
BODY_OUTSIDE_FRAC_MIN = 0.35  # % du corps hors box min
RANGE_VS_BOX_MIN = 0.65       # (range / hauteur de box) min
WICK_OUT_MAX_FRAC = 0.55      # % max de mèche côté cassure (vs range de la bougie)

# Filtre d’overextension (avant l’entrée)
OVEREXT_MULT = 1.25

# Fenêtres horaires (New York)
OPEN_START_NY = "09:30"
OPEN_END_NY = "09:44"
BREAK_SCAN_START_NY = "09:45"
BREAK_SCAN_END_NY = "11:25"
MAX_TRADE_END_NY = "15:59"

# Filtre de taille de box
BOX_FILTER_ENABLE = True
BOX_FILTER_TYPE = "band"       # "max" | "min" | "band"
BOX_MAX = 60.0
BOX_MIN = 2.75
BOX_BAND_MIN = 30.0
BOX_BAND_MAX = 210.0

# Timeframe de la bougie de cassure (ex: "1min", "3min", "5min", "10min")
BREAK_TF = "15min"

# >>> STOP LOSS en % de la box (depuis l’extrémité côté cassure) <<<
# ex: 0.20 (20%), 0.30 (30%), 0.40 (40%), 0.50 (50% = mid), etc.
STOP_FRAC = 0.50

# Sortie / plots
SHOW_PLOTS = True
SAVE_PLOTS = True
PLOT_DPI = 120

# =====================================================
# HELPERS
# =====================================================
FiveMinAgg = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}  # utilisé pour tout TF

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

    open_col_list = [k for k, v in cols.items() if v.startswith("open")]
    high_col_list = [k for k, v in cols.items() if v.startswith("high")]
    low_col_list  = [k for k, v in cols.items() if v.startswith("low")]
    close_col_list= [k for k, v in cols.items() if v.startswith("close")]
    if not (open_col_list and high_col_list and low_col_list and close_col_list):
        raise ValueError("Colonnes OHLC introuvables.")

    open_col, high_col, low_col, close_col = open_col_list[0], high_col_list[0], low_col_list[0], close_col_list[0]
    df = df[[time_col, open_col, high_col, low_col, close_col]].copy()
    df.columns = ["Time", "Open", "High", "Low", "Close"]

    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Time"]).sort_values("Time")

    if df["Time"].dt.tz is None:
        df["Time"] = df["Time"].dt.tz_localize(DATA_TZ)
    else:
        df["Time"] = df["Time"].dt.tz_convert(DATA_TZ)

    df["Time_PARIS"] = df["Time"].dt.tz_convert(LOCAL_TZ)
    return df.set_index("Time_PARIS")[['Open', 'High', 'Low', 'Close']].copy()

def first_break_close_out(df_brk_day: pd.DataFrame, bh: float, bl: float):
    """Première clôture BREAK_TF qui sort de la box (fenêtre BREAK)."""
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
    """Mèche côté cassure / range bougie (0..1)."""
    rng = max(h - l, 1e-12)
    body_hi = max(o, c)
    body_lo = min(o, c)
    if side == "long":
        wick_out = max(0.0, h - body_hi)  # mèche haute
    else:
        wick_out = max(0.0, body_lo - l)  # mèche basse
    return float(wick_out / rng)

def compute_stop_and_risk(side: str, box_high: float, box_low: float, stop_frac: float) -> Tuple[float, float]:
    """
    SL en % de la box depuis l’extrémité côté cassure.
    - long : SL = box_high - stop_frac * (box_high - box_low)
    - short: SL = box_low  + stop_frac * (box_high - box_low)
    Retourne (stop, risk_points) avec risk_points > 0 si possible, sinon (stop, 0).
    """
    # normalisation prudente
    sf = float(np.clip(stop_frac, 0.0, 0.999999))
    height = float(box_high - box_low)
    if height <= 0:
        return (box_low if side == "short" else box_high, 0.0)

    if side == "long":
        stop = box_high - sf * height
        entry = box_high
        risk = entry - stop
    else:
        stop = box_low + sf * height
        entry = box_low
        risk = stop - entry

    return float(stop), float(max(risk, 0.0))

def simulate_trade(
    day_paris: pd.DataFrame,
    side: str,
    box_high: float,
    box_low: float,
    break_end_paris: pd.Timestamp,
    break_date_ny,
    break_px: float,
    tp_r: float,
    overext_mult: float,
    retest_minutes: int
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp, float, str, float]]:
    """
    Règles strictes :
    - Retest STRICTEMENT après la bougie de cassure.
    - Sur la 1ère bougie d'entrée : si SL touché -> SL direct ; si TP seul -> on ignore le TP et on continue ;
      si TP+SL sur la même bougie -> SL.
    SL placé selon STOP_FRAC (% de la box depuis l’extrémité côté cassure).
    """
    entry = box_high if side == "long" else box_low
    stop, risk_points = compute_stop_and_risk(side, box_high, box_low, STOP_FRAC)
    if risk_points <= 0:
        return None

    end = break_end_paris + pd.Timedelta(minutes=retest_minutes)
    w = day_paris[(day_paris.index > break_end_paris) & (day_paris.index < end)]
    if w.empty:
        return None

    # Premier toucher du niveau d'entrée
    hit = w[w["Low"] <= box_high] if side == "long" else w[w["High"] >= box_low]
    if hit.empty:
        return None

    # Overextension avant entrée
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

    # Définition du TP à tp_r * risk_points depuis l'entrée
    if side == "long":
        tp = entry + tp_r * risk_points
    else:
        tp = entry - tp_r * risk_points

    first_bar = True
    for ts, row in trail.iterrows():
        hi, lo = float(row["High"]), float(row["Low"])

        if first_bar:
            if side == "long":
                touched_tp = hi >= tp
                touched_stop = lo <= stop
                if touched_tp and touched_stop:
                    return entry_ts, ts, stop, "SL", risk_points
                if touched_stop:
                    return entry_ts, ts, stop, "SL", risk_points
                # touched_tp seul -> on ignore et on continue
            else:
                touched_tp = lo <= tp
                touched_stop = hi >= stop
                if touched_tp and touched_stop:
                    return entry_ts, ts, stop, "SL", risk_points
                if touched_stop:
                    return entry_ts, ts, stop, "SL", risk_points
                # touched_tp seul -> on ignore et on continue
            first_bar = False
            continue

        if side == "long":
            if lo <= stop:
                return entry_ts, ts, stop, "SL", risk_points
            if hi >= tp:
                return entry_ts, ts, tp, "TP", risk_points
        else:
            if hi >= stop:
                return entry_ts, ts, stop, "SL", risk_points
            if lo <= tp:
                return entry_ts, ts, tp, "TP", risk_points

    # Pas de TP ni SL -> TIMEOUT
    return entry_ts, trail.index[-1], float(trail.iloc[-1]["Close"]), "TIMEOUT", risk_points

def body_and_range_pass(side: str, bh: float, bl: float,
                        o: float, h: float, l: float, c: float,
                        body_outside_frac_min: float,
                        range_vs_box_min: float) -> bool:
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
    setups: List[Dict[str, Any]] = []

    # agrégation au TF choisi pour la bougie de cassure
    df_brk = ohlc_paris.resample(BREAK_TF, label="left", closed="left").agg(FiveMinAgg).dropna()

    for date_paris, day_paris in ohlc_paris.resample("D"):
        if day_paris.empty:
            continue

        day_brk = df_brk.loc[day_paris.index.min(): day_paris.index.max()]
        if day_brk.empty:
            continue

        # Box sur la fenêtre d’open NY
        day_ny = day_paris.tz_convert(EXCHANGE_TZ)
        box_ny = day_ny.between_time(OPEN_START_NY, OPEN_END_NY)
        if box_ny.empty:
            continue

        bh, bl = float(box_ny["High"].max()), float(box_ny["Low"].min())
        if not passes_box_filter(bh, bl):
            continue

        side, bstart_paris, bend_paris, break_px = first_break_close_out(day_brk, bh, bl)
        if side is None:
            continue

        r = day_brk.loc[bstart_paris]
        o5, h5, l5, c5 = map(float, (r["Open"], r["High"], r["Low"], r["Close"]))

        trade_date_ny = bstart_paris.tz_convert(EXCHANGE_TZ).date()
        setups.append(dict(
            date_paris=date_paris.date(),
            day_paris=day_paris,
            box_high=bh,
            box_low=bl,
            side=side,
            break_start=bstart_paris,
            break_end=bend_paris,
            break_px=break_px,
            trade_date_ny=trade_date_ny,
            o5=o5, h5=h5, l5=l5, c5=c5,
        ))

    return setups

# =====================================================
# RUN & REPORTS
# =====================================================
def run_strategy(ohlc_paris: pd.DataFrame) -> pd.DataFrame:
    setups = build_daily_setups(ohlc_paris)
    rows: List[List[Any]] = []

    for st in setups:
        # 1) Filtre mèche côté cassure
        if outside_wick_frac(st["side"], st["box_high"], st["box_low"],
                             st["o5"], st["h5"], st["l5"], st["c5"]) > WICK_OUT_MAX_FRAC:
            continue

        # 2) Filtre corps/range
        if not body_and_range_pass(
            st["side"], st["box_high"], st["box_low"], st["o5"], st["h5"], st["l5"], st["c5"],
            BODY_OUTSIDE_FRAC_MIN, RANGE_VS_BOX_MIN
        ):
            continue

        # 3) Simulation
        sim = simulate_trade(
            day_paris=st["day_paris"],
            side=st["side"],
            box_high=st["box_high"],
            box_low=st["box_low"],
            break_end_paris=st["break_end"],
            break_date_ny=st["trade_date_ny"],
            break_px=st["break_px"],
            tp_r=float(TP_R),
            overext_mult=float(OVEREXT_MULT),
            retest_minutes=int(RETEST_MINUTES),
        )
        if not sim:
            continue

        entry_ts, exit_ts, exit_px, reason, risk_pts = sim
        entry = st["box_high"] if st["side"] == "long" else st["box_low"]
        R = ((exit_px - entry) / risk_pts) if st["side"] == "long" else ((entry - exit_px) / risk_pts)
        rows.append([
            st["date_paris"], st["side"], entry_ts, float(entry), float(st["box_high"]), float(st["box_low"]),
            st["break_end"], exit_ts, float(exit_px), reason, float(R)
        ])

    trades = pd.DataFrame(rows, columns=[
        "date", "side", "entry_ts", "entry", "box_high", "box_low", "break_end", "exit_ts", "exit_px", "reason", "R"
    ])
    if trades.empty:
        return trades

    trades["cum_R"] = trades["R"].cumsum()
    return trades

def summarize_global(trades: pd.DataFrame, ohlc_paris: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame([[0,0,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0]], columns=[
            "Trades","TP","SL","BE","TIMEOUT","Winrate_%","R_mean","R_total","Max_DD_R","Trades_per_day","R_mean_TIMEOUT"
        ])
    t = trades.copy()
    n = len(t)
    tp_c = (t["reason"] == "TP").sum()
    sl_c = (t["reason"] == "SL").sum()
    be_c = (t["reason"] == "BE").sum() if "BE" in t["reason"].unique() else 0
    to_c = (t["reason"] == "TIMEOUT").sum()
    winrate = (tp_c / n) * 100.0
    r_mean = t["R"].mean()
    r_total = t["R"].sum()
    r_timeout = t.loc[t["reason"] == "TIMEOUT", "R"].mean() if to_c > 0 else 0.0
    max_dd = (t["cum_R"].cummax() - t["cum_R"]).max()

    nb_days = (ohlc_paris.index.date.max() - ohlc_paris.index.date.min()).days + 1
    trades_per_day = (n / nb_days) if nb_days > 0 else 0.0

    return pd.DataFrame([[n, tp_c, sl_c, be_c, to_c, winrate, r_mean, r_total, float(max_dd), trades_per_day, r_timeout]], columns=[
        "Trades","TP","SL","BE","TIMEOUT","Winrate_%","R_mean","R_total","Max_DD_R","Trades_per_day","R_mean_TIMEOUT"
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
            (g["reason"] == "TP").sum(),
            (g["reason"] == "SL").sum(),
            (g["reason"] == "TIMEOUT").sum(),
            g["reason"].eq("TP").mean() * 100.0,
            g["R"].mean(),
            g["R"].sum(),
            float((cum.cummax() - cum).max()),
        ])
    return pd.DataFrame(rows, columns=["month","Trades","TP","SL","TIMEOUT","Winrate_%","R_mean","R_total","Max_DD_R"]).sort_values("month").reset_index(drop=True)

def make_last_10(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.sort_values("exit_ts").tail(10).copy()
    t["entry_time"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    t["exit_time"] = pd.to_datetime(t["exit_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    return t[["date","side","entry_time","entry","exit_time","exit_px","reason","R"]].copy()

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
        print("\n=== Bilan global — stratégie finale ===")
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
        plt.plot(t_sorted["exit_ts"], t_sorted["cum_R"])  # ligne unique, pas de couleur imposée
        plt.title(f"Équité cumulée (R) — stratégie finale (SL={int(STOP_FRAC*100)}% de la box)")
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



