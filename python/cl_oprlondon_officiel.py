import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Any, Dict, List, Tuple, Optional

# ==============================================
# CONFIG — SCRIPT SIMPLE (nettoyé + filtre side)
# ==============================================
CSV_PATH = "CL10.csv"

# Timezones
DATA_TZ    = "UTC"
LOCAL_TZ   = "Europe/Paris"
SESSION_TZ = "Europe/London"

# Fenêtres (heure de Londres)
ASIAN_BOX_START_LON = "00:00"
ASIAN_BOX_END_LON   = "09:00"
ENTRY_START_LON     = "09:00"
ENTRY_CUTOFF_LON    = "13:00"
MAX_TRADE_END_LON   = "14:30"  # Trail jusqu'à 14:30 Londres

# === TES PARAMS ===
TIMEFRAME = "5min"                    # "5min" | "10min" | "15min"
TP_R      = 2.25                         # Take Profit en R
BE_R: Optional[float] = None          # Seuil d'activation du BE (None = désactivé). Doit être < TP_R.
DAYS      = [1,3,4]               # 0=Lun .. 6=Dim (filtre d'étude)
WICK_MAX  = 70.0                      # % mèche max sur la bougie de cassure
INACTIVE_MONTHS: Tuple[int,...] = (5,9)  # mois à exclure (filtre d'étude)
SIDE_MODE: Optional[str] = "both"       # None="both" | "long" | "short"

# ==============================================
# HELPERS
# ==============================================
BarAgg = {"Open":"first","High":"max","Low":"min","Close":"last"}

def _norm(s: Any) -> str:
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())

def load_ohlc_paris(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=None, engine="python")
    cols = {c: _norm(c) for c in df.columns}

    time_col_candidates = [k for k, v in cols.items()
        if ("timeleft" in v) or v.startswith("time") or ("datetime" in v) or v.endswith("time")]
    if not time_col_candidates:
        raise ValueError("Colonne temps introuvable.")
    time_col = time_col_candidates[0]

    open_col  = next(k for k,v in cols.items() if v.startswith("open"))
    high_col  = next(k for k,v in cols.items() if v.startswith("high"))
    low_col   = next(k for k,v in cols.items() if v.startswith("low"))
    close_col = next(k for k,v in cols.items() if v.startswith("close"))

    df = df[[time_col, open_col, high_col, low_col, close_col]].copy()
    df.columns = ["Time","Open","High","Low","Close"]
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Time"]).sort_values("Time")

    if df["Time"].dt.tz is None:
        df["Time"] = df["Time"].dt.tz_localize(DATA_TZ)
    else:
        df["Time"] = df["Time"].dt.tz_convert(DATA_TZ)

    df["Time_PARIS"] = df["Time"].dt.tz_convert(LOCAL_TZ)
    ohlc = df.set_index("Time_PARIS")[ ["Open","High","Low","Close"] ].copy()
    return ohlc

def ema(series: pd.Series, period: int = 50) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def wick_dir_pct(side: str, h: float, l: float, o: float, c: float) -> float:
    rng = max(h - l, 1e-9)
    wick = (h - c) if side=="long" else (c - l)
    return float(max(0.0, wick) / rng * 100.0)

# ==============================================
# BACKTEST (TF unique, TP unique) → liste de trades "setups"
# ==============================================
def build_setups(ohlc_paris: pd.DataFrame, tf: str) -> List[Dict[str, Any]]:
    df_tf = ohlc_paris.resample(tf, label="left", closed="left").agg(BarAgg).dropna()
    df_tf["ema50"] = ema(df_tf["Close"], 50)

    setups: List[Dict[str,Any]] = []

    for _, day_paris in ohlc_paris.resample("D"):
        if day_paris.empty:
            continue

        day_lon = day_paris.tz_convert(SESSION_TZ)
        if day_lon.empty:
            continue

        # Jour/mois en Londres (cohérent avec fenêtres)
        ts0 = day_lon.index[0]
        weekday = int(ts0.weekday())
        year    = int(ts0.year)
        month   = int(ts0.month)

        # Box asiatique 00:00→09:00 Londres (robuste si data commence tard)
        box_nominal = day_lon.between_time(ASIAN_BOX_START_LON, ASIAN_BOX_END_LON)
        if box_nominal.empty:
            first_ts = day_lon.index.min()
            if first_ts.tz_convert(SESSION_TZ).time() >= pd.to_datetime(ASIAN_BOX_END_LON).time():
                continue
            start_str = first_ts.tz_convert(SESSION_TZ).strftime("%H:%M")
            box = day_lon.between_time(start_str, ASIAN_BOX_END_LON)
        else:
            box = box_nominal
        if box.empty:
            continue

        bh, bl = float(box["High"].max()), float(box["Low"].min())
        box_range = float(max(bh - bl, 0.0))

        # Fenêtre 09:00→13:00 Londres dans le TF courant
        day_tf = df_tf.loc[day_paris.index.min(): day_paris.index.max()].copy()
        if day_tf.empty:
            continue
        win_tf = day_tf.tz_convert(SESSION_TZ).between_time(ENTRY_START_LON, ENTRY_CUTOFF_LON)
        if win_tf.empty:
            continue

        # Première clôture hors box (entrée à la clôture)
        signal = None
        for ts_lon, row in win_tf.iterrows():
            c = float(row["Close"])
            if c > bh:
                signal = ("long", ts_lon, c); break
            if c < bl:
                signal = ("short", ts_lon, c); break
        if signal is None:
            continue

        side, entry_ts_lon, entry_price = signal
        entry_ts_paris = entry_ts_lon.tz_convert(LOCAL_TZ)

        # SL = EMA50 as-of
        ema_asof = df_tf["ema50"].loc[:entry_ts_paris].dropna()
        if ema_asof.empty:
            continue
        stop_price = float(ema_asof.iloc[-1])
        risk = (entry_price - stop_price) if side=="long" else (stop_price - entry_price)
        if risk <= 0:
            continue

        # Wick% sur la bougie de cassure (bougie d'entrée)
        bar = df_tf.loc[entry_ts_paris]
        o, h, l, c = map(float, (bar["Open"], bar["High"], bar["Low"], bar["Close"]))
        wick_pct = wick_dir_pct(side, h, l, o, c)

        # ===== trail après la bougie d'entrée =====
        cut_lon = pd.Timestamp(f"{entry_ts_paris.tz_convert(SESSION_TZ).date()} {MAX_TRADE_END_LON}", tz=SESSION_TZ)
        cut_par = cut_lon.tz_convert(LOCAL_TZ)
        trail = df_tf[(df_tf.index > entry_ts_paris) & (df_tf.index <= cut_par)]
        if trail.empty:
            continue

        H = trail["High"].to_numpy(dtype=float)
        L = trail["Low"].to_numpy(dtype=float)
        Cl= trail["Close"].to_numpy(dtype=float)

        if side == "long":
            r_up = (H - entry_price)/risk
            r_dn = (entry_price - L)/risk
        else:
            r_up = (entry_price - L)/risk
            r_dn = (H - entry_price)/risk

        # Index SL (1R) avant BE
        idx_stop_arr = np.where(r_dn >= 1.0)[0]
        idx_stop = int(idx_stop_arr[0]) if idx_stop_arr.size else 10**9

        # Index TP (après entrée seulement)
        tp = float(TP_R)
        cum_up = np.maximum.accumulate(r_up)
        hit_tp_idx_arr = np.where(cum_up >= tp)[0]
        idx_tp = int(hit_tp_idx_arr[0]) if hit_tp_idx_arr.size else 10**9

        # Index BE (si configuré et < TP)
        active_be = (BE_R is not None) and (BE_R < TP_R)
        if active_be:
            hit_be_idx_arr = np.where(cum_up >= float(BE_R))[0]
            idx_be = int(hit_be_idx_arr[0]) if hit_be_idx_arr.size else 10**9
        else:
            idx_be = 10**9

        # Index retour à l'entrée APRÈS activation BE
        if idx_be < 10**9:
            post_be_dn = r_dn[idx_be:]
            idx_back_rel = np.where(post_be_dn > 0.0)[0]
            idx_back = (idx_be + int(idx_back_rel[0])) if idx_back_rel.size else 10**9
        else:
            idx_back = 10**9

        # R TIMEOUT (si aucun TP/SL/BE avant cut)
        last_R = (Cl[-1] - entry_price)/risk if side=="long" else (entry_price - Cl[-1])/risk

        # ===== décision de l'issue + sortie (ts/prix)
        # Priorité en cas d'égalité de bougie: SL > TP > BE
        idx_stop_eff = idx_stop if (idx_stop < idx_be) else 10**9
        first_idx = min(idx_stop_eff, idx_tp, idx_back)

        if first_idx == 10**9:
            outcome = "TIMEOUT"
            r_final = float(last_R)
            exit_idx = len(trail) - 1
            exit_price = float(Cl[-1])
        elif first_idx == idx_stop_eff:
            outcome = "SL"
            r_final = -1.0
            exit_idx = int(idx_stop_eff)
            exit_price = float(stop_price)
        elif first_idx == idx_tp:
            outcome = "TP"
            r_final = float(tp)
            exit_idx = int(idx_tp)
            exit_price = float(entry_price + tp * risk) if side=="long" else float(entry_price - tp * risk)
        else:  # first_idx == idx_back
            outcome = "BE"
            r_final = 0.0
            exit_idx = int(idx_back)
            exit_price = float(entry_price)

        # Timestamp de sortie (bar du premier passage)
        exit_idx = max(0, min(exit_idx, len(trail)-1))
        exit_ts_paris = trail.index[exit_idx]

        setups.append(dict(
            side=side,
            ts_paris=entry_ts_paris,
            entry_ts=entry_ts_paris,
            entry_price=float(entry_price),
            stop_price=float(stop_price),
            exit_ts=exit_ts_paris,
            exit_price=float(exit_price),

            date_paris=entry_ts_paris.date(),
            year=int(year),
            month=int(month),
            weekday=int(weekday),
            wick_pct=float(wick_pct),
            box_range=float(box_range),
            R=float(r_final),
            Outcome=outcome
        ))

    return setups

# ==============================================
# MÉTRIQUES & AGRÉGATIONS
# ==============================================
def eligible_days_count(ohlc_paris: pd.DataFrame) -> int:
    """
    Jours CALENDAIRES présents dans le dataset qui sont
    dans 'DAYS' et hors 'INACTIVE_MONTHS' (jours sans trade inclus).
    """
    days = []
    for _, day_paris in ohlc_paris.resample("D"):
        if day_paris.empty:
            continue
        day_lon = day_paris.tz_convert(SESSION_TZ)
        if day_lon.empty:
            continue
        ts0 = day_lon.index[0]
        wd = int(ts0.weekday())
        mo = int(ts0.month)
        if (wd in DAYS) and (mo not in set(int(m) for m in INACTIVE_MONTHS)):
            days.append(ts0.date())
    return int(len(pd.Index(days).unique()))

def tradable_days_count_all(ohlc_paris: pd.DataFrame) -> int:
    """
    Compte les jours CALENDAIRES tradables (lun→ven) présents dans le dataset,
    SANS tenir compte de DAYS ni INACTIVE_MONTHS (dénominateur de la moyenne).
    Basé sur la date en SESSION_TZ pour rester cohérent.
    """
    days = []
    for _, day_paris in ohlc_paris.resample("D"):
        if day_paris.empty:
            continue
        day_lon = day_paris.tz_convert(SESSION_TZ)
        if day_lon.empty:
            continue
        ts0 = day_lon.index[0]
        if int(ts0.weekday()) <= 4:  # Lun..Ven
            days.append(ts0.date())
    return int(len(pd.Index(days).unique()))

def apply_filters_and_metrics(setups: List[Dict[str,Any]],
                              eligible_days: int,
                              tradable_days_all: int) -> Dict[str, Any]:
    # Filtres fixes sur les setups (pour l'analyse)
    sel = [s for s in setups if (s["weekday"] in DAYS) and (s["wick_pct"] <= WICK_MAX)]
    if INACTIVE_MONTHS:
        bad = set(int(m) for m in INACTIVE_MONTHS)
        sel = [s for s in sel if s["month"] not in bad]
    # Filtre directionnel (nouveau)
    if SIDE_MODE is not None:
        mode = SIDE_MODE.lower()
        if mode in ("long", "short"):
            sel = [s for s in sel if s.get("side") == mode]

    trades_df = pd.DataFrame(sel)
    if not trades_df.empty:
        trades_df = trades_df.sort_values("ts_paris").reset_index(drop=True)
        trades_df["Trade#"] = np.arange(1, len(trades_df) + 1, dtype=int)
        trades_df["CumR"] = trades_df["R"].cumsum()
    else:
        trades_df = pd.DataFrame(columns=[
            "side","ts_paris","entry_ts","entry_price","stop_price","exit_ts","exit_price",
            "date_paris","year","month","weekday","wick_pct","box_range","R","Outcome","Trade#","CumR"
        ])

    R_vals = trades_df["R"].to_numpy(dtype=float)

    if R_vals.size == 0:
        return {
            "Trades": 0,
            "Winrate_%": 0.0,
            "R_mean": 0.0,
            "R_total": 0.0,
            "Max_DD_R": 0.0,
            "Score": 0.0,
            "EligibleDays": eligible_days,
            "TradableDaysAll": tradable_days_all,
            "Avg_Trades_per_Day_All": 0.0,
            "SideMode": SIDE_MODE or "both",
            "LongShortCounts": {"long": 0, "short": 0},
            "Pct_TP": 0.0,
            "Pct_SL": 0.0,
            "Pct_BE": 0.0,
            "Pct_TIMEOUT": 0.0,
            "Timeout_R_mean": 0.0,
            "TradesDF": trades_df
        }

    trades = int(R_vals.size)
    R_total = float(R_vals.sum())
    R_mean  = float(R_vals.mean())
    winrate = float((R_vals > 0).mean() * 100.0)
    cum = np.cumsum(R_vals)
    max_dd = float((np.maximum.accumulate(cum) - cum).max())
    score = R_total / (1.0 + abs(max_dd))

    counts = trades_df["Outcome"].value_counts()
    n_tp = int(counts.get("TP", 0))
    n_sl = int(counts.get("SL", 0))
    n_be = int(counts.get("BE", 0))
    n_to = int(counts.get("TIMEOUT", 0))
    pct_tp = 100.0 * n_tp / trades
    pct_sl = 100.0 * n_sl / trades
    pct_be = 100.0 * n_be / trades
    pct_to = 100.0 * n_to / trades

    timeout_R = trades_df.loc[trades_df["Outcome"]=="TIMEOUT","R"].to_numpy(dtype=float)
    timeout_R_mean = float(timeout_R.mean()) if timeout_R.size else 0.0

    # Moyenne sur TOUS les jours calendrier tradables (Lun..Ven) présents dans le CSV
    avg_trades_per_day_all = float(trades / tradable_days_all) if tradable_days_all > 0 else 0.0

    # Comptes long/short (après filtres)
    ls_counts = trades_df["side"].value_counts().to_dict()
    ls_counts = {"long": int(ls_counts.get("long", 0)), "short": int(ls_counts.get("short", 0))}

    return {
        "Trades": trades,
        "Winrate_%": winrate,
        "R_mean": R_mean,
        "R_total": R_total,
        "Max_DD_R": max_dd,
        "Score": score,
        "EligibleDays": eligible_days,              # info secondaire (avec filtres DAYS/MOIS)
        "TradableDaysAll": tradable_days_all,       # dénominateur de la moyenne
        "Avg_Trades_per_Day_All": avg_trades_per_day_all,
        "SideMode": SIDE_MODE or "both",
        "LongShortCounts": ls_counts,
        "Pct_TP": pct_tp,
        "Pct_SL": pct_sl,
        "Pct_BE": pct_be,
        "Pct_TIMEOUT": pct_to,
        "Timeout_R_mean": timeout_R_mean,
        "TradesDF": trades_df
    }

# ==============================================
# MAIN
# ==============================================
if __name__ == "__main__":
    # Garde-fou BE
    if (BE_R is not None) and (BE_R >= TP_R):
        print(f"[AVERTISSEMENT] BE_R={BE_R} >= TP_R={TP_R} → BE désactivé pour éviter un comportement incohérent.")
        BE_R = None

    print("[SBA SIMPLE] Chargement CSV…")
    ohlc = load_ohlc_paris(CSV_PATH)

    print(f"Construction des setups sur {TIMEFRAME}…")
    setups = build_setups(ohlc, TIMEFRAME)

    print("Calcul des compteurs de jours…")
    elig_days = eligible_days_count(ohlc)              # avec filtres DAYS/MOIS (info)
    tradable_days_all = tradable_days_count_all(ohlc)  # Lun..Ven sans filtres (moyenne)

    print("Application des filtres et calcul des métriques…")
    res = apply_filters_and_metrics(setups, elig_days, tradable_days_all)

    print("=== BILAN GÉNÉRAL ===")
    print(f"Timeframe={TIMEFRAME} | TP_R={TP_R} | BE_R={BE_R} | Days={DAYS} | "
          f"Wick%≤{WICK_MAX} | InactiveMonths={INACTIVE_MONTHS} | SideMode={res['SideMode']}")
    print(f"Trades={res['Trades']} | Winrate%={res['Winrate_%']:.2f} | R_mean={res['R_mean']:.6f} | "
          f"R_total={res['R_total']:.6f} | Max_DD_R={res['Max_DD_R']:.6f} | Score={res['Score']:.6f}")
    print(f"Répartition après filtres — LONG: {res['LongShortCounts']['long']} | SHORT: {res['LongShortCounts']['short']}")

    print("=== JOURS & MOYENNES ===")
    print(f"Jours calendrier tradables (Lun→Ven, sans filtres) : {res['TradableDaysAll']}")
    print(f"Nb moyen de trades / jour (sur ces jours) : {res['Avg_Trades_per_Day_All']:.4f}")
    print(f"(Info) Jours éligibles avec filtres DAYS/MOIS : {res['EligibleDays']}")

    print("=== ISSUES DES TRADES ===")
    print(f"% TP : {res['Pct_TP']:.2f} | % SL : {res['Pct_SL']:.2f} | % BE : {res['Pct_BE']:.2f} | % Timeout : {res['Pct_TIMEOUT']:.2f}")
    print(f"R_mean des timeouts : {res['Timeout_R_mean']:.6f}")

    # ======= GRAPHIQUE : ÉVOLUTION DU R CUMULÉ PAR TRADE (repères d'année) =======
    trades_df = res["TradesDF"]
    if not trades_df.empty:
        fig, ax = plt.subplots(figsize=(8,4))
        ax.plot(trades_df["Trade#"], trades_df["CumR"], linewidth=1.3)  # ligne fine, continue, sans point
        ax.set_title("Évolution du R cumulé par trade (repères d'année)")
        ax.set_xlabel("Trade # (ordre chronologique)")
        ax.set_ylabel("R cumulé")
        ax.grid(True, linestyle="--", alpha=0.5)

        # Axe secondaire : trades cumulés (comparaison visuelle)
        ax2 = ax.twinx()
        ax2.plot(trades_df["Trade#"], trades_df["Trade#"], alpha=0.2, linewidth=0.8)
        ax2.set_ylabel("Trades cumulés")

        # Repères de changement d'année
        years = trades_df["ts_paris"].dt.year.to_numpy()
        change_idx = [i+1 for i in range(len(years)-1) if years[i+1] != years[i]]
        y_top = ax.get_ylim()[1]
        for i in change_idx:
            ax.axvline(i, linestyle="--", alpha=0.15, linewidth=0.8)
            ax.text(i+0.1, y_top, str(years[i]), rotation=90, va="top", ha="left", fontsize=8)

        if len(years) > 0:
            ax.text(trades_df["Trade#"].iloc[-1]+0.1, ax.get_ylim()[1], str(years[-1]),
                    rotation=90, va="top", ha="left", fontsize=8)

        plt.tight_layout()
        plt.show()
    else:
        print("(aucun trade après filtres — pas de graphique)")

    # ======= 10 DERNIERS TRADES =======
    print("=== 10 DERNIERS TRADES (après filtres) ===")
    if not trades_df.empty:
        last10 = trades_df.tail(10).copy()
        for _, row in last10.iterrows():
            d_in  = row["entry_ts"].strftime("%Y-%m-%d %H:%M")
            d_out = row["exit_ts"].strftime("%Y-%m-%d %H:%M")
            side  = row.get("side","")
            print(
                f"{d_in} → {d_out} | {side.upper():>5} | "
                f"Entry={row['entry_price']:.5f} | SL={row['stop_price']:.5f} | "
                f"Exit={row['exit_price']:.5f} | Outcome={row['Outcome']:<8} | "
                f"R={row['R']:+.2f} | CumR={row['CumR']:+.2f}"
            )
    else:
        print("(aucun)")

