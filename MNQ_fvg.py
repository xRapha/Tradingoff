#!/usr/bin/env python3
"""
ICT mechanical FVG strategy with:
- 3min execution TF
- 1h HTF bias
- FVG + BOS -> wait for retest in FVG -> enter on next candle's open
- Fixed SL in points, variable RR (TP = RR * SL)
- Session filter
- FVG size filter
- Stop size (points) sweep
- Parameter sweep to find top combos (like CL_4h.py)
- *** Single trade at a time: no overlapping trades allowed ***
"""

from __future__ import annotations

import pickle
import warnings
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# =====================================================
# CONFIGURATION
# =====================================================

# Fichier CSV minute (même format que ton M2K / ES, etc.)
CSV_PATH = Path("MNQ.csv")  # <<< ADAPTE ÇA

# Cache de pré-calcul (sera placé à côté du CSV)
DATA_CACHE_FILENAME: Optional[str] = None  # None => <stem>_ict_precalc.pkl
USE_DATA_CACHE: bool = True

# Timezones
DATA_TZ = "UTC"
LOCAL_TZ = "Europe/Paris"

# Timeframes
MAIN_TF = "3min"
HTF_TF = "60min"

# Config "de base" (pour affichage détaillé)
BASE_RR = 4.0  # R:R de la config de base
# On peut tester plusieurs stops "de base" d’un coup
BASE_STOP_POINTS_VALUES: Sequence[float] = [12.5]

MAX_TRADE_MINUTES = 24 * 60    # durée max d'un trade

# Param sweep (comme CL_4h)
RR_VALUES: Sequence[float] = [4.0]
STOP_POINTS_VALUES: Sequence[float] = [12.5]  # pour la grid combos
# Sessions : (nom, start_time, end_time) heure locale (Europe/Paris)
SESSION_WINDOWS: Sequence[Tuple[str, Optional[time], Optional[time]]] = [("NY_9h30_16h", time(0, 0), time(16, 44)),  # adapte si besoin
]
# FVG size filters (en points) : hauteur fvg_high - fvg_low
FVG_MIN_SIZES: Sequence[Optional[float]] = [None]
FVG_MAX_SIZES: Sequence[Optional[float]] = [None]

MIN_TRADES_PER_COMBO = 1000   # min trades pour considérer un combo
TOP_COMBOS = 10              # combien de combos top on affiche

# Affichage / graph
SHOW_PLOTS = True
SAVE_PLOTS = True
PLOT_DPI = 120

AggOHLC = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}


# =====================================================
# HELPERS GÉNÉRAUX
# =====================================================

def _norm(s: Any) -> str:
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())


def load_ohlc_paris(csv_path: Path) -> pd.DataFrame:
    """
    Charge le CSV minute, détecte colonne temps + OHLC, renvoie un DF indexé en Europe/Paris.
    """
    df = pd.read_csv(csv_path, sep=None, engine="python")
    cols = {c: _norm(c) for c in df.columns}

    # Colonne temps
    time_col_candidates = [
        k
        for k, v in cols.items()
        if ("timeleft" in v) or v.startswith("time") or ("datetime" in v) or v.endswith("time")
    ]
    if not time_col_candidates:
        raise ValueError("Impossible de détecter la colonne de temps.")
    time_col = time_col_candidates[0]

    # Colonnes OHLC
    open_col_list = [k for k, v in cols.items() if v.startswith("open")]
    high_col_list = [k for k, v in cols.items() if v.startswith("high")]
    low_col_list = [k for k, v in cols.items() if v.startswith("low")]
    close_col_list = [k for k, v in cols.items() if v.startswith("close")]
    if not (open_col_list and high_col_list and low_col_list and close_col_list):
        raise ValueError("Impossible de détecter les colonnes OHLC.")

    open_col, high_col, low_col, close_col = (
        open_col_list[0],
        high_col_list[0],
        low_col_list[0],
        close_col_list[0],
    )

    df = df[[time_col, open_col, high_col, low_col, close_col]].copy()
    df.columns = ["Time", "Open", "High", "Low", "Close"]

    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Time"]).sort_values("Time")

    # Timezone source
    if df["Time"].dt.tz is None:
        df["Time"] = df["Time"].dt.tz_localize(DATA_TZ)
    else:
        df["Time"] = df["Time"].dt.tz_convert(DATA_TZ)

    df["Time_PARIS"] = df["Time"].dt.tz_convert(LOCAL_TZ)
    ohlc_paris = df.set_index("Time_PARIS")[["Open", "High", "Low", "Close"]].copy()
    return ohlc_paris


# =====================================================
# MARKET STRUCTURE
# =====================================================

def detect_swings_simple(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    highs = df["High"].values
    lows = df["Low"].values
    n = len(df)
    swing_high = np.zeros(n, dtype=bool)
    swing_low = np.zeros(n, dtype=bool)
    for i in range(1, n - 1):
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            swing_high[i] = True
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            swing_low[i] = True
    return pd.Series(swing_high, index=df.index), pd.Series(swing_low, index=df.index)


def prev_swings_values(
    df: pd.DataFrame, swing_high: pd.Series, swing_low: pd.Series
) -> Tuple[pd.Series, pd.Series]:
    n = len(df)
    prev_sh = np.full(n, np.nan)
    prev_sl = np.full(n, np.nan)
    highs = df["High"].values
    lows = df["Low"].values
    last_sh = np.nan
    last_sl = np.nan

    for i in range(n):
        prev_sh[i] = last_sh
        prev_sl[i] = last_sl
        if swing_high.iat[i]:
            last_sh = highs[i]
        if swing_low.iat[i]:
            last_sl = lows[i]

    return pd.Series(prev_sh, index=df.index), pd.Series(prev_sl, index=df.index)


def compute_ms_bias(df: pd.DataFrame) -> pd.Series:
    """
    Market structure simple :
    - bull si close casse le dernier swing high
    - bear si close casse le dernier swing low
    """
    sh, sl = detect_swings_simple(df)
    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    n = len(df)

    bias = []
    last_sh_val = np.nan
    last_sl_val = np.nan
    state = "none"

    for i in range(n):
        if sh.iat[i]:
            last_sh_val = highs[i]
        if sl.iat[i]:
            last_sl_val = lows[i]

        bos = None
        if not np.isnan(last_sh_val) and closes[i] > last_sh_val:
            bos = "bull"
        if not np.isnan(last_sl_val) and closes[i] < last_sl_val:
            if bos is None:
                bos = "bear"
            else:
                if abs(closes[i] - last_sl_val) > abs(closes[i] - last_sh_val):
                    bos = "bear"

        if bos == "bull":
            state = "bull"
        elif bos == "bear":
            state = "bear"

        bias.append(state)

    return pd.Series(bias, index=df.index)


# =====================================================
# FVG ICT (3 BOUGIES)
# =====================================================

def detect_fvg_m3(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    FVG ICT classique sur 3 bougies (i-1, i, i+1) :

    - FVG haussier (long) :
        High(i-1) < Low(i+1)
        => zone [High(i-1), Low(i+1)]

    - FVG baissier (short) :
        Low(i-1) > High(i+1)
        => zone [High(i+1), Low(i-1)]

    Indexé sur la bougie i (celle du milieu).
    """
    n = len(df)
    side = pd.Series([None] * n, index=df.index, dtype=object)
    fvg_low = pd.Series(np.nan, index=df.index)
    fvg_high = pd.Series(np.nan, index=df.index)

    highs = df["High"].values
    lows = df["Low"].values

    for i in range(1, n - 1):
        i_prev = i - 1
        i_next = i + 1

        h_prev = highs[i_prev]
        l_prev = lows[i_prev]
        h_next = highs[i_next]
        l_next = lows[i_next]

        # FVG long
        if h_prev < l_next:
            side.iat[i] = "long"
            zone_low = h_prev
            zone_high = l_next
            if zone_high > zone_low:
                fvg_low.iat[i] = zone_low
                fvg_high.iat[i] = zone_high
            else:
                side.iat[i] = None

        # FVG short
        elif l_prev > h_next:
            side.iat[i] = "short"
            zone_low = h_next
            zone_high = l_prev
            if zone_high > zone_low:
                fvg_low.iat[i] = zone_low
                fvg_high.iat[i] = zone_high
            else:
                side.iat[i] = None

    return side, fvg_low, fvg_high


# =====================================================
# DATA PRECALC + CACHE
# =====================================================

@dataclass
class ICTPrecalc:
    ohlc_paris: pd.DataFrame
    df_m3: pd.DataFrame
    df_h1: pd.DataFrame
    h1_bias_m3: pd.Series
    fvg_side: pd.Series
    fvg_low: pd.Series
    fvg_high: pd.Series
    prev_sh: pd.Series
    prev_sl: pd.Series
    closes: np.ndarray


def _load_cached_precalc(cache_path: Path, sources: Sequence[Path]) -> Optional[ICTPrecalc]:
    if not cache_path.exists():
        return None
    cache_mtime = cache_path.stat().st_mtime
    for src in sources:
        if src.exists() and src.stat().st_mtime > cache_mtime:
            return None
    with cache_path.open("rb") as handle:
        return pickle.load(handle)


def _store_cached_precalc(cache_path: Path, precalc: ICTPrecalc) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("wb") as handle:
        pickle.dump(precalc, handle, protocol=pickle.HIGHEST_PROTOCOL)


def build_or_load_precalc(csv_path: Path) -> ICTPrecalc:
    csv_path = csv_path.resolve()
    stem = csv_path.stem
    cache_name = DATA_CACHE_FILENAME or f"{stem}_ict_precalc.pkl"
    cache_path = csv_path.with_name(cache_name)

    sources: List[Path] = [csv_path]

    if USE_DATA_CACHE:
        cached = _load_cached_precalc(cache_path, sources)
        if cached is not None:
            print(f"[ICT] Pré-calcul rechargé depuis le cache : {cache_path.name}")
            return cached

    # 1) Lecture minute + timezone
    ohlc_paris = load_ohlc_paris(csv_path)

    # 2) TF M3 / H1
    df_m3 = ohlc_paris.resample(MAIN_TF, label="left", closed="left").agg(AggOHLC).dropna()
    df_h1 = ohlc_paris.resample(HTF_TF, label="left", closed="left").agg(AggOHLC).dropna()

    # 3) Bias H1 -> projeté sur M3
    h1_bias = compute_ms_bias(df_h1)
    h1_bias_m3 = h1_bias.reindex(df_m3.index, method="ffill")

    # 4) Swings / BOS / FVG sur M3
    sh_m3, sl_m3 = detect_swings_simple(df_m3)
    prev_sh, prev_sl = prev_swings_values(df_m3, sh_m3, sl_m3)
    closes = df_m3["Close"].values
    fvg_side, fvg_low, fvg_high = detect_fvg_m3(df_m3)

    precalc = ICTPrecalc(
        ohlc_paris=ohlc_paris,
        df_m3=df_m3,
        df_h1=df_h1,
        h1_bias_m3=h1_bias_m3,
        fvg_side=fvg_side,
        fvg_low=fvg_low,
        fvg_high=fvg_high,
        prev_sh=prev_sh,
        prev_sl=prev_sl,
        closes=closes,
    )

    if USE_DATA_CACHE:
        _store_cached_precalc(cache_path, precalc)
        print(f"[ICT] Pré-calcul sauvegardé dans le cache : {cache_path.name}")

    return precalc


# =====================================================
# CANDIDATES FVG ICT (FVG + BOS + RETEST)
# =====================================================

@dataclass
class CandidateFVG:
    day_start: pd.Timestamp
    side: str           # "long" / "short"
    fvg_ts: pd.Timestamp
    fvg_low: float
    fvg_high: float
    fvg_size: float
    entry_ts: pd.Timestamp
    entry_price: float
    post_times: List[pd.Timestamp]   # includes entry bar and following
    post_highs: List[float]
    post_lows: List[float]
    post_closes: List[float]


def build_candidates_from_precalc(precalc: ICTPrecalc) -> Dict[pd.Timestamp, List[CandidateFVG]]:
    """
    Pour chaque FVG qui casse une structure ET aligné avec le biais H1 :

    - Dès que le FVG (3 bougies i-1, i, i+1) est formé,
      on considère qu'un ordre LIMIT est posé sur l'extrémité du FVG :
        * long  : buy limit à fvg_high
        * short : sell limit à fvg_low

    - Le FVG est connu à la clôture de la 3ᵉ bougie (i+1),
      donc on ne peut commencer à être exécuté qu'à partir de i+2.

    - On cherche la première bougie (>= i+2) dont le range High/Low
      touche ce prix limite -> entrée au prix limite, dans cette bougie.

    - Ensuite on stocke tout le chemin post-entrée pour pouvoir rejouer
      le trade avec différents R:R / stops / filtres.
    """
    df_m3 = precalc.df_m3
    h1_bias_m3 = precalc.h1_bias_m3
    fvg_side = precalc.fvg_side
    fvg_low = precalc.fvg_low
    fvg_high = precalc.fvg_high
    prev_sh = precalc.prev_sh
    prev_sl = precalc.prev_sl
    closes = precalc.closes

    candidates_by_day: Dict[pd.Timestamp, List[CandidateFVG]] = {}

    idx_list = df_m3.index
    n = len(df_m3)

    for i, ts in enumerate(idx_list):
        if i < 2:
            continue

        # -------- 1) Détection BOS (cassure de structure) --------
        bos_side: Optional[str] = None
        if not np.isnan(prev_sh.iat[i]) and closes[i] > prev_sh.iat[i]:
            bos_side = "long"
        if not np.isnan(prev_sl.iat[i]) and closes[i] < prev_sl.iat[i]:
            if bos_side is None:
                bos_side = "short"
            else:
                if abs(closes[i] - prev_sl.iat[i]) > abs(closes[i] - prev_sh.iat[i]):
                    bos_side = "short"

        if bos_side is None:
            continue

        # -------- 2) FVG même sens sur cette bougie i --------
        if fvg_side.iat[i] != bos_side:
            continue

        # -------- 3) Biais H1 aligné --------
        bias_h1 = h1_bias_m3.iat[i]
        if (bos_side == "long" and bias_h1 != "bull") or (bos_side == "short" and bias_h1 != "bear"):
            continue

        this_fvg_low = float(fvg_low.iat[i])
        this_fvg_high = float(fvg_high.iat[i])
        if not np.isfinite(this_fvg_low) or not np.isfinite(this_fvg_high):
            continue
        if this_fvg_high <= this_fvg_low:
            continue

        # -------- 4) Prix de la LIMIT à placer dès que le FVG est formé --------
        # FVG sur 3 bougies (i-1, i, i+1) -> complet à la fin de i+1,
        # donc premier retest possible sur la bougie i+2.
        if bos_side == "long":
            limit_price = this_fvg_high  # borne haute du FVG
        else:
            limit_price = this_fvg_low   # borne basse du FVG

        # -------- 5) Chercher la première bougie qui touche le prix limite --------
        # On commence à i+2 (après la 3ᵉ bougie du pattern)
        if i + 2 >= n:
            continue
        after = df_m3.iloc[i + 2 :]

        entry_idx: Optional[int] = None
        for ts2, row in after.iterrows():
            hi = float(row["High"])
            lo = float(row["Low"])
            # La bougie touche la limite ?
            if lo <= limit_price <= hi:
                entry_idx = df_m3.index.get_loc(ts2)
                break

        if entry_idx is None:
            # L'ordre limit n'a jamais été exécuté
            continue

        entry_ts = idx_list[entry_idx]
        entry_price = float(limit_price)

        # -------- 6) Construire le chemin post-entrée --------
        max_end_time = min(entry_ts + pd.Timedelta(minutes=int(MAX_TRADE_MINUTES)), idx_list[-1])
        trail = df_m3[(df_m3.index >= entry_ts) & (df_m3.index <= max_end_time)]
        if trail.empty:
            continue

        post_times = list(trail.index)
        post_highs = list(trail["High"].astype(float).values)
        post_lows = list(trail["Low"].astype(float).values)
        post_closes = list(trail["Close"].astype(float).values)

        fvg_size = this_fvg_high - this_fvg_low
        day_start = entry_ts.normalize()  # minuit jour local

        cand = CandidateFVG(
            day_start=day_start,
            side=bos_side,
            fvg_ts=ts,
            fvg_low=this_fvg_low,
            fvg_high=this_fvg_high,
            fvg_size=fvg_size,
            entry_ts=entry_ts,
            entry_price=entry_price,
            post_times=post_times,
            post_highs=post_highs,
            post_lows=post_lows,
            post_closes=post_closes,
        )
        candidates_by_day.setdefault(day_start, []).append(cand)

    return candidates_by_day

# =====================================================
# SIMULATION D’UNE CANDIDATE POUR (RR, STOP) DONNÉ
# =====================================================

def simulate_candidate_for_rr(
    cand: CandidateFVG,
    rr: float,
    stop_points: float,
) -> Tuple[pd.Timestamp, pd.Timestamp, float, str, float]:
    """
    Applique les règles d'entrée / SL / TP sur une candidate FVG
    pour un R:R et un stop donné.

    Règles 1ʳᵉ bougie après entrée :
      - SL et TP touchés -> SL direct
      - SL seul -> SL direct
      - TP seul -> TP ignoré

    Bougies suivantes :
      - SL et TP touchés -> SL (priorité au stop)
      - SL -> SL
      - TP -> TP

    Retourne (entry_ts, exit_ts, exit_price, reason, R).
    """
    entry_ts = cand.entry_ts
    entry_price = cand.entry_price
    side = cand.side

    stop_points = float(stop_points)

    if side == "long":
        sl = entry_price - stop_points
        tp = entry_price + float(rr * stop_points)
    else:
        sl = entry_price + stop_points
        tp = entry_price - float(rr * stop_points)

    times = cand.post_times
    highs = cand.post_highs
    lows = cand.post_lows
    closes = cand.post_closes

    if not times:
        return entry_ts, entry_ts, entry_price, "TIMEOUT", 0.0

    first_bar = True
    for ts, hi, lo in zip(times, highs, lows):
        hi = float(hi)
        lo = float(lo)

        if first_bar:
            if side == "long":
                touched_tp = hi >= tp
                touched_sl = lo <= sl
            else:
                touched_tp = lo <= tp
                touched_sl = hi >= sl

            if touched_tp and touched_sl:
                return entry_ts, ts, sl, "SL", -1.0
            if touched_sl:
                return entry_ts, ts, sl, "SL", -1.0
            if touched_tp:
                # TP ignoré sur la 1ʳᵉ bougie
                first_bar = False
                continue

            first_bar = False
            continue

        # à partir de la 2e bougie
        if side == "long":
            touched_tp = hi >= tp
            touched_sl = lo <= sl
        else:
            touched_tp = lo <= tp
            touched_sl = hi >= sl

        if touched_tp and touched_sl:
            # priorité au stop
            return entry_ts, ts, sl, "SL", -1.0
        if touched_sl:
            return entry_ts, ts, sl, "SL", -1.0
        if touched_tp:
            return entry_ts, ts, tp, rr

    # TIMEOUT : on sort à la dernière close
    last_ts = times[-1]
    last_close = float(closes[-1])
    if side == "long":
        R = (last_close - entry_price) / stop_points
    else:
        R = (entry_price - last_close) / stop_points
    return entry_ts, last_ts, last_close, "TIMEOUT", float(R)


# =====================================================
# BACKTEST POUR UN SET DE PARAMS
# (AVEC CONTRAINTE "UN SEUL TRADE À LA FOIS")
# =====================================================

def session_filter_ok(ts: pd.Timestamp, session_start: Optional[time], session_end: Optional[time]) -> bool:
    local_ts = ts.tz_convert(LOCAL_TZ)
    t = local_ts.time()
    if session_start is not None and t < session_start:
        return False
    if session_end is not None and t >= session_end:
        return False
    return True


def fvg_size_filter_ok(size: float, min_size: Optional[float], max_size: Optional[float]) -> bool:
    if min_size is not None and size < min_size:
        return False
    if max_size is not None and size > max_size:
        return False
    return True


def generate_trades_for_combo(
    candidates_by_day: Dict[pd.Timestamp, List[CandidateFVG]],
    rr: float,
    stop_points: float,
    session_start: Optional[time],
    session_end: Optional[time],
    min_fvg_size: Optional[float],
    max_fvg_size: Optional[float],
) -> pd.DataFrame:
    """
    Génère les trades pour un combo donné (RR, stop, session, taille FVG)
    en respectant la contrainte : un seul trade à la fois (pas de chevauchement).

    Tolère les fonctions simulate_candidate_for_rr qui renvoient 4 ou 5 valeurs.
    """
    # 1) on rassemble tous les candidats filtrés
    filtered_cands: List[CandidateFVG] = []

    for _, cands in candidates_by_day.items():
        for cand in cands:
            if not session_filter_ok(cand.entry_ts, session_start, session_end):
                continue
            if not fvg_size_filter_ok(cand.fvg_size, min_fvg_size, max_fvg_size):
                continue
            filtered_cands.append(cand)

    if not filtered_cands:
        return pd.DataFrame(
            columns=[
                "date", "side", "fvg_ts", "entry_ts", "entry",
                "fvg_low", "fvg_high", "exit_ts", "exit_px", "reason", "R", "cum_R"
            ]
        )

    # 2) tri par ordre chronologique d'entrée
    filtered_cands.sort(key=lambda c: c.entry_ts)

    rows: List[List[Any]] = []
    open_until: Optional[pd.Timestamp] = None

    for cand in filtered_cands:
        # ⛔ Chevauchement interdit : on refuse tout entry_ts <= dernier exit_ts
        if open_until is not None and cand.entry_ts <= open_until:
            continue

        res = simulate_candidate_for_rr(cand, rr, stop_points)

        # Compatibilité 4 / 5 valeurs
        if isinstance(res, tuple):
            if len(res) == 5:
                entry_ts, exit_ts, exit_px, reason, R = res
            elif len(res) == 4:
                entry_ts, exit_ts, exit_px, reason = res
                if reason == "TP":
                    R = float(rr)
                elif reason == "SL":
                    R = -1.0
                else:  # TIMEOUT ou autre
                    if cand.side == "long":
                        R = (exit_px - cand.entry_price) / float(stop_points)
                    else:
                        R = (cand.entry_price - exit_px) / float(stop_points)
            else:
                raise ValueError(
                    f"simulate_candidate_for_rr returned {len(res)} values, expected 4 or 5."
                )
        else:
            raise TypeError("simulate_candidate_for_rr must return a tuple-like object.")

        # le prochain trade ne peut commencer qu'après cette date
        open_until = exit_ts

        rows.append(
            [
                entry_ts.date(),
                cand.side,
                cand.fvg_ts,
                entry_ts,
                cand.entry_price,
                cand.fvg_low,
                cand.fvg_high,
                exit_ts,
                exit_px,
                reason,
                R,
            ]
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "date", "side", "fvg_ts", "entry_ts", "entry",
                "fvg_low", "fvg_high", "exit_ts", "exit_px", "reason", "R", "cum_R"
            ]
        )

    trades = pd.DataFrame(
        rows,
        columns=[
            "date",
            "side",
            "fvg_ts",
            "entry_ts",
            "entry",
            "fvg_low",
            "fvg_high",
            "exit_ts",
            "exit_px",
            "reason",
            "R",
        ],
    )

    # On calcule l’équité dans l’ordre des sorties…
    t_exit_sorted = trades.sort_values("exit_ts")
    cum_R = t_exit_sorted["R"].cumsum()
    trades["cum_R"] = cum_R.reindex(trades.index)

    # …mais on sauve le CSV classé par ordre D’ENTRÉE (plus lisible pour voir les chevauchements)
    trades = trades.sort_values("entry_ts").reset_index(drop=True)

    return trades


# =====================================================
# PARAMETER SWEEP (TOP COMBOS)
# =====================================================

@dataclass
class ComboSummary:
    rr: float
    stop_points: float
    session_name: str
    session_start: Optional[time]
    session_end: Optional[time]
    min_fvg_size: Optional[float]
    max_fvg_size: Optional[float]
    trades: int
    wins: int
    losses: int
    timeouts: int
    total_R: float
    avg_R: float
    winrate: float


def run_combo_stats(
    candidates_by_day: Dict[pd.Timestamp, List[CandidateFVG]],
    rr: float,
    stop_points: float,
    session_start: Optional[time],
    session_end: Optional[time],
    min_fvg_size: Optional[float],
    max_fvg_size: Optional[float],
) -> ComboSummary:
    trades_df = generate_trades_for_combo(
        candidates_by_day,
        rr=rr,
        stop_points=stop_points,
        session_start=session_start,
        session_end=session_end,
        min_fvg_size=min_fvg_size,
        max_fvg_size=max_fvg_size,
    )

    trades = len(trades_df)
    if trades == 0:
        return ComboSummary(
            rr=rr,
            stop_points=stop_points,
            session_name="",
            session_start=session_start,
            session_end=session_end,
            min_fvg_size=min_fvg_size,
            max_fvg_size=max_fvg_size,
            trades=0,
            wins=0,
            losses=0,
            timeouts=0,
            total_R=0.0,
            avg_R=0.0,
            winrate=0.0,
        )

    wins = (trades_df["reason"] == "TP").sum()
    losses = (trades_df["reason"] == "SL").sum()
    timeouts = (trades_df["reason"] == "TIMEOUT").sum()
    total_R = trades_df["R"].sum()
    avg_R = trades_df["R"].mean()
    winrate = wins / trades * 100.0

    return ComboSummary(
        rr=rr,
        stop_points=stop_points,
        session_name="",
        session_start=session_start,
        session_end=session_end,
        min_fvg_size=min_fvg_size,
        max_fvg_size=max_fvg_size,
        trades=trades,
        wins=wins,
        losses=losses,
        timeouts=timeouts,
        total_R=float(total_R),
        avg_R=float(avg_R),
        winrate=float(winrate),
    )


def sweep_parameter_grid(
    candidates_by_day: Dict[pd.Timestamp, List[CandidateFVG]],
    rr_values: Sequence[float],
    stop_points_values: Sequence[float],
    session_windows: Sequence[Tuple[str, Optional[time], Optional[time]]],
    fvg_min_sizes: Sequence[Optional[float]],
    fvg_max_sizes: Sequence[Optional[float]],
    min_trades: int = MIN_TRADES_PER_COMBO,
) -> List[ComboSummary]:
    results: List[ComboSummary] = []

    for rr in rr_values:
        for stop_points in stop_points_values:
            for session_name, sess_start, sess_end in session_windows:
                for min_size in fvg_min_sizes:
                    for max_size in fvg_max_sizes:
                        if (
                            min_size is not None
                            and max_size is not None
                            and min_size > max_size
                        ):
                            continue
                        combo = run_combo_stats(
                            candidates_by_day,
                            rr=rr,
                            stop_points=stop_points,
                            session_start=sess_start,
                            session_end=sess_end,
                            min_fvg_size=min_size,
                            max_fvg_size=max_size,
                        )
                        if combo.trades < min_trades:
                            continue
                        combo.session_name = session_name
                        results.append(combo)

    # tri décroissant sur total_R
    results.sort(key=lambda c: c.total_R, reverse=True)
    return results


# =====================================================
# STATS & RÉSUMÉS
# =====================================================

def summarize_global(trades: pd.DataFrame, ohlc_paris: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            [
                [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ]
            ],
            columns=[
                "Trades",
                "TP",
                "SL",
                "BE",
                "TIMEOUT",
                "Winrate_%",
                "R_mean",
                "R_total",
                "Max_DD_R",
                "Trades_per_day",
                "R_mean_TIMEOUT",
            ],
        )

    t = trades.copy()
    n = len(t)
    tp_c = (t["reason"] == "TP").sum()
    sl_c = (t["reason"] == "SL").sum()
    be_c = 0
    to_c = (t["reason"] == "TIMEOUT").sum()
    winrate = (tp_c / n) * 100.0
    r_mean = t["R"].mean()
    r_total = t["R"].sum()
    r_timeout = t.loc[t["reason"] == "TIMEOUT", "R"].mean() if to_c > 0 else 0.0
    max_dd = (t["cum_R"].cummax() - t["cum_R"]).max()

    nb_days = (ohlc_paris.index.date.max() - ohlc_paris.index.date.min()).days + 1
    trades_per_day = (n / nb_days) if nb_days > 0 else 0.0

    return pd.DataFrame(
        [
            [
                n,
                tp_c,
                sl_c,
                be_c,
                to_c,
                winrate,
                r_mean,
                r_total,
                float(max_dd),
                trades_per_day,
                r_timeout,
            ]
        ],
        columns=[
            "Trades",
            "TP",
            "SL",
            "BE",
            "TIMEOUT",
            "Winrate_%",
            "R_mean",
            "R_total",
            "Max_DD_R",
            "Trades_per_day",
            "R_mean_TIMEOUT",
        ],
    )


def summarize_monthly(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "Trades",
                "TP",
                "SL",
                "TIMEOUT",
                "Winrate_%",
                "R_mean",
                "R_total",
                "Max_DD_R",
            ]
        )

    t = trades.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        t["month"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.to_period("M").astype(str)

    rows = []
    for m, g in t.groupby("month"):
        g = g.sort_values("exit_ts")
        cum = g["R"].cumsum()
        rows.append(
            [
                m,
                len(g),
                (g["reason"] == "TP").sum(),
                (g["reason"] == "SL").sum(),
                (g["reason"] == "TIMEOUT").sum(),
                g["reason"].eq("TP").mean() * 100.0,
                g["R"].mean(),
                g["R"].sum(),
                float((cum.cummax() - cum).max()),
            ]
        )

    return (
        pd.DataFrame(
            rows,
            columns=[
                "month",
                "Trades",
                "TP",
                "SL",
                "TIMEOUT",
                "Winrate_%",
                "R_mean",
                "R_total",
                "Max_DD_R",
            ],
        )
        .sort_values("month")
        .reset_index(drop=True)
    )


def make_last_10(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.sort_values("exit_ts").tail(10).copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        t["fvg_time"] = pd.to_datetime(t["fvg_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
        t["entry_time"] = pd.to_datetime(t["entry_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
        t["exit_time"] = pd.to_datetime(t["exit_ts"]).dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    return t[
        [
            "date",
            "side",
            "fvg_time",
            "entry_time",
            "entry",
            "exit_time",
            "exit_px",
            "reason",
            "R",
        ]
    ].copy()


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    precalc = build_or_load_precalc(CSV_PATH)
    candidates_by_day = build_candidates_from_precalc(precalc)

    if not candidates_by_day:
        print("Aucun candidat FVG (BOS + FVG + retest) trouvé.")
        raise SystemExit

    ohlc_paris = precalc.ohlc_paris

    # --- Backtests de base pour tous les stops définis dans BASE_STOP_POINTS_VALUES ---
    print("\n=== BACKTESTS DE BASE (RR = {:.2f}) POUR CHAQUE STOP DÉFINI ===".format(BASE_RR))
    first_base_trades = None
    first_stop_for_plot: Optional[float] = None

    for idx, stop_points in enumerate(BASE_STOP_POINTS_VALUES, start=1):
        print(f"\n--- Base config #{idx} : stop = {stop_points} points ---")
        base_trades = generate_trades_for_combo(
            candidates_by_day,
            rr=BASE_RR,
            stop_points=stop_points,
            session_start=None,
            session_end=None,
            min_fvg_size=None,
            max_fvg_size=None,
        )

        if base_trades.empty:
            print("Aucun trade exécuté pour ce stop.")
            continue

        suffix = f"_stop_{str(stop_points).replace('.', 'p')}"

        trades_file = f"ict_fvg_trades{suffix}.csv"
        global_file = f"ict_fvg_global_summary{suffix}.csv"
        monthly_file = f"ict_fvg_monthly{suffix}.csv"
        last10_file = f"ict_fvg_last10{suffix}.csv"

        base_trades.to_csv(trades_file, index=False)
        global_sum = summarize_global(base_trades, ohlc_paris)
        global_sum.to_csv(global_file, index=False)
        monthly = summarize_monthly(base_trades)
        monthly.to_csv(monthly_file, index=False)
        last10 = make_last_10(base_trades)
        last10.to_csv(last10_file, index=False)

        print("\nBilan global pour stop =", stop_points)
        print(global_sum.to_string(index=False))

        print("\nBilan mensuel pour stop =", stop_points)
        if monthly.empty:
            print("(Aucun trade dans l'échantillon)")
        else:
            print(monthly.to_string(index=False))

        print("\n10 derniers trades (avec heure du FVG) pour stop =", stop_points)
        print(last10.to_string(index=False))

        if first_base_trades is None:
            first_base_trades = base_trades
            first_stop_for_plot = stop_points

    if first_base_trades is None:
        print("\nAucun trade exécuté pour tous les stops de BASE_STOP_POINTS_VALUES.")
        raise SystemExit

    # --- Sweep paramètres (RR / stop size / sessions / taille FVG) ---
    print("\n=== Recherche des meilleurs combos (RR / stop / session / taille FVG) ===")
    combos = sweep_parameter_grid(
        candidates_by_day,
        rr_values=RR_VALUES,
        stop_points_values=STOP_POINTS_VALUES,
        session_windows=SESSION_WINDOWS,
        fvg_min_sizes=FVG_MIN_SIZES,
        fvg_max_sizes=FVG_MAX_SIZES,
        min_trades=MIN_TRADES_PER_COMBO,
    )

    if not combos:
        print("Aucun combo ne remplit le critère de nombre minimum de trades.")
    else:
        print(f"Top {min(TOP_COMBOS, len(combos))} combos (classés par R total décroissant) :\n")
        print(
            " idx |   RR | stop | session           | FVG_min | FVG_max | trades | win%   | avg_R  | total_R"
        )
        print("-" * 104)
        for idx, c in enumerate(combos[:TOP_COMBOS], start=1):
            sess_label = c.session_name
            fmin = f"{c.min_fvg_size:.2f}" if c.min_fvg_size is not None else "-"
            fmax = f"{c.max_fvg_size:.2f}" if c.max_fvg_size is not None else "-"
            print(
                f"{idx:>4} | {c.rr:4.2f} | {c.stop_points:4.1f} | {sess_label:<16} | "
                f"{fmin:>7} | {fmax:>7} | {c.trades:>6} | {c.winrate:6.2f} | "
                f"{c.avg_R:6.4f} | {c.total_R:7.2f}"
            )

    # --- Courbe d'équité pour le premier stop de base ---
    if first_base_trades is not None:
        plt.figure(figsize=(9, 4))
        t_sorted = first_base_trades.sort_values("exit_ts")
        plt.plot(t_sorted["exit_ts"], t_sorted["cum_R"])
        sp_label = first_stop_for_plot if first_stop_for_plot is not None else "?"
        plt.title(f"Équité cumulée (R) — ICT FVG ES (3min / 1H) — base (stop={sp_label})")
        plt.xlabel("Date")
        plt.ylabel("R cumulés")
        plt.grid(True)
        plt.tight_layout()
        if SAVE_PLOTS:
            plt.savefig("ict_fvg_equity_curve.png", dpi=PLOT_DPI)
        if SHOW_PLOTS:
            plt.show()
        else:
            plt.close()

    print(
        "\nFichiers sauvegardés (par stop de BASE_STOP_POINTS_VALUES) : "
        "ict_fvg_trades_stop_*.csv, ict_fvg_global_summary_stop_*.csv, "
        "ict_fvg_monthly_stop_*.csv, ict_fvg_last10_stop_*.csv, ict_fvg_equity_curve.png"
    )
