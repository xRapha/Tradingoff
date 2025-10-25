using System;
using System.Collections.Generic;
using TradingPlatform.BusinessLayer;

public class OPR45_NQ_Live : Strategy
{
    // ===== Inputs =====
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    [InputParameter("Tick Size", 30)] public double TickSize = 0.25;
    [InputParameter("Tick Value USD", 40)] public double TickValueUSD = 0.5;
    [InputParameter("Risk USD", 50)] public double RiskUSD = 150.0;
    [InputParameter("Max Contracts", 60)] public int MaxContracts = 10;
    [InputParameter("Tag", 70)] public string Tag = "OPR45-NQ";

    // Règles / filtres (valeurs par défaut génériques, ajustables dans l'UI)
    [InputParameter("TP in R", 80)] public double TP_R = 2.0;
    [InputParameter("STOP_FRAC (0..1)", 90)] public double STOP_FRAC = 0.50;
    [InputParameter("Retest minutes", 100)] public int RETEST_MIN = 120;
    [InputParameter("Body outside ≥", 110)] public double BODY_OUTSIDE_FRAC_MIN = 0.35;
    [InputParameter("Range/Box ≥", 120)] public double RANGE_VS_BOX_MIN = 0.30;
    [InputParameter("Wick out ≤", 130)] public double WICK_OUT_MAX_FRAC = 0.60;
    [InputParameter("OVEREXT_MULT", 140)] public double OVEREXT_MULT = 1.25;

    [InputParameter("Box MAX (pts)", 150)] public double BOX_MAX = 240.00;

    // Fenêtres NY
    private readonly TimeSpan OpenStart = new(9, 30, 0);
    private readonly TimeSpan OpenEnd = new(9, 44, 59);
    private readonly TimeSpan BreakStart = new(9, 45, 0);
    private readonly TimeSpan BreakEnd = new(11, 25, 59);

    // Flatten (heure NY)
    [InputParameter("Flatten Hour NY", 170)] public int FlattenHour = 15;
    [InputParameter("Flatten Minute NY", 180)] public int FlattenMinute = 59;

    // ===== Internes =====
    private HistoricalData hd1m;                 // série 1m moteur (Last)
    private TimeZoneInfo tzNy;
    private System.Threading.Timer flattenTimer;

    // Box
    private DateTime currentNyDate;
    private double boxHigh, boxLow; private bool boxReady;

    // Agrégateur 15m (moteur)
    private struct Ohlc { public double O, H, L, C; public DateTime OpenUtc, CloseUtc; }
    private readonly Dictionary<DateTime, Ohlc> agg15 = new();
    private readonly HashSet<DateTime> validatedBuckets = new();

    // Cassure & retest
    private string breakSide;                 // "long"/"short"
    private DateTime breakStartParis, breakEndParis;
    private double breakClosePx;              // close 15m moteur (pour overext)
    private bool breakDetected;
    private DateTime retestDeadlineParis;
    private bool tradeDoneToday;

    // Overextension buffer
    private double postBreakMaxHigh, postBreakMinLow; private bool postBreakInit;

    // ===== 1m LIVE (ticks → bar) pour coords exactes à la cassure =====
    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC;
    private long liveV;
    // minuteCloseUtc -> OHLCV de la 1m live
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    // ========= Lifecycle =========
    protected override void OnRun()
    {
        if (Symbol == null || Account == null) { Log("Select Symbol + Account", StrategyLoggingLevel.Error); return; }

        tzNy = GetNyTz();
        ResetDay(DateTime.UtcNow);

        // Série 1m moteur en Last (TRADES)
        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, DateTime.UtcNow.AddHours(-12));
        Log("[INIT] HistoryType=Last period=1m (preload=" + (hd1m?.Count) + ")");
        hd1m.NewHistoryItem += OnNewEngineBar;

        // Backfill box 09:30–09:44 NY
        TryBackfillBoxFromHistory();

        // Flux LIVE ticks → 1m
        Symbol.NewLast += OnNewLast;

        ScheduleDailyFlatten();
        Log("OPR45_NQ_Live started (ticks→1m live + backfill box).");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewEngineBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        flattenTimer?.Dispose();
        Log("OPR45_NQ_Live stopped.");
    }

    // ========= Ticks → 1m LIVE (cache) =========
    private void OnNewLast(Symbol symbol, Last last)
    {
        if (symbol != this.Symbol) return;

        double price = last.Price;
        DateTime? tickTime = ParseDate(TryGet(last, "ServerTime") ?? TryGet(last, "Time"));
        DateTime tUtc = tickTime.HasValue ? DateTime.SpecifyKind(tickTime.Value, DateTimeKind.Utc) : DateTime.UtcNow;

        // minute de départ UTC
        DateTime start = new DateTime(tUtc.Year, tUtc.Month, tUtc.Day, tUtc.Hour, tUtc.Minute, 0, DateTimeKind.Utc);

        // changement de minute → clôturer la 1m live précédente
        if (liveBarStartUtc != null && start > liveBarStartUtc.Value)
        {
            CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
            liveBarStartUtc = null;
        }

        if (liveBarStartUtc == null)
        {
            liveBarStartUtc = start;
            liveO = liveH = liveL = liveC = price;
            liveV = 0;
        }

        if (price > liveH) liveH = price;
        if (price < liveL) liveL = price;
        liveC = price;

        long size = ParseLong(TryGet(last, "Volume") ?? TryGet(last, "Size") ?? TryGet(last, "Qty")) ?? 1;
        liveV += size;
    }

    private void CloseCurrentLiveBar(DateTime closeUtc)
    {
        live1mAtClose[closeUtc] = (liveO, liveH, liveL, liveC, liveV);
        // [LIVE 1m CLOSE] volontairement désactivé pour éviter le spam.
    }

    // ===== Helpers BOX : merge moteur + live =====
    private void UpdateBoxWith(double hi, double lo)
    {
        if (double.IsNaN(hi) || double.IsNaN(lo)) return;
        if (boxHigh == double.MinValue) { boxHigh = hi; boxLow = lo; }
        else { boxHigh = Math.Max(boxHigh, hi); boxLow = Math.Min(boxLow, lo); }
    }

    private void TryMergeLiveMinute(DateTime engineBarOpenUtc)
    {
        var closeUtc = engineBarOpenUtc.AddMinutes(1);
        if (live1mAtClose.TryGetValue(closeUtc, out var lv))
        {
            UpdateBoxWith(lv.H, lv.L);
        }
    }

    // ========= 1m MOTEUR → box + 15m + retest =========
    private void OnNewEngineBar(object s, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar bar) return;

        DateTime barOpenUtc = DateTime.SpecifyKind(bar.TimeLeft, DateTimeKind.Utc);
        var ny = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzNy);
        var paris = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"));

        if (ny.Date != currentNyDate) ResetDay(barOpenUtc);

        // 1) Box 09:30–09:44 NY : merge MOTEUR + LIVE pour ne rater aucune mèche
        if (!boxReady)
        {
            if (IsInRange(ny, 9, 30, 0, 9, 44, 59))
            {
                UpdateBoxWith(bar.High, bar.Low);      // moteur
                TryMergeLiveMinute(barOpenUtc);        // live si dispo
            }
            if (ny.TimeOfDay >= new TimeSpan(9, 45, 0))
            {

                double w = boxHigh - boxLow;
                if (w > 0 && w <= BOX_MAX)
                {
                    boxReady = true;
                    Log($"[BOX READY] {{boxLow:F2}} → {{boxHigh:F2}} (width={{w:F2}}) @ {{ny:HH:mm}} NY");
                }
                else
                {
                    Log($"[BOX TOO WIDE] width={{w:F2}} > {{BOX_MAX:F2}} or 0 → no trade today.");
                    tradeDoneToday = true;
                }

            }
        }

        // 2) Cassure: 1ère clôture 15m hors box (09:45–11:25) + filtres (moteur + live1m)
        if (boxReady && !breakDetected && IsInRange(ny, 9, 45, 0, 11, 25, 59))
        {
            // agrégation 15m (moteur)
            var bucket15Ny = new DateTime(ny.Year, ny.Month, ny.Day, ny.Hour, (ny.Minute / 15) * 15, 0, ny.Kind);
            var openUtc15 = TimeZoneInfo.ConvertTimeToUtc(bucket15Ny, tzNy);
            var closeUtc15 = openUtc15.AddMinutes(15);

            if (!agg15.TryGetValue(openUtc15, out var a))
                a = new Ohlc { O = bar.Open, H = bar.High, L = bar.Low, C = bar.Close, OpenUtc = openUtc15, CloseUtc = closeUtc15 };
            else { a.H = Math.Max(a.H, bar.High); a.L = Math.Min(a.L, bar.Low); a.C = bar.Close; }
            agg15[openUtc15] = a;

            // tolérance de 1s pour valider la clôture
            if (!validatedBuckets.Contains(closeUtc15) && barOpenUtc >= closeUtc15.AddSeconds(-1))
            {
                validatedBuckets.Add(closeUtc15);

                string candidate = null;
                if (a.C > boxHigh + TickSize) candidate = "long";
                else if (a.C < boxLow - TickSize) candidate = "short";

                if (candidate != null && (a.O <= boxHigh && a.O >= boxLow))
                {
                    // Remplacer les coords par la 1m LIVE alignée si dispo (sinon fallback 15m moteur)
                    double oX = a.O, hX = a.H, lX = a.L, cX = a.C;
                    double oB = oX, hB = hX, lB = lX, cB = cX;
                    bool usedLive = false;

                    if (live1mAtClose.TryGetValue(closeUtc15, out var live1m))
                    {
                        usedLive = true;
                        oB = live1m.O; hB = live1m.H; lB = live1m.L; cB = live1m.C;
                    }

                    Log($"[BREAK CAND] side={candidate}  src={(usedLive ? "LIVE1m" : "ENGINE15m")}  O={oB:F2} H={hB:F2} L={lB:F2} C={cB:F2}");

                    if (PassBreakFilters(candidate, boxHigh, boxLow, oB, hB, lB, cB))
                    {
                        breakSide = candidate; breakClosePx = cX;
                        breakStartParis = ToParis(a.OpenUtc);
                        breakEndParis = ToParis(a.CloseUtc);
                        retestDeadlineParis = breakEndParis.AddMinutes(RETEST_MIN);
                        breakDetected = true;

                        // init buffer overext post-break
                        postBreakMaxHigh = double.MinValue;
                        postBreakMinLow = double.MaxValue;
                        postBreakInit = true;

                        Log($"[BREAK OK] side={breakSide}  retest until {retestDeadlineParis:HH:mm} Paris.");
                    }
                    else
                    {
                        Log("[BREAK REJECTED] filters KO → day done.");
                        tradeDoneToday = true;
                    }
                }
            }
        }

        // 3) Retest strict après break_end, avant deadline → 1 trade max
        if (boxReady && breakDetected && !tradeDoneToday)
        {
            if (paris > breakEndParis && paris < retestDeadlineParis)
            {
                // maj buffer overext
                if (postBreakInit)
                {
                    postBreakMaxHigh = (postBreakMaxHigh == double.MinValue) ? bar.High : Math.Max(postBreakMaxHigh, bar.High);
                    postBreakMinLow = (postBreakMinLow == double.MaxValue) ? bar.Low : Math.Min(postBreakMinLow, bar.Low);
                }

                bool hit = (breakSide == "long") ? (bar.Low <= boxHigh) : (bar.High >= boxLow);
                if (!hit) return;

                // Overextension (si activée)
                if (OVEREXT_MULT > 0.0)
                {
                    double boxMid = 0.5 * (boxHigh + boxLow);
                    double distMid = Math.Abs(breakClosePx - boxMid);
                    if (distMid > 0)
                    {
                        if (breakSide == "long")
                        {
                            double runup = postBreakMaxHigh - breakClosePx;
                            if (runup > OVEREXT_MULT * distMid) { tradeDoneToday = true; return; }
                        }
                        else
                        {
                            double rundown = breakClosePx - postBreakMinLow;
                            if (rundown > OVEREXT_MULT * distMid) { tradeDoneToday = true; return; }
                        }
                    }
                }

                // Entrée au bord de box
                double entry = (breakSide == "long") ? boxHigh : boxLow;
                double height = Math.Max(1e-12, boxHigh - boxLow);
                double stop = (breakSide == "long") ? (boxHigh - STOP_FRAC * height) : (boxLow + STOP_FRAC * height);
                double riskPts = Math.Abs(entry - stop); if (riskPts <= 0) { tradeDoneToday = true; return; }
                double tp = (breakSide == "long") ? (entry + TP_R * riskPts) : (entry - TP_R * riskPts);

                int stopTicks = Math.Max(1, (int)Math.Round(Math.Abs(entry - stop) / TickSize));
                int tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tp - entry) / TickSize));

                int qty = Math.Max(1, Math.Min(MaxContracts,
                    (int)Math.Floor(RiskUSD / (stopTicks * Math.Max(1e-9, TickValueUSD)))));

                var side = (breakSide == "long") ? Side.Buy : Side.Sell;
                var req = new PlaceOrderRequestParameters()
                {
                    Account = this.Account,
                    Symbol = this.Symbol,
                    Side = side,
                    Quantity = qty,
                    OrderTypeId = OrderType.Market,
                    StopLoss = SlTpHolder.CreateSL(stopTicks, PriceMeasurement.Offset),
                    TakeProfit = SlTpHolder.CreateTP(tpTicks, PriceMeasurement.Offset),
                    Comment = Tag
                };
                var res = Core.Instance.PlaceOrder(req);
                if (res.Status != TradingOperationResultStatus.Success)
                {
                    Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
                }
                else
                {
                    Log($"[TRADE] {breakSide.ToUpper()} x{qty}  Entry={entry:F2} | SL={stop:F2} ({stopTicks}t) | TP={tp:F2} ({tpTicks}t) | R={TP_R:F2}",
                        StrategyLoggingLevel.Trading);
                }

                tradeDoneToday = true;
            }
            else if (paris >= retestDeadlineParis) tradeDoneToday = true;
        }
    }

    // ===== Filtres cassure =====
    private bool PassBreakFilters(string side, double bh, double bl, double o, double h, double l, double c)
    {
        if (string.IsNullOrEmpty(side)) return false;

        double wickFrac = OutsideWickFrac(side, bh, bl, o, h, l, c);
        if (wickFrac > WICK_OUT_MAX_FRAC) return false;

        if (!BodyAndRangePass(side, bh, bl, o, h, l, c, BODY_OUTSIDE_FRAC_MIN, RANGE_VS_BOX_MIN))
            return false;

        return true;
    }

    private static double OutsideWickFrac(string side, double bh, double bl, double o, double h, double l, double c)
    {
        double rng = Math.Max(h - l, 1e-12);
        double bodyHi = Math.Max(o, c), bodyLo = Math.Min(o, c);
        double wickOut = (side == "long") ? Math.Max(0.0, h - bodyHi) : Math.Max(0.0, bodyLo - l);
        return (rng > 0) ? (wickOut / rng) : 0.0;
    }

    private static bool BodyAndRangePass(string side, double bh, double bl, double o, double h, double l, double c,
                                         double bodyMin, double rangeMin)
    {
        double bodyHi = Math.Max(o, c), bodyLo = Math.Min(o, c);
        double body = Math.Max(1e-12, bodyHi - bodyLo);
        double rng = Math.Max(1e-12, h - l);
        double boxH = Math.Max(1e-12, bh - bl);

        double bodyOut = (side == "long")
            ? (bodyHi > bh ? (bodyHi - Math.Max(bh, bodyLo)) : 0.0)
            : (bodyLo < bl ? (Math.Min(bl, bodyHi) - bodyLo) : 0.0);

        double bodyFrac = bodyOut / body;
        double rangeFrac = rng / boxH;

        return (bodyFrac >= bodyMin) && (rangeFrac >= rangeMin);
    }

    // ===== Backfill box (historique du jour) =====
    private void TryBackfillBoxFromHistory()
    {
        if (hd1m == null) return;

        var nowUtc = DateTime.UtcNow;
        var nowNy = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        var nyDate = nowNy.Date;

        double hi = double.MinValue, lo = double.MaxValue;
        int used = 0;

        for (int i = 0; i < hd1m.Count; i++)
        {
            if (hd1m[i] is not HistoryItemBar b) continue;

            DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
            var ny = TimeZoneInfo.ConvertTimeFromUtc(openUtc, tzNy);
            if (ny.Date != nyDate) continue;

            var t = ny.TimeOfDay;
            if (t >= OpenStart && t <= OpenEnd)
            {
                hi = (hi == double.MinValue) ? b.High : Math.Max(hi, b.High);
                lo = (lo == double.MaxValue) ? b.Low : Math.Min(lo, b.Low);
                used++;
            }
        }

        if (used == 0) return;

        boxHigh = hi; boxLow = lo;

        // Si on démarre entre 09:30 et 09:44 → merge LIVE déjà capturées
        if (nowNy.TimeOfDay < new TimeSpan(9, 45, 0))
        {
            for (int i = 0; i < hd1m.Count; i++)
            {
                if (hd1m[i] is not HistoryItemBar b) continue;
                DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
                var ny = TimeZoneInfo.ConvertTimeFromUtc(openUtc, tzNy);
                var t = ny.TimeOfDay;
                if (ny.Date == nowNy.Date && t >= OpenStart && t <= OpenEnd)
                    TryMergeLiveMinute(openUtc);
            }
            var wP = boxHigh - boxLow;
            Log($"[BOX BACKFILL PARTIAL] {boxLow:F2} → {boxHigh:F2} (w={wP:F2})  minutes={used}  @ start {nowNy:HH:mm} NY");
            boxReady = false;
        }
        else
        {

            var w = boxHigh - boxLow;
            if (w > 0 && w <= BOX_MAX)
            {
                boxReady = true;
                Log($"[BOX BACKFILL READY] {{boxLow:F2}} → {{boxHigh:F2}} (w={{w:F2}})  (frozen @ 09:45 NY)");
            }
            else
            {
                boxReady = false;
                tradeDoneToday = true;
                Log($"[BOX BACKFILL TOO WIDE] w={{w:F2}} > {{BOX_MAX:F2}} or 0 → no trade today.");
            }

        }
    }

    // ===== Utils =====
    private void ResetDay(DateTime nowUtc)
    {
        var ny = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        currentNyDate = ny.Date;

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxReady = false;
        agg15.Clear(); validatedBuckets.Clear();

        breakDetected = false; breakSide = null; tradeDoneToday = false;

        postBreakMaxHigh = double.MinValue; postBreakMinLow = double.MaxValue; postBreakInit = false;

        ScheduleDailyFlatten();
    }

    private static bool IsInRange(DateTime ny, int h1, int m1, int s1, int h2, int m2, int s2)
    {
        var t = ny.TimeOfDay;
        return t >= new TimeSpan(h1, m1, s1) && t <= new TimeSpan(h2, m2, s2);
    }

    private DateTime ToParis(DateTime utc) =>
        TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"));

    private void ScheduleDailyFlatten()
    {
        var nowUtc = DateTime.UtcNow;
        var nowNy = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        var targetNy = new DateTime(nowNy.Year, nowNy.Month, nowNy.Day, FlattenHour, FlattenMinute, 0);
        if (nowNy >= targetNy) targetNy = targetNy.AddDays(1);
        var targetUtc = TimeZoneInfo.ConvertTimeToUtc(targetNy, tzNy);
        var due = targetUtc - nowUtc;

        flattenTimer?.Dispose();
        flattenTimer = new System.Threading.Timer(_ =>
        {
            try
            {
                foreach (var pos in Core.Instance.Positions)
                    if (pos.Symbol == this.Symbol && pos.Account == this.Account)
                        Core.Instance.ClosePosition(pos);
                foreach (var ord in Core.Instance.Orders)
                    if (ord.Symbol == this.Symbol && ord.Account == this.Account)
                        Core.Instance.CancelOrder(ord);
            }
            finally { ScheduleDailyFlatten(); }
        }, null, due, System.Threading.Timeout.InfiniteTimeSpan);
    }

    private static TimeZoneInfo GetNyTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }

    private static string TryGet(object obj, string name)
    {
        var p = obj?.GetType().GetProperty(name);
        return p == null ? null : Convert.ToString(p.GetValue(obj));
    }
    private static long? ParseLong(string s) => long.TryParse(s, out var r) ? r : (long?)null;
    private static DateTime? ParseDate(string s) => DateTime.TryParse(s, out var d) ? (DateTime?)d : null;
}