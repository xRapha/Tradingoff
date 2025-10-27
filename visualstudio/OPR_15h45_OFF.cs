using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TradingPlatform.BusinessLayer;

public class OPR45_NQ_Live : Strategy
{
    // ====== Inputs ======
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    [InputParameter("Tick Size", 30, 0.0000001, 10.0, 0.0000001)] public double TickSize = 0.25;
    [InputParameter("Tick Value USD", 40)] public double TickValueUSD = 0.5;
    [InputParameter("Risk USD", 50)] public double RiskUSD = 150.0;
    [InputParameter("Max Contracts", 60)] public int MaxContracts = 10;
    [InputParameter("Order Tag", 70)] public string Tag = "OPR45-NQ";

    // Filtres cassure & money management
    [InputParameter("TP in R", 80, 0.1, 10.0, 0.1)] public double TP_R = 2.0;
    [InputParameter("SL % box height", 90, 0.01, 1.00, 0.01)] public double SL_FRAC_BOX = 0.50;
    [InputParameter("Break TF minutes", 100, 1, 120, 1)] public int BreakTFmin = 15;
    [InputParameter("Retest minutes", 110, 1, 600, 1)] public int RETEST_MIN = 120;
    [InputParameter("Body outside % min", 120, 0, 100, 1)] public double BODY_OUTSIDE_PCT_MIN = 35.0;
    [InputParameter("Range vs box % min", 130, 0, 100, 1)] public double RANGE_VS_BOX_PCT_MIN = 30.0;
    [InputParameter("Wick outside % max", 140, 0, 100, 1)] public double WICK_OUTSIDE_PCT_MAX = 60.0;
    [InputParameter("Overextension multiple", 150, 0, 10, 0.05)] public double OVEREXT_MULT = 1.25;

    [InputParameter("Use box min", 160)] public bool UseBoxMin = false;
    [InputParameter("Box min (pts)", 170, 0, 1000, 0.25)] public double BOX_MIN = 0.0;
    [InputParameter("Use box max", 180)] public bool UseBoxMax = true;
    [InputParameter("Box max (pts)", 190, 0, 1000, 0.25)] public double BOX_MAX = 240.0;

    [InputParameter("Use LIMIT order", 200)] public bool UseLimitOrder = true;
    [InputParameter("Simulate only", 210)] public bool SimulateOnly = true;

    [InputParameter("Reconcile box with history", 220)] public bool ReconcileBox = true;
    [InputParameter("Reconcile tolerance (ticks)", 230, 0, 50, 1)] public int ReconcileTolTicks = 1;

    // Fenêtres NY (OPR 09:30 → 11:25)
    private readonly TimeSpan BoxStartSpan = new(9, 30, 0);
    private readonly TimeSpan BoxEndExact = new(9, 45, 0);     // exclusif
    private readonly TimeSpan BreakStartSpan = new(9, 45, 0);
    private readonly TimeSpan BreakEndSpan = new(11, 25, 59);

    // Flatten (heure NY)
    [InputParameter("Flatten Hour NY", 300)] public int FlattenHour = 15;
    [InputParameter("Flatten Minute NY", 310)] public int FlattenMinute = 59;

    // ====== Internes ======
    private HistoricalData hd1m;
    private TimeZoneInfo tzNy;
    private TimeZoneInfo tzParis;
    private System.Threading.Timer flattenTimer;

    // Box
    private DateTime currentNyDate;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;
    private bool boxReady;

    // TF agrégé (moteur)
    private struct Ohlc { public double O, H, L, C; public DateTime OpenUtc, CloseUtc; }
    private readonly Dictionary<DateTime, Ohlc> aggTF = new();
    private readonly HashSet<DateTime> validatedBuckets = new();

    // Cassure / trade
    private bool breakFound;
    private string side;
    private double entryPx, stopPx, tpPx, riskPts;
    private DateTime breakCloseParis, retestDeadlineParis;
    private bool tradePlaced;
    private bool orderExpired;

    // Overextension buffer
    private double postBreakMaxHigh = double.MinValue, postBreakMinLow = double.MaxValue;
    private bool postBreakTracking;
    private double breakClosePx;

    // LIVE tick → 1m
    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC; private long liveV;
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    // ====== Lifecycle ======
    protected override void OnRun()
    {
        if (Symbol == null || Account == null)
        {
            Log("Select Symbol + Account.", StrategyLoggingLevel.Error);
            return;
        }

        tzNy = GetNyTz();
        tzParis = TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris");

        ResetDay(DateTime.UtcNow);

        // Historique depuis minuit NY
        var nyNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy);
        var nyMid = new DateTime(nyNow.Year, nyNow.Month, nyNow.Day, 0, 0, 0, nyNow.Kind);
        var utcFrom = TimeZoneInfo.ConvertTimeToUtc(nyMid, tzNy);

        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, utcFrom);
        Log("[INIT] HistoryType=Last 1m preload=" + (hd1m?.Count ?? 0));
        if (hd1m != null) hd1m.NewHistoryItem += OnNewBar;

        Symbol.NewLast += OnNewLast;
        TryBackfillBoxFromHistory();

        ScheduleDailyFlatten();
        Log("OPR45_NQ_Live started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        flattenTimer?.Dispose();
        Log("OPR45_NQ_Live stopped.");
    }

    // ====== Ticks → 1m LIVE cache ======
    private void OnNewLast(Symbol s, Last last)
    {
        if (s != this.Symbol) return;

        double price = last.Price;
        DateTime ts = ParseDate(TryGet(last, "ServerTime") ?? TryGet(last, "Time")) ?? DateTime.UtcNow;
        ts = DateTime.SpecifyKind(ts, DateTimeKind.Utc);

        DateTime start = new DateTime(ts.Year, ts.Month, ts.Day, ts.Hour, ts.Minute, 0, DateTimeKind.Utc);
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
        liveV += ParseLong(TryGet(last, "Volume") ?? TryGet(last, "Size") ?? TryGet(last, "Qty")) ?? 1;
    }

    private void CloseCurrentLiveBar(DateTime closeUtc)
        => live1mAtClose[closeUtc] = (liveO, liveH, liveL, liveC, liveV);

    // ====== 1m moteur ======
    private void OnNewBar(object _, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar bar) return;

        DateTime barOpenUtc = DateTime.SpecifyKind(bar.TimeLeft, DateTimeKind.Utc);
        var ny = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzNy);

        if (ny.Date != currentNyDate)
            ResetDay(barOpenUtc);

        // [1] BOX construction
        if (!boxReady)
        {
            var t = ny.TimeOfDay;
            if (t >= BoxStartSpan && t < BoxEndExact)
            {
                UpdateBoxWith(bar.High, bar.Low);
                TryMergeLiveMinute(barOpenUtc);
            }
            if (t >= BoxEndExact)
            {
                var freezeNy = new DateTime(ny.Year, ny.Month, ny.Day, BoxEndExact.Hours, BoxEndExact.Minutes, 0, ny.Kind);
                FinalizeBoxIfAny(freezeNy);
            }
        }

        // [2] Agrégation TF cassure
        int m = Math.Max(1, BreakTFmin);
        var bucketNy = new DateTime(ny.Year, ny.Month, ny.Day, ny.Hour, (ny.Minute / m) * m, 0, ny.Kind);
        var openUtcTF = TimeZoneInfo.ConvertTimeToUtc(bucketNy, tzNy);
        var closeUtcTF = openUtcTF.AddMinutes(m);

        if (!aggTF.TryGetValue(openUtcTF, out var a))
            a = new Ohlc { O = bar.Open, H = bar.High, L = bar.Low, C = bar.Close, OpenUtc = openUtcTF, CloseUtc = closeUtcTF };
        else { a.H = Math.Max(a.H, bar.High); a.L = Math.Min(a.L, bar.Low); a.C = bar.Close; }
        aggTF[openUtcTF] = a;

        foreach (var k in aggTF.Values.Where(v => !validatedBuckets.Contains(v.CloseUtc) && barOpenUtc >= v.CloseUtc).OrderBy(v => v.CloseUtc))
        {
            validatedBuckets.Add(k.CloseUtc);

            if (!boxReady || breakFound) continue;

            var kCloseNy = TimeZoneInfo.ConvertTimeFromUtc(k.CloseUtc, tzNy);
            if (kCloseNy.TimeOfDay < BreakStartSpan || kCloseNy.TimeOfDay > BreakEndSpan) continue;

            double o = k.O, h = k.H, l = k.L, c = k.C;
            bool usedLive = false;
            if (m == 1 && live1mAtClose.TryGetValue(k.CloseUtc, out var lv))
            {
                usedLive = true;
                o = lv.O; h = lv.H; l = lv.L; c = lv.C;
            }

            string candidate = null;
            if (c > boxHigh + TickSize) candidate = "long";
            else if (c < boxLow - TickSize) candidate = "short";

            if (candidate == null) continue;
            if (!(k.O <= boxHigh && k.O >= boxLow)) continue;

            if (!PassBreakFilters(candidate, boxHigh, boxLow, o, h, l, c))
            {
                Log($"[BREAK REJECTED] filters KO ({candidate}) src={(usedLive ? "LIVE1m" : "ENGINE")}");
                breakFound = true;
                orderExpired = true;
                return;
            }

            Log($"[BREAK OK] {candidate.ToUpper()} src={(usedLive ? "LIVE1m" : "ENGINE")} O={o:F2} H={h:F2} L={l:F2} C={c:F2}");
            PrepareTrade(candidate, k, o, h, l, c);
        }

        // [3] Suivi post-break (overextension + expiration ordre)
        if (breakFound && tradePlaced && !orderExpired)
        {
            if (postBreakTracking)
            {
                postBreakMaxHigh = (postBreakMaxHigh == double.MinValue) ? bar.High : Math.Max(postBreakMaxHigh, bar.High);
                postBreakMinLow = (postBreakMinLow == double.MaxValue) ? bar.Low : Math.Min(postBreakMinLow, bar.Low);

                if (OVEREXT_MULT > 0)
                {
                    double boxMidLocal = 0.5 * (boxHigh + boxLow);
                    double distMid = Math.Abs(breakClosePx - boxMidLocal);
                    if (distMid > 0)
                    {
                        if (side == "long")
                        {
                            double runup = postBreakMaxHigh - breakClosePx;
                            if (runup > OVEREXT_MULT * distMid)
                            {
                                CancelPendingOrder("Overextension long");
                                return;
                            }
                        }
                        else
                        {
                            double rundown = breakClosePx - postBreakMinLow;
                            if (rundown > OVEREXT_MULT * distMid)
                            {
                                CancelPendingOrder("Overextension short");
                                return;
                            }
                        }
                    }
                }
            }

            var nowParis = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzParis);
            if (nowParis >= retestDeadlineParis)
            {
                CancelPendingOrder("Retest window expired");
            }
        }
    }

    private void PrepareTrade(string candidate, Ohlc bucket, double o, double h, double l, double c)
    {
        breakFound = true;
        side = candidate;

        double height = Math.Max(1e-12, boxHigh - boxLow);
        double stopOffset = Math.Max(0.0, SL_FRAC_BOX * height);

        entryPx = (side == "long") ? boxHigh : boxLow;
        stopPx = (side == "long") ? entryPx - stopOffset : entryPx + stopOffset;
        riskPts = Math.Abs(entryPx - stopPx);
        if (riskPts <= 0)
        {
            Log("[BREAK CANCEL] risk=0", StrategyLoggingLevel.Info);
            orderExpired = true;
            return;
        }
        tpPx = (side == "long") ? entryPx + TP_R * riskPts : entryPx - TP_R * riskPts;

        breakClosePx = c;
        breakCloseParis = TimeZoneInfo.ConvertTimeFromUtc(bucket.CloseUtc, tzParis);
        retestDeadlineParis = breakCloseParis.AddMinutes(RETEST_MIN);

        postBreakMaxHigh = h;
        postBreakMinLow = l;
        postBreakTracking = true;

        PlaceInitialOrder();
    }

    private void PlaceInitialOrder()
    {
        int stopTicks = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopPx) / Math.Max(1e-12, TickSize)));
        int qtyByRisk = Math.Max(1, (int)Math.Floor(RiskUSD / (stopTicks * Math.Max(1e-9, TickValueUSD))));
        int qty = Math.Max(1, Math.Min(MaxContracts, qtyByRisk));

        if (SimulateOnly)
        {
            Log($"[SIM ORDER] {(UseLimitOrder ? "LIMIT" : "MARKET")} {side.ToUpper()} x{qty} Entry={entryPx:F2} SL={stopPx:F2} TP={tpPx:F2} deadline={retestDeadlineParis:HH:mm}");
            tradePlaced = true;
            orderExpired = false;
            return;
        }

        var req = new PlaceOrderRequestParameters()
        {
            Account = this.Account,
            Symbol = this.Symbol,
            Side = (side == "long") ? Side.Buy : Side.Sell,
            Quantity = qty,
            OrderTypeId = UseLimitOrder ? OrderType.Limit : OrderType.Market,
            Comment = Tag,
            StopLoss = SlTpHolder.CreateSL(RoundToTick(stopPx, TickSize), PriceMeasurement.Absolute),
            TakeProfit = SlTpHolder.CreateTP(RoundToTick(tpPx, TickSize), PriceMeasurement.Absolute),
            Price = UseLimitOrder ? RoundToTick(entryPx, TickSize) : 0.0
        };

        var res = Core.Instance.PlaceOrder(req);
        if (res.Status != TradingOperationResultStatus.Success)
        {
            Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
            orderExpired = true;
            return;
        }

        tradePlaced = true;
        orderExpired = false;
        Log($"[ORDER] {(UseLimitOrder ? "LIMIT" : "MARKET")} {side.ToUpper()} x{qty} Entry={entryPx:F2} SL={stopPx:F2} TP={tpPx:F2} deadline={retestDeadlineParis:HH:mm}");
    }

    private void CancelPendingOrder(string reason)
    {
        if (!tradePlaced || orderExpired) return;

        if (SimulateOnly)
        {
            Log($"[SIM CANCEL] {reason}");
        }
        else
        {
            try
            {
                foreach (var ord in Core.Instance.Orders.ToList())
                {
                    if (ord.Symbol == this.Symbol && ord.Account == this.Account)
                    {
                        if (string.IsNullOrEmpty(Tag) || string.Equals(ord.Comment, Tag, StringComparison.Ordinal))
                            CancelOrderCompat(ord);
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Cancel order error: " + UnwrapExceptionMessage(ex), StrategyLoggingLevel.Error);
            }
            Log($"[ORDER CANCEL] {reason}", StrategyLoggingLevel.Info);
        }

        orderExpired = true;
        tradePlaced = false;
        postBreakTracking = false;
    }

    // ====== Box helpers ======
    private void UpdateBoxWith(double hi, double lo)
    {
        if (double.IsNaN(hi) || double.IsNaN(lo)) return;
        if (boxHigh == double.MinValue) { boxHigh = hi; boxLow = lo; }
        else { boxHigh = Math.Max(boxHigh, hi); boxLow = Math.Min(boxLow, lo); }
    }

    private void TryMergeLiveMinute(DateTime openUtc)
    {
        var closeUtc = openUtc.AddMinutes(1);
        if (live1mAtClose.TryGetValue(closeUtc, out var lv))
            UpdateBoxWith(lv.H, lv.L);
    }

    private void FinalizeBoxIfAny(DateTime nyNow)
    {
        if (boxHigh == double.MinValue || boxLow == double.MaxValue)
        {
            Log($"[BOX EMPTY] aucune minute enregistrée avant {nyNow:HH:mm} NY");
            boxReady = true;
            return;
        }

        if (ReconcileBox) ReconcileBoxWithOfficial1m();

        double width = boxHigh - boxLow;
        boxMid = 0.5 * (boxHigh + boxLow);

        if (UseBoxMin && width < BOX_MIN)
        {
            Log($"[BOX FAIL] width={width:F2} < min={BOX_MIN:F2}");
            boxReady = false;
            orderExpired = true;
            breakFound = true;
            return;
        }

        if (UseBoxMax && width > BOX_MAX)
        {
            Log($"[BOX FAIL] width={width:F2} > max={BOX_MAX:F2}");
            boxReady = false;
            orderExpired = true;
            breakFound = true;
            return;
        }

        Log($"[BOX READY] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} @ {nyNow:HH:mm} NY (width={width:F2})");
        boxReady = true;
    }

    private void ReconcileBoxWithOfficial1m()
    {
        try
        {
            var utcStart = TimeZoneInfo.ConvertTimeToUtc(new DateTime(currentNyDate.Year, currentNyDate.Month, currentNyDate.Day, BoxStartSpan.Hours, BoxStartSpan.Minutes, 0), tzNy);
            var utcEnd = TimeZoneInfo.ConvertTimeToUtc(new DateTime(currentNyDate.Year, currentNyDate.Month, currentNyDate.Day, BoxEndExact.Hours, BoxEndExact.Minutes, 0), tzNy);

            double hi = double.MinValue, lo = double.MaxValue; int used = 0;
            if (hd1m != null)
            {
                for (int i = 0; i < hd1m.Count; i++)
                {
                    if (hd1m[i] is not HistoryItemBar b) continue;
                    DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
                    if (openUtc < utcStart || openUtc >= utcEnd) continue;
                    hi = (hi == double.MinValue) ? b.High : Math.Max(hi, b.High);
                    lo = (lo == double.MaxValue) ? b.Low : Math.Min(lo, b.Low);
                    used++;
                }
            }

            if (used == 0)
            {
                Log("[BOX RECONCILE] aucun historique 1m disponible", StrategyLoggingLevel.Info);
                return;
            }

            int tol = Math.Max(0, ReconcileTolTicks);
            int dHiTicks = (int)Math.Round(Math.Abs(hi - boxHigh) / Math.Max(1e-12, TickSize));
            int dLoTicks = (int)Math.Round(Math.Abs(lo - boxLow) / Math.Max(1e-12, TickSize));

            if (dHiTicks <= tol && dLoTicks <= tol)
            {
                Log($"[BOX RECONCILE] confirmé (Δhi={dHiTicks}t Δlo={dLoTicks}t tol={tol}t)", StrategyLoggingLevel.Info);
                return;
            }

            double oldHi = boxHigh, oldLo = boxLow;
            boxHigh = hi; boxLow = lo;
            Log($"[BOX CORRECTED] officiel=({hi:F2}/{lo:F2}) remplace live=({oldHi:F2}/{oldLo:F2}) Δhi={dHiTicks}t Δlo={dLoTicks}t", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[BOX RECONCILE] erreur: " + ex.Message, StrategyLoggingLevel.Error);
        }
    }

    // ====== Filtres cassure ======
    private bool PassBreakFilters(string candidate, double bh, double bl, double o, double h, double l, double c)
    {
        double boxHeight = Math.Max(1e-12, bh - bl);
        double bodyHi = Math.Max(o, c);
        double bodyLo = Math.Min(o, c);
        double body = Math.Max(1e-12, bodyHi - bodyLo);
        double range = Math.Max(1e-12, h - l);

        double bodyOutside = (candidate == "long")
            ? Math.Max(0.0, bodyHi - Math.Max(bh, bodyLo))
            : Math.Max(0.0, Math.Min(bl, bodyHi) - bodyLo);

        double wickOutside = (candidate == "long") ? Math.Max(0.0, h - bodyHi) : Math.Max(0.0, bodyLo - l);

        double bodyOutsidePct = (body > 0) ? (100.0 * bodyOutside / body) : 0.0;
        double rangeVsBoxPct = (boxHeight > 0) ? (100.0 * range / boxHeight) : 0.0;
        double wickPct = (range > 0) ? (100.0 * wickOutside / range) : 0.0;

        if (bodyOutsidePct < BODY_OUTSIDE_PCT_MIN)
        {
            Log($"[FILTER] bodyOutside {bodyOutsidePct:F1}% < {BODY_OUTSIDE_PCT_MIN:F1}%");
            return false;
        }
        if (rangeVsBoxPct < RANGE_VS_BOX_PCT_MIN)
        {
            Log($"[FILTER] range/box {rangeVsBoxPct:F1}% < {RANGE_VS_BOX_PCT_MIN:F1}%");
            return false;
        }
        if (wickPct > WICK_OUTSIDE_PCT_MAX)
        {
            Log($"[FILTER] wick {wickPct:F1}% > {WICK_OUTSIDE_PCT_MAX:F1}%");
            return false;
        }

        return true;
    }

    // ====== Backfill box ======
    private void TryBackfillBoxFromHistory()
    {
        if (hd1m == null) return;

        var nowNy = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy);
        var nyDate = nowNy.Date;

        double hi = double.MinValue, lo = double.MaxValue; int used = 0;

        for (int i = 0; i < hd1m.Count; i++)
        {
            if (hd1m[i] is not HistoryItemBar b) continue;

            DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
            var ny = TimeZoneInfo.ConvertTimeFromUtc(openUtc, tzNy);
            if (ny.Date != nyDate) continue;

            var tb = ny.TimeOfDay;
            if (tb >= BoxStartSpan && tb < BoxEndExact)
            {
                hi = (hi == double.MinValue) ? b.High : Math.Max(hi, b.High);
                lo = (lo == double.MaxValue) ? b.Low : Math.Min(lo, b.Low);
                used++;
                TryMergeLiveMinute(openUtc);
            }
        }

        if (used > 0)
        {
            boxHigh = hi; boxLow = lo; boxMid = 0.5 * (boxHigh + boxLow);

            if (nowNy.TimeOfDay < BoxEndExact)
            {
                Log($"[BOX BACKFILL PARTIAL] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} minutes={used} @ {nowNy:HH:mm} NY");
            }
            else
            {
                FinalizeBoxIfAny(new DateTime(nowNy.Year, nowNy.Month, nowNy.Day, BoxEndExact.Hours, BoxEndExact.Minutes, 0, nowNy.Kind));
            }
        }
    }

    // ====== Utils ======
    private void ResetDay(DateTime nowUtc)
    {
        var ny = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        currentNyDate = ny.Date;

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxMid = 0.0; boxReady = false;
        aggTF.Clear(); validatedBuckets.Clear();

        breakFound = false; tradePlaced = false; orderExpired = false;
        postBreakTracking = false; postBreakMaxHigh = double.MinValue; postBreakMinLow = double.MaxValue;
    }

    private static readonly MethodInfo cancelOrderNew = typeof(Core).GetMethod("CancelOrder", new[] { typeof(Order) });
    private static readonly MethodInfo cancelOrderLegacy = typeof(Core).GetMethod("CancelOrder", new[] { typeof(Order), typeof(string) });

    private void CancelOrderCompat(Order ord)
    {
        if (ord == null) return;

        if (cancelOrderNew != null)
        {
            cancelOrderNew.Invoke(Core.Instance, new object[] { ord });
            return;
        }

        if (cancelOrderLegacy != null)
        {
            cancelOrderLegacy.Invoke(Core.Instance, new object[] { ord, null });
        }
    }

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
                foreach (var pos in Core.Instance.Positions.ToList())
                    if (pos.Symbol == this.Symbol && pos.Account == this.Account)
                        Core.Instance.ClosePosition(pos);
                foreach (var ord in Core.Instance.Orders.ToList())
                    if (ord.Symbol == this.Symbol && ord.Account == this.Account)
                        CancelOrderCompat(ord);
            }
            catch (Exception ex)
            {
                Log("Flatten error: " + UnwrapExceptionMessage(ex), StrategyLoggingLevel.Error);
            }
            finally { ScheduleDailyFlatten(); }
        }, null, due, System.Threading.Timeout.InfiniteTimeSpan);
    }

    private static string UnwrapExceptionMessage(Exception ex)
    {
        if (ex is TargetInvocationException tie && tie.InnerException != null)
            return tie.InnerException.Message;
        return ex?.Message;
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

    private static double RoundToTick(double price, double tick)
        => (tick > 0) ? Math.Round(price / tick) * tick : price;
}