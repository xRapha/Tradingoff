using System;
using System.Collections.Generic;
using System.Globalization;
using TradingPlatform.BusinessLayer;

public class OPR_15h45_OFF : Strategy
{
    // ===== Inputs =====
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    [InputParameter("Tick Size", 30, 0.0000001, 1.0, 0.0000001)] public double TickSize = 0.25;
    [InputParameter("Tick Value", 40)] public double TickValue = 0.5;
    [InputParameter("Risk Currency", 50)] public double RiskCurrency = 150.0;
    [InputParameter("Max Contracts", 60)] public int MaxContracts = 10;
    [InputParameter("Order Tag", 70)] public string Tag = "OPR-15H45";

    [InputParameter("TP in R", 80)] public double TP_R = 2.0;
    [InputParameter("Stop fraction of box (0..1)", 90, 0.0, 0.99, 0.01)] public double StopFrac = 0.50;
    [InputParameter("Break TF minutes", 100, 1, 60, 1)] public int BreakTFmin = 1;

    [InputParameter("Box Start Time NY (HH:mm)", 110)] public string BoxStartTime = "07:51";
    [InputParameter("Box End Time NY (HH:mm)", 120)] public string BoxEndTime = "07:52";

    [InputParameter("Entry Start Hour NY", 150)] public int EntryStartHour = 07;
    [InputParameter("Entry Start Minute NY", 160)] public int EntryStartMinute = 52;
    [InputParameter("Entry Cutoff Hour NY", 170)] public int EntryCutoffHour = 17;
    [InputParameter("Entry Cutoff Minute NY", 180)] public int EntryCutoffMinute = 0;

    [InputParameter("Max Trade End Hour NY", 190)] public int MaxTradeEndHour = 15;
    [InputParameter("Max Trade End Minute NY", 200)] public int MaxTradeEndMinute = 59;

    [InputParameter("Simulate only (no real orders)", 210)] public bool SimulateOnly = true;

    // ===== Internals =====
    private HistoricalData hd1m;
    private TimeZoneInfo tzNy;
    private TimeZoneInfo tzParis;

    private DateTime currentNyDate;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;
    private bool boxReady;
    private TimeSpan boxStartSpan, boxEndSpan;

    private struct Ohlc
    {
        public double O, H, L, C;
        public DateTime OpenUtc, CloseUtc;
        public int Samples;
    }
    private readonly Dictionary<DateTime, Ohlc> aggTF = new();
    private readonly HashSet<DateTime> validatedBuckets = new();

    private bool breakFound;
    private bool tradePlaced;
    private bool dayFinished;
    private string side;
    private double entryPx, stopPx, tpPx, riskPts;
    private DateTime cutoffParis;

    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC; private long liveV;
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    // ===== Lifecycle =====
    protected override void OnRun()
    {
        if (Symbol == null || Account == null)
        {
            Log("Select Symbol + Account.", StrategyLoggingLevel.Error);
            return;
        }

        tzNy = GetNyTz();
        tzParis = GetParisTz();

        UpdateBoxWindow();

        ResetDay(DateTime.UtcNow);

        var nyNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy);
        var nyMid = new DateTime(nyNow.Year, nyNow.Month, nyNow.Day, 0, 0, 0, nyNow.Kind);
        var utcFrom = TimeZoneInfo.ConvertTimeToUtc(nyMid.AddHours(-6), tzNy);

        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, utcFrom);
        Log("[INIT] HistoryType=Last 1m preload=" + (hd1m?.Count ?? 0));
        if (hd1m != null) hd1m.NewHistoryItem += OnNewBar;

        WarmupFromHistory();
        Symbol.NewLast += OnNewLast;
        TryBackfillBoxFromHistory();

        Log("OPR_15h45_OFF started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        Log("OPR_15h45_OFF stopped.");
    }

    // ===== Tick → live 1m cache =====
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

    // ===== 1m engine =====
    private void OnNewBar(object _, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar bar) return;
        ProcessMinuteBar(bar, allowTrading: true);
    }

    private void ProcessMinuteBar(HistoryItemBar bar, bool allowTrading)
    {
        DateTime barOpenUtc = DateTime.SpecifyKind(bar.TimeLeft, DateTimeKind.Utc);
        var ny = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzNy);

        if (ny.Date != currentNyDate)
            ResetDay(barOpenUtc);

        if (!boxReady)
        {
            var t = ny.TimeOfDay;
            if (t >= boxStartSpan && t < boxEndSpan)
            {
                UpdateBoxWith(bar.High, bar.Low);
                TryMergeLiveMinute(barOpenUtc);
            }
            if (t >= boxEndSpan)
            {
                FinalizeBoxIfAny(ny);
            }
        }

        int tf = Math.Max(1, BreakTFmin);
        var bucketNy = new DateTime(ny.Year, ny.Month, ny.Day, ny.Hour, (ny.Minute / tf) * tf, 0, ny.Kind);
        var openUtcTF = TimeZoneInfo.ConvertTimeToUtc(bucketNy, tzNy);
        var closeUtcTF = openUtcTF.AddMinutes(tf);

        if (!aggTF.TryGetValue(openUtcTF, out var a))
        {
            a = new Ohlc
            {
                O = bar.Open,
                H = bar.High,
                L = bar.Low,
                C = bar.Close,
                OpenUtc = openUtcTF,
                CloseUtc = closeUtcTF,
                Samples = 1
            };
        }
        else
        {
            a.H = Math.Max(a.H, bar.High);
            a.L = Math.Min(a.L, bar.Low);
            a.C = bar.Close;
            a.Samples = Math.Min(tf, a.Samples + 1);
        }
        aggTF[openUtcTF] = a;

        if (!validatedBuckets.Contains(closeUtcTF) && a.Samples >= tf && barOpenUtc >= closeUtcTF)
        {
            validatedBuckets.Add(closeUtcTF);
            ValidateBreak(a, allowTrading);
        }

        if (allowTrading && tradePlaced)
        {
            var paris = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzParis);
            if (paris >= cutoffParis)
            {
                if (SimulateOnly) Log("[SIM EXIT] Time cutoff → flat");
                else CloseAnyOpenPosition();
                tradePlaced = false;
            }
        }
    }

    private void ValidateBreak(Ohlc bucket, bool allowTrading)
    {
        if (!allowTrading || dayFinished || !boxReady || breakFound)
            return;

        var nyClose = TimeZoneInfo.ConvertTimeFromUtc(bucket.CloseUtc, tzNy);
        var entryStart = new TimeSpan(EntryStartHour, EntryStartMinute, 0);
        var entryCut = new TimeSpan(EntryCutoffHour, EntryCutoffMinute, 0);
        if (nyClose.TimeOfDay < entryStart || nyClose.TimeOfDay > entryCut)
            return;

        double o = bucket.O, h = bucket.H, l = bucket.L, c = bucket.C;
        int tf = Math.Max(1, BreakTFmin);
        if (tf == 1 && live1mAtClose.TryGetValue(bucket.CloseUtc, out var live))
        {
            o = live.O; h = live.H; l = live.L; c = live.C;
        }

        string candidate = null;
        if (c > boxHigh + TickSize) candidate = "long";
        else if (c < boxLow - TickSize) candidate = "short";
        if (candidate == null)
            return;

        side = candidate;
        entryPx = (side == "long") ? boxHigh : boxLow;
        double height = boxHigh - boxLow;
        if (height <= 0)
        {
            Log("[BREAK REJECTED] Box height invalid.");
            dayFinished = true;
            return;
        }

        double stop = ComputeStop(side, boxHigh, boxLow, StopFrac);
        double risk = Math.Abs(entryPx - stop);
        if (risk <= 0)
        {
            Log("[BREAK REJECTED] Stop distance <= 0 → day done.");
            dayFinished = true;
            return;
        }

        riskPts = risk;
        stopPx = stop;
        tpPx = (side == "long") ? entryPx + TP_R * riskPts : entryPx - TP_R * riskPts;
        cutoffParis = ToParis(new DateTime(nyClose.Year, nyClose.Month, nyClose.Day, MaxTradeEndHour, MaxTradeEndMinute, 0));
        boxMid = 0.5 * (boxHigh + boxLow);

        double boxRange = Math.Max(0.0, boxHigh - boxLow);
        Log($"[BREAK VALID] {side.ToUpper()} @ {nyClose:HH:mm} NY | close={c:F2} range={boxRange:F2} entry={entryPx:F2} stop={stopPx:F2} tp={tpPx:F2}");

        breakFound = true;
        PlaceInitialOrder();
        dayFinished = true;
    }

    private double ComputeStop(string s, double hi, double lo, double stopFrac)
    {
        double height = Math.Max(hi - lo, 0.0);
        double frac = stopFrac;
        if (frac < 0.0) frac = 0.0;
        if (frac > 0.99) frac = 0.99;
        if (height <= 0)
            return (s == "long") ? hi : lo;
        return (s == "long") ? (hi - frac * height) : (lo + frac * height);
    }

    private void PlaceInitialOrder()
    {
        double entryAbs = RoundToTick(entryPx, TickSize);
        double stopAbs = RoundToTick(stopPx, TickSize);
        double tpAbs = RoundToTick(tpPx, TickSize);

        double tick = Math.Max(TickSize, 1e-9);
        int stopTicks = Math.Max(1, (int)Math.Round(Math.Abs(entryAbs - stopAbs) / tick));
        int qtyRisk = Math.Max(1, (int)Math.Floor(RiskCurrency / (stopTicks * Math.Max(1e-9, TickValue))));
        int qty = Math.Max(1, Math.Min(MaxContracts, qtyRisk));

        if (SimulateOnly)
        {
            Log($"[SIM ORDER] LIMIT {side.ToUpper()} x{qty} entry={entryAbs:F2} SL={stopAbs:F2} TP={tpAbs:F2}");
            Log("[SIM ORDER BRACKET] Stop-loss and take-profit armed.", StrategyLoggingLevel.Info);
            tradePlaced = true;
            return;
        }

        var req = new PlaceOrderRequestParameters()
        {
            Account = this.Account,
            Symbol = this.Symbol,
            Side = (side == "long") ? Side.Buy : Side.Sell,
            Quantity = qty,
            OrderTypeId = OrderType.Limit,
            Price = entryAbs,
            Comment = Tag,
            StopLoss = SlTpHolder.CreateSL(stopAbs, PriceMeasurement.Absolute),
            TakeProfit = SlTpHolder.CreateTP(tpAbs, PriceMeasurement.Absolute)
        };

        var res = Core.Instance.PlaceOrder(req);
        if (res.Status != TradingOperationResultStatus.Success)
        {
            Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
        }
        else
        {
            Log($"[ORDER] LIMIT {side.ToUpper()} x{qty} entry={entryAbs:F2} SL={stopAbs:F2} TP={tpAbs:F2}");
            Log("[ORDER BRACKET] Stop-loss and take-profit armed.", StrategyLoggingLevel.Info);
        }
        tradePlaced = true;
    }

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
        if (boxReady) return;
        if (boxHigh == double.MinValue || boxLow == double.MaxValue)
        {
            Log($"[BOX EMPTY] aucune minute avant {nyNow:HH:mm} NY");
            boxReady = true;
            return;
        }

        boxMid = 0.5 * (boxHigh + boxLow);
        double range = Math.Max(0.0, boxHigh - boxLow);
        Log($"[BOX READY] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} range={range:F2} @ {nyNow:HH:mm} NY");
        boxReady = true;
    }

    private void WarmupFromHistory()
    {
        if (hd1m == null) return;

        var nyToday = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy).Date;
        var bars = new List<HistoryItemBar>();
        for (int i = 0; i < hd1m.Count; i++)
            if (hd1m[i] is HistoryItemBar b) bars.Add(b);

        if (bars.Count == 0) return;
        bars.Sort((a, b) => a.TimeLeft.CompareTo(b.TimeLeft));

        foreach (var b in bars)
        {
            var ny = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc), tzNy);
            if (ny.Date != nyToday) continue;
            if (ny.TimeOfDay > new TimeSpan(MaxTradeEndHour, MaxTradeEndMinute, 0)) break;
            ProcessMinuteBar(b, allowTrading: false);
        }
    }

    private void TryBackfillBoxFromHistory()
    {
        if (hd1m == null) return;

        var nyToday = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy).Date;
        double hi = double.MinValue, lo = double.MaxValue;
        int used = 0;

        var bars = new List<HistoryItemBar>();
        for (int i = 0; i < hd1m.Count; i++)
            if (hd1m[i] is HistoryItemBar b) bars.Add(b);
        bars.Sort((a, b) => a.TimeLeft.CompareTo(b.TimeLeft));

        foreach (var b in bars)
        {
            DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
            var ny = TimeZoneInfo.ConvertTimeFromUtc(openUtc, tzNy);
            if (ny.Date != nyToday) continue;
            var t = ny.TimeOfDay;
            if (t >= boxStartSpan && t < boxEndSpan)
            {
                hi = (hi == double.MinValue) ? b.High : Math.Max(hi, b.High);
                lo = (lo == double.MaxValue) ? b.Low : Math.Min(lo, b.Low);
                used++;
                TryMergeLiveMinute(openUtc);
            }
        }

        if (used > 0)
        {
            boxHigh = hi;
            boxLow = lo;
            boxMid = 0.5 * (boxHigh + boxLow);
            var nyNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy);
            if (nyNow.TimeOfDay < boxEndSpan)
            {
                double partialRange = Math.Max(0.0, boxHigh - boxLow);
                Log($"[BOX BACKFILL PARTIAL] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} range={partialRange:F2} minutes={used} @ {nyNow:HH:mm} NY");
            }
            else
            {
                boxReady = true;
                double readyRange = Math.Max(0.0, boxHigh - boxLow);
                Log($"[BOX BACKFILL READY] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} range={readyRange:F2} (freeze @ {FormatHm(boxEndSpan)} NY)");
            }
        }
    }

    private void UpdateBoxWindow()
    {
        boxStartSpan = ParseHm(BoxStartTime, "box start") ?? new TimeSpan(9, 30, 0);
        boxEndSpan = ParseHm(BoxEndTime, "box end") ?? new TimeSpan(9, 45, 0);

        if (boxEndSpan <= boxStartSpan)
        {
            var adjustedEnd = boxStartSpan + TimeSpan.FromMinutes(1);
            if (adjustedEnd >= TimeSpan.FromDays(1))
            {
                adjustedEnd = new TimeSpan(23, 59, 0);
                Log("[CONFIG] Box end ≤ start → forcing end to 23:59 NY", StrategyLoggingLevel.Warning);
            }
            else
            {
                Log($"[CONFIG] Box end ≤ start → adjusting end to {FormatHm(adjustedEnd)} NY", StrategyLoggingLevel.Warning);
            }
            boxEndSpan = adjustedEnd;
        }
    }

    private void ResetDay(DateTime nowUtc)
    {
        var ny = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        currentNyDate = ny.Date;

        UpdateBoxWindow();

        boxHigh = double.MinValue;
        boxLow = double.MaxValue;
        boxMid = 0.0;
        boxReady = false;

        aggTF.Clear();
        validatedBuckets.Clear();

        breakFound = false;
        tradePlaced = false;
        dayFinished = false;
        side = null;
        entryPx = stopPx = tpPx = riskPts = 0.0;
    }

    private DateTime ToParis(DateTime nyTime)
    {
        var utc = TimeZoneInfo.ConvertTimeToUtc(nyTime, tzNy);
        return TimeZoneInfo.ConvertTimeFromUtc(utc, tzParis);
    }

    private static TimeZoneInfo GetNyTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }
        catch { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
    }

    private static TimeZoneInfo GetParisTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"); }
        catch { return TimeZoneInfo.Utc; }
    }

    private static string TryGet(object obj, string name)
    {
        var p = obj?.GetType().GetProperty(name);
        return p == null ? null : Convert.ToString(p.GetValue(obj));
    }

    private static long? ParseLong(string s) => long.TryParse(s, out var r) ? r : (long?)null;
    private static DateTime? ParseDate(string s) => DateTime.TryParse(s, out var d) ? (DateTime?)d : null;

    private void CloseAnyOpenPosition()
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
        catch (Exception ex)
        {
            Log("CloseAnyOpenPosition error: " + ex.Message, StrategyLoggingLevel.Error);
        }
        Log("[FLATTEN] positions closed & orders canceled for " + (Symbol?.Name ?? "?"), StrategyLoggingLevel.Info);
    }

    private TimeSpan? ParseHm(string input, string label)
    {
        var trimmed = input?.Trim();
        if (TimeSpan.TryParseExact(trimmed, new[] { "hh\:mm", "h\:mm" }, CultureInfo.InvariantCulture, out var span))
            return span;

        if (TimeSpan.TryParse(trimmed, out span))
            return span;

        if (!string.IsNullOrWhiteSpace(trimmed))
            Log($"[CONFIG] {label} '{trimmed}' invalid → using default", StrategyLoggingLevel.Warning);

        return null;
    }

    private static string FormatHm(TimeSpan span) => span.ToString(@"hh\:mm");

    private static double RoundToTick(double price, double tick)
        => (tick > 0) ? Math.Round(price / tick) * tick : price;
}