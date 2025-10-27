using System;
using System.Linq;
using System.Collections.Generic;
using TradingPlatform.BusinessLayer;

public class SBA_LondonBreak_EMA50_6E_Live : Strategy
{
    // ====== Inputs ======
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    [InputParameter("Tick Size", 30, 0.0000001, 1.0, 0.0000001)] public double TickSize = 0.00005;
    [InputParameter("Tick Value", 40)] public double TickValue = 6.25;
    [InputParameter("Risk Currency", 50)] public double RiskCurrency = 100.0;
    [InputParameter("Max Contracts", 60)] public int MaxContracts = 10;
    [InputParameter("Order Tag", 70)] public string Tag = "SBA-LON-6E";

    [InputParameter("TP in R", 80)] public double TP_R = 1.0;
    [InputParameter("BE_R (neg = off)", 90)] public double BE_R = -1.0;
    [InputParameter("Break TF minutes", 100)] public int BreakTFmin = 5;

    [InputParameter("Wick% max", 110)] public double WICK_MAX = 30.0;
    [InputParameter("Side mode (both/long/short)", 120)] public string SIDE_MODE = "both";

    [InputParameter("Enabled weekdays (0=Mon..6=Sun)", 130)] public string EnabledWeekdays = "0,1,2,3";
    [InputParameter("Inactive months (1..12)", 140)] public string InactiveMonths = "";

    // Box Londres (FIN EXCLUSIVE à HH:MM:00)
    [InputParameter("Box Start Hour LON", 150)] public int BoxStartHour = 8;
    [InputParameter("Box Start Minute LON", 160)] public int BoxStartMinute = 0;
    [InputParameter("Box End Hour LON", 170)] public int BoxEndHour = 9;
    [InputParameter("Box End Minute LON", 180)] public int BoxEndMinute = 45;

    // Fenêtre d’entrée Londres
    [InputParameter("Entry Start Hour LON", 190)] public int EntryStartHour = 9;
    [InputParameter("Entry Start Minute LON", 200)] public int EntryStartMinute = 45;
    [InputParameter("Entry Cutoff Hour LON", 210)] public int EntryCutoffHour = 13;
    [InputParameter("Entry Cutoff Minute LON", 220)] public int EntryCutoffMinute = 0;

    // Fin max gestion (Londres)
    [InputParameter("Max Trade End Hour LON", 230)] public int MaxTradeEndHour = 14;
    [InputParameter("Max Trade End Minute LON", 240)] public int MaxTradeEndMinute = 30;

    [InputParameter("Use LIMIT order (else MARKET)", 250)] public bool UseLimitOrder = true;
    [InputParameter("Simulate only (no real orders)", 260)] public bool SimulateOnly = true;

    // Réconciliation box
    [InputParameter("Reconcile box with history", 270)] public bool ReconcileBox = true;
    [InputParameter("Reconcile tolerance (ticks)", 280, 0, 50, 1)] public int ReconcileTolTicks = 1;

    // 🚀 Déclenchement immédiat
    [InputParameter("Enter intrabar on break", 290)] public bool EnterIntrabar = true;

    // ====== Internes ======
    private HistoricalData hd1m;
    private TimeZoneInfo tzLon;

    // Box
    private DateTime currentLonDate;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;
    private bool boxReady;
    private TimeSpan BoxStartSpan, BoxEndExact;

    // EMA50
    private const double EMA_P = 50.0;
    private readonly double alpha = 2.0 / (EMA_P + 1.0);
    private double ema; private bool emaInit;

    // TF aggregation
    private struct Ohlc
    {
        public double O, H, L, C;
        public DateTime OpenUtc, CloseUtc;
        public int Samples;
    }
    private readonly Dictionary<DateTime, Ohlc> aggTF = new();
    private readonly HashSet<DateTime> validatedBuckets = new();

    // Cassure / trade
    private bool breakFound;
    private string side;
    private double entryPx, stopPx, tpPx, riskPts;
    private bool tradePlaced, beArmed, beActivated;
    private double entryForBE;
    private DateTime cutoffParis;
    private bool dayFinished;

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

        tzLon = GetLondonTz();

        BoxStartSpan = new TimeSpan(BoxStartHour, BoxStartMinute, 0);
        BoxEndExact = new TimeSpan(BoxEndHour, BoxEndMinute, 0);

        ResetDay(DateTime.UtcNow, resetEma: true);

        // Historique depuis minuit Londres (avec marge pour chauffer l'EMA)
        var lonNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
        var lonMid = new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, 0, 0, 0, lonNow.Kind);
        int tfMinutes = Math.Max(1, BreakTFmin);
        int warmupMinutes = (int)((EMA_P + 5) * tfMinutes);
        var lonWarmStart = lonMid.AddMinutes(-warmupMinutes);
        var utcFrom = TimeZoneInfo.ConvertTimeToUtc(lonWarmStart, tzLon);

        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, utcFrom);
        Log("[INIT] HistoryType=Last 1m preload=" + (hd1m?.Count ?? 0));
        if (hd1m != null) hd1m.NewHistoryItem += OnNewBar;

        Symbol.NewLast += OnNewLast;       // ticks
        TryBackfillBoxFromHistory();

        Log("SBA_LondonBreak_EMA50_6E_Live started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        Log("SBA_LondonBreak_EMA50_6E_Live stopped.");
    }

    // ====== Ticks → 1m LIVE cache ======
    private void OnNewLast(Symbol s, Last last)
    {
        if (s != this.Symbol) return;

        // 1) Construire 1m live
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

        // Déclenchement intrabar désactivé : les validations se font désormais uniquement à la clôture officielle
    }

    private void CloseCurrentLiveBar(DateTime closeUtc)
        => live1mAtClose[closeUtc] = (liveO, liveH, liveL, liveC, liveV);

    // ====== 1m moteur (backfill, EMA, validation close si intrabar désactivé) ======
    private void OnNewBar(object _, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar bar) return;

        DateTime barOpenUtc = DateTime.SpecifyKind(bar.TimeLeft, DateTimeKind.Utc);
        ProcessMinuteBar(barOpenUtc, bar.Open, bar.High, bar.Low, bar.Close, allowTrading: true, fromHistory: false);
    }

    private void ProcessMinuteBar(DateTime barOpenUtc, double open, double high, double low, double close, bool allowTrading, bool fromHistory)
    {
        var lon = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzLon);

        if (lon.Date != currentLonDate)
            ResetDay(barOpenUtc);

        if (!boxReady)
        {
            var t = lon.TimeOfDay;
            if (t >= BoxStartSpan && t < BoxEndExact)
            {
                UpdateBoxWith(high, low);
                if (!fromHistory)
                    TryMergeLiveMinute(barOpenUtc);
            }
            if (t >= BoxEndExact)
            {
                var freezeLon = new DateTime(lon.Year, lon.Month, lon.Day, BoxEndHour, BoxEndMinute, 0, lon.Kind);
                FinalizeBoxIfAny(freezeLon, logReady: !fromHistory);
            }
        }

        int m = Math.Max(1, BreakTFmin);
        var bucketLon = new DateTime(lon.Year, lon.Month, lon.Day, lon.Hour, (lon.Minute / m) * m, 0, lon.Kind);
        var openUtcTF = TimeZoneInfo.ConvertTimeToUtc(bucketLon, tzLon);
        var closeUtcTF = openUtcTF.AddMinutes(m);

        if (!aggTF.TryGetValue(openUtcTF, out var a))
        {
            a = new Ohlc
            {
                O = open,
                H = high,
                L = low,
                C = close,
                OpenUtc = openUtcTF,
                CloseUtc = closeUtcTF,
                Samples = 1
            };
        }
        else
        {
            a.H = Math.Max(a.H, high);
            a.L = Math.Min(a.L, low);
            a.C = close;
            a.Samples++;
        }
        aggTF[openUtcTF] = a;

        var barCloseUtc = barOpenUtc.AddMinutes(1);

        var readyBuckets = aggTF.Values
            .Where(v => !validatedBuckets.Contains(v.CloseUtc) && barCloseUtc >= v.CloseUtc)
            .OrderBy(v => v.CloseUtc)
            .ToList();

        foreach (var bucket in readyBuckets)
        {
            validatedBuckets.Add(bucket.CloseUtc);

            if (bucket.Samples < m)
                continue;
            UpdateEma(bucket.C);

            if (!allowTrading || !boxReady || breakFound || dayFinished)
                continue;

            var bucketCloseLon = TimeZoneInfo.ConvertTimeFromUtc(bucket.CloseUtc, tzLon);
            var entryStart = new TimeSpan(EntryStartHour, EntryStartMinute, 0);
            var entryCut = new TimeSpan(EntryCutoffHour, EntryCutoffMinute, 0);

            if (!IsInRange(bucketCloseLon.TimeOfDay, entryStart, entryCut) || !DayMonthAllowed(bucketCloseLon))
                continue;

            double o = bucket.O, h = bucket.H, l = bucket.L, c = bucket.C;
            if (m == 1 && live1mAtClose.TryGetValue(bucket.CloseUtc, out var lv))
            {
                o = lv.O; h = lv.H; l = lv.L; c = lv.C;
            }

            string candidate = null;
            bool closeAbove = c > boxHigh + TickSize;
            bool closeBelow = c < boxLow - TickSize;
            if (closeAbove && SideAllowed("long")) candidate = "long";
            else if (closeBelow && SideAllowed("short")) candidate = "short";

            if (candidate == null || !WickOk(candidate, o, h, l, c))
                continue;

            if (!emaInit)
                continue;

            double entry = (candidate == "long") ? boxHigh : boxLow;
            if (!EmaSupportsSide(candidate, entry))
            {
                dayFinished = true;
                breakFound = true;
                Log($"[BREAK ABORTED] EMA50={ema:F5} incompatible with entry at {entry:F5} → no trade, session closed.");
                continue;
            }

            double stop = RoundToTick(ema, TickSize);
            double risk = (candidate == "long") ? (entry - stop) : (stop - entry);
            if (risk <= 0)
                continue;

            side = candidate;
            entryPx = entry;
            stopPx = stop;
            riskPts = Math.Abs(entry - stop);
            tpPx = ComputeTakeProfit(side, entry, riskPts);

            var cutoffLon = new DateTime(bucketCloseLon.Year, bucketCloseLon.Month, bucketCloseLon.Day, MaxTradeEndHour, MaxTradeEndMinute, 0);
            cutoffParis = ParisAt(cutoffLon);
            beArmed = (BE_R >= 0 && BE_R < TP_R);
            beActivated = false;
            entryForBE = entryPx;

            breakFound = true;
            Log($"[BREAK VALID (CLOSE)] {side.ToUpper()} @ {bucketCloseLon:HH:mm:ss} LON | Entry={entryPx:F5} SL(EMA50)={stopPx:F5} TP={tpPx:F5}");
            PlaceInitialOrder();
            tradePlaced = true;
        }

        if (!allowTrading || !tradePlaced)
            return;

        if (beArmed && !beActivated)
        {
            double favorable = (side == "long") ? (high - entryPx) : (entryPx - low);
            if (favorable >= BE_R * riskPts) beActivated = true;
        }
        if (beActivated)
        {
            bool back = (side == "long") ? (low <= entryForBE) : (high >= entryForBE);
            if (back)
            {
                if (SimulateOnly) Log("[SIM EXIT] BE hit → flat");
                else CloseAnyOpenPosition();
                tradePlaced = false;
                return;
            }
        }

        var nowParis = TimeZoneInfo.ConvertTimeFromUtc(barCloseUtc, TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"));
        if (nowParis >= cutoffParis)
        {
            if (SimulateOnly) Log("[SIM EXIT] Time cutoff → flat");
            else CloseAnyOpenPosition();
            tradePlaced = false;
        }
    }

    // ====== Orders (SL/TP en ABSOLU) ======
    private void PlaceInitialOrder()
    {
        // SL/TP absolus (prix) pour coller exactement à EMA50 et au TP calculé
        double stopAbs = RoundToTick(stopPx, TickSize);
        double tpAbs = RoundToTick(tpPx, TickSize);

        // Taille
        int stopTicks = 1;
        if (TickSize > 0)
            stopTicks = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopAbs) / TickSize));
        int qtyByRisk = Math.Max(1, (int)Math.Floor(RiskCurrency / (stopTicks * Math.Max(1e-9, TickValue))));
        int qty = Math.Max(1, Math.Min(MaxContracts, qtyByRisk));

        int tpTicks = 0;
        if (TickSize > 0)
            tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tpAbs - entryPx) / TickSize));

        var sideOrd = (side == "long") ? Side.Buy : Side.Sell;

        if (SimulateOnly)
        {
            Log($"[SIM ORDER] {(UseLimitOrder ? "LIMIT" : "MARKET")} {side.ToUpper()} x{qty} | Entry={entryPx:F5} SL={stopAbs:F5} TP={tpAbs:F5}");
            return;
        }

        var stopHolder = (TickSize > 0)
            ? SlTpHolder.CreateSL(stopTicks, PriceMeasurement.Offset)
            : SlTpHolder.CreateSL(stopAbs, PriceMeasurement.Absolute);
        var tpHolder = (TickSize > 0 && tpTicks > 0)
            ? SlTpHolder.CreateTP(tpTicks, PriceMeasurement.Offset)
            : SlTpHolder.CreateTP(tpAbs, PriceMeasurement.Absolute);

        var req = new PlaceOrderRequestParameters()
        {
            Account = this.Account,
            Symbol = this.Symbol,
            Side = sideOrd,
            Quantity = qty,
            OrderTypeId = UseLimitOrder ? OrderType.Limit : OrderType.Market,
            Comment = Tag,
            StopLoss = stopHolder,
            TakeProfit = tpHolder,
            Price = UseLimitOrder ? entryPx : 0.0
        };

        var res = Core.Instance.PlaceOrder(req);
        if (res.Status != TradingOperationResultStatus.Success)
            Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
        else
        {
            Log($"[ORDER] {(UseLimitOrder ? "LIMIT" : "MARKET")} {side.ToUpper()} x{qty} @ {entryPx:F5} | SL={stopAbs:F5} TP={tpAbs:F5}");
            Log($"[ORDER BRACKET] OCO armed.", StrategyLoggingLevel.Info);
        }
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

    private void FinalizeBoxIfAny(DateTime lonNow, bool logReady = true)
    {
        if (boxHigh == double.MinValue || boxLow == double.MaxValue)
        {
            if (logReady)
                Log($"[BOX EMPTY] aucune minute enregistrée avant {lonNow:HH:mm} LON");
            boxReady = true;
            return;
        }

        if (ReconcileBox) ReconcileBoxWithOfficial1m();

        boxMid = 0.5 * (boxHigh + boxLow);
        if (logReady)
            Log($"[BOX READY] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} EMA50={FormatEmaForLog()} @ {lonNow:HH:mm} LON");
        boxReady = true;
    }

    private void ReconcileBoxWithOfficial1m()
    {
        try
        {
            var utcStart = TimeZoneInfo.ConvertTimeToUtc(new DateTime(currentLonDate.Year, currentLonDate.Month, currentLonDate.Day, BoxStartHour, BoxStartMinute, 0), tzLon);
            var utcEnd = TimeZoneInfo.ConvertTimeToUtc(new DateTime(currentLonDate.Year, currentLonDate.Month, currentLonDate.Day, BoxEndHour, BoxEndMinute, 0), tzLon);

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
                Log("[BOX RECONCILE] aucun historique 1m disponible pour la fenêtre de box", StrategyLoggingLevel.Info);
                return;
            }

            int tol = Math.Max(0, ReconcileTolTicks);
            int dHiTicks = (int)Math.Round(Math.Abs(hi - boxHigh) / Math.Max(1e-12, TickSize));
            int dLoTicks = (int)Math.Round(Math.Abs(lo - boxLow) / Math.Max(1e-12, TickSize));

            if (dHiTicks <= tol && dLoTicks <= tol)
            {
                Log($"[BOX RECONCILE] confirmé par historique (Δhi={dHiTicks}t, Δlo={dLoTicks}t, tol={tol}t)", StrategyLoggingLevel.Info);
                return;
            }

            double oldHi = boxHigh, oldLo = boxLow;
            boxHigh = hi; boxLow = lo;

            Log($"[BOX CORRECTED] officiel hi/lo=({hi:F5}/{lo:F5}) remplace live ({oldHi:F5}/{oldLo:F5}) Δhi={dHiTicks}t Δlo={dLoTicks}t tol={tol}t", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[BOX RECONCILE] erreur: " + ex.Message, StrategyLoggingLevel.Error);
        }
    }

    // ====== Backfill box ======
    private void TryBackfillBoxFromHistory()
    {
        if (hd1m == null) return;

        var nowLon = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
        var lonDate = nowLon.Date;
        bool wasReady = boxReady;
        int usedMinutes = 0;

        var ordered = new List<(DateTime OpenUtc, double Open, double High, double Low, double Close)>();
        for (int i = 0; i < hd1m.Count; i++)
        {
            if (hd1m[i] is not HistoryItemBar b) continue;

            DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
            if (openUtc >= DateTime.UtcNow) continue;

            ordered.Add((openUtc, b.Open, b.High, b.Low, b.Close));
        }

        if (ordered.Count == 0)
            return;

        ordered.Sort((a, b) => a.OpenUtc.CompareTo(b.OpenUtc));

        foreach (var bar in ordered)
        {
            ProcessMinuteBar(bar.OpenUtc, bar.Open, bar.High, bar.Low, bar.Close, allowTrading: false, fromHistory: true);

            var lonBar = TimeZoneInfo.ConvertTimeFromUtc(bar.OpenUtc, tzLon);
            if (lonBar.Date == lonDate)
            {
                var tb = lonBar.TimeOfDay;
                if (tb >= BoxStartSpan && tb < BoxEndExact)
                    usedMinutes++;
            }
        }

        if (usedMinutes == 0)
            return;

        if (!boxReady && boxHigh != double.MinValue && boxLow != double.MaxValue)
        {
            boxMid = 0.5 * (boxHigh + boxLow);
            Log($"[BOX BACKFILL PARTIAL] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} EMA50={FormatEmaForLog()} minutes={usedMinutes} @ {nowLon:HH:mm} LON", StrategyLoggingLevel.Info);
        }
        else if (!wasReady && boxReady)
        {
            Log($"[BOX BACKFILL READY] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} EMA50={FormatEmaForLog()} (frozen @ {BoxEndHour:00}:{BoxEndMinute:00} LON)", StrategyLoggingLevel.Info);
        }
    }

    // ====== Utils ======
    private void ResetDay(DateTime nowUtc, bool resetEma = false)
    {
        var lon = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzLon);
        currentLonDate = lon.Date;

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxMid = 0.0; boxReady = false;
        aggTF.Clear(); validatedBuckets.Clear();
        if (resetEma)
        {
            emaInit = false;
            ema = 0.0;
        }
        breakFound = false; tradePlaced = false; beActivated = false; beArmed = false; dayFinished = false;
    }

    private static bool IsInRange(TimeSpan t, TimeSpan a, TimeSpan b) => t >= a && t <= b;

    private DateTime ParisAt(DateTime tLon)
    {
        var utc = TimeZoneInfo.ConvertTimeToUtc(tLon, tzLon);
        return TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"));
    }

    private static HashSet<int> ParseSet(string csv)
    {
        var set = new HashSet<int>();
        if (string.IsNullOrWhiteSpace(csv)) return set;
        foreach (var p in csv.Split(',')) if (int.TryParse(p.Trim(), out int v)) set.Add(v);
        return set;
    }

    private static TimeZoneInfo GetLondonTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("Europe/London"); }
        catch { return TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time"); }
    }

    private static string TryGet(object obj, string name)
    {
        var p = obj?.GetType().GetProperty(name);
        return p == null ? null : Convert.ToString(p.GetValue(obj));
    }
    private static long? ParseLong(string s) => long.TryParse(s, out var r) ? r : (long?)null;
    private static DateTime? ParseDate(string s) => DateTime.TryParse(s, out var d) ? (DateTime?)d : null;

    private bool WickOk(string s, double o, double h, double l, double c)
    {
        if (WICK_MAX >= 100.0) return true;
        double rng = Math.Max(h - l, 1e-9);
        double wick = (s == "long") ? Math.Max(0.0, h - c) : Math.Max(0.0, c - l);
        double pct = (rng > 0) ? (100.0 * wick / rng) : 0.0;
        return pct <= WICK_MAX;
    }

    private bool DayMonthAllowed(DateTime lon)
    {
        var okDays = ParseSet(EnabledWeekdays);
        int wdPy = ((int)lon.DayOfWeek + 6) % 7; // 0=Lun..6=Dim
        if (!okDays.Contains(wdPy)) return false;

        var badMonths = ParseSet(InactiveMonths);
        if (badMonths.Contains(lon.Month)) return false;

        return true;
    }

    private bool SideAllowed(string s)
    {
        string mode = (SIDE_MODE ?? "both").Trim().ToLowerInvariant();
        if (mode == "both" || string.IsNullOrEmpty(mode)) return true;
        if (mode == "long" && s == "long") return true;
        if (mode == "short" && s == "short") return true;
        return false;
    }

    private void UpdateEma(double close)
    {
        if (!emaInit) { ema = close; emaInit = true; }
        else ema = alpha * close + (1 - alpha) * ema;
    }

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

    private static double RoundToTick(double price, double tick)
        => (tick > 0) ? Math.Round(price / tick) * tick : price;

    private string FormatEmaForLog()
        => emaInit ? ema.ToString("F5") : "N/A";

    private double ComputeTakeProfit(string candidate, double entry, double riskPts)
    {
        double baseTarget = (candidate == "long") ? entry + TP_R * riskPts : entry - TP_R * riskPts;
        double rounded = RoundToTick(baseTarget, TickSize);

        double minDistance = TickSize * 0.5;
        if (TickSize <= 0)
            return rounded;

        if (Math.Abs(rounded - entry) < minDistance)
        {
            double adjustment = (candidate == "long") ? TickSize : -TickSize;
            rounded = entry + adjustment;
        }

        return rounded;
    }

    private bool EmaSupportsSide(string candidate, double entry)
    {
        if (!emaInit)
            return false;

        double tolerance = TickSize * 0.1;
        if (candidate == "long")
            return ema <= entry - tolerance;
        if (candidate == "short")
            return ema >= entry + tolerance;
        return false;
    }
}