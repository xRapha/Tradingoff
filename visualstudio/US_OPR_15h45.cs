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
    // 🚀 Déclenchement immédiat (gardé mais non utilisé pour le break)
    [InputParameter("Enter intrabar on break", 82)] public bool EnterIntrabar = true;
    [InputParameter("SL % box height", 84, 0.01, 1.00, 0.01)] public double SL_FRAC_BOX = 0.50;
    [InputParameter("Retest minutes (expiry)", 86, 1, 600, 1)] public int RETEST_MIN = 120;
    [InputParameter("Body outside % min", 88, 0, 100, 1)] public double BODY_OUTSIDE_PCT_MIN = 35.0;
    [InputParameter("Range vs box % min", 90, 0, 100, 1)] public double RANGE_VS_BOX_PCT_MIN = 30.0;
    [InputParameter("Wick outside % max", 92, 0, 100, 1)] public double WICK_OUTSIDE_PCT_MAX = 60.0;
    [InputParameter("Overextension multiple", 94, 0, 10, 0.05)] public double OVEREXT_MULT = 1.25;
    [InputParameter("BE_R (neg = off)", 96)] public double BE_R = -1.0;
    [InputParameter("Break TF minutes", 100)] public int BreakTFmin = 5;
    [InputParameter("Side mode (both/long/short)", 120)] public string SIDE_MODE = "both";

    [InputParameter("Enabled weekdays (0=Mon..6=Sun)", 130)] public string EnabledWeekdays = "0,1,2,3";
    [InputParameter("Inactive months (1..12)", 140)] public string InactiveMonths = "";

    // Filtres de taille de box
    [InputParameter("Box filter enable", 300)] public bool BoxFilterEnable = false;
    [InputParameter("Box filter mode (max/min/band)", 310)] public string BoxFilterMode = "max";
    [InputParameter("Box width max", 320)] public double BoxWidthMax = 0.0;
    [InputParameter("Box width min", 330)] public double BoxWidthMin = 0.0;
    [InputParameter("Box band min", 340)] public double BoxBandMin = 0.0;
    [InputParameter("Box band max", 350)] public double BoxBandMax = 0.0;

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

    // ====== Internes ======
    // --- OCO simulé (références locales) ---
    private Order slChild;
    private Order tpChild;
    private bool childrenPlaced;
    private HistoricalData hd1m;
    private TimeZoneInfo tzLon;
    private TimeZoneInfo tzParis;

    // Box
    private DateTime currentLonDate;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;
    private bool boxReady;
    private TimeSpan BoxStartSpan, BoxEndExact;

    // ⛔ Empêche d’entrer sur la bougie qui clôture la box
    private DateTime boxFrozenCloseUtc = DateTime.MinValue;

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
    private bool signalArmed;
    private DateTime retestExpiryUtc;
    private double retestLevel;
    private double breakClosePrice;
    private DateTime breakCloseUtc;
    private double maxPreRetestHigh;
    private double minPreRetestLow;
    private Order retestLimitOrder;
    private bool retestOrderActive;
    private int retestOrderQty;

    // LIVE tick → 1m (conservé pour la box/live log)
    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC; private long liveV;
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    private string RetestOrderComment => string.IsNullOrWhiteSpace(Tag) ? "RETEST" : (Tag + "-RETEST");

    // ====== Lifecycle ======
    protected override void OnRun()
    {
        if (Symbol == null || Account == null)
        {
            Log("Select Symbol + Account.", StrategyLoggingLevel.Error);
            return;
        }

        tzLon = GetLondonTz();
        tzParis = GetParisTz();

        BoxStartSpan = new TimeSpan(BoxStartHour, BoxStartMinute, 0);
        BoxEndExact = new TimeSpan(BoxEndHour, BoxEndMinute, 0);

        ResetDay(DateTime.UtcNow);

        // Historique depuis minuit Londres (avec marge pour reconstituer la box)
        var lonNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
        var lonMid = new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, 0, 0, 0, lonNow.Kind);
        int tfMinutes = Math.Max(1, BreakTFmin);
        int warmupMinutes = Math.Max(1, tfMinutes * 5);
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
        CancelRetestLimitOrder("strategy stop");
        Log("SBA_LondonBreak_EMA50_6E_Live stopped.");
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

        if (signalArmed)
        {
            if (side == "long")
                maxPreRetestHigh = Math.Max(maxPreRetestHigh, price);
            else
                minPreRetestLow = Math.Min(minPreRetestLow, price);

            CheckOverextensionRealtime();
        }

        // (optionnel) place ici des hooks si besoin
    }

    private void CloseCurrentLiveBar(DateTime closeUtc)
        => live1mAtClose[closeUtc] = (liveO, liveH, liveL, liveC, liveV);

    // ====== OFFICIAL 1m fetchers (silencieux) ======
    private bool TryFetchOfficial1m(DateTime openUtc, out (double O, double H, double L, double C) bar, out bool isFlat)
    {
        bar = (double.NaN, double.NaN, double.NaN, double.NaN);
        isFlat = false;

        try
        {
            var h = Symbol.GetHistory(Period.MIN1, HistoryType.Last, openUtc);
            if (h == null || h.Count == 0)
                return false;

            for (int i = 0; i < h.Count; i++)
            {
                if (h[i] is not HistoryItemBar b) continue;
                DateTime t = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
                if (t != openUtc) continue;

                double O = b.Open, H = b.High, L = b.Low, C = b.Close;
                bar = (O, H, L, C);
                isFlat = (Math.Abs(O - H) < 1e-12 &&
                          Math.Abs(H - L) < 1e-12 &&
                          Math.Abs(L - C) < 1e-12);
                return true;
            }
            return false;
        }
        catch { return false; }
    }

    // Agrège la bougie TF *courante* [openTfUtc ; closeTfUtc) depuis les 1m officielles
    private bool GetOfficialTFBarAggregated(DateTime openTfUtc, DateTime closeTfUtc,
                                            out (double O, double H, double L, double C) bar)
    {
        bar = (double.NaN, double.NaN, double.NaN, double.NaN);
        if (closeTfUtc <= openTfUtc) return false;

        const int maxRetries = 10;     // ~1.5s
        const int delayMs = 150;

        for (int k = 0; k < maxRetries; k++)
        {
            bool allOk = true;
            double O = double.NaN, H = double.MinValue, L = double.MaxValue, C = double.NaN;
            int got = 0;

            for (DateTime t = openTfUtc; t < closeTfUtc; t = t.AddMinutes(1))
            {
                if (!TryFetchOfficial1m(t, out var m1, out bool isFlat) || isFlat)
                {
                    allOk = false; break;
                }
                if (got == 0) O = m1.O;
                H = (H == double.MinValue) ? m1.H : Math.Max(H, m1.H);
                L = (L == double.MaxValue) ? m1.L : Math.Min(L, m1.L);
                C = m1.C;
                got++;
            }

            int expected = (int)Math.Round((closeTfUtc - openTfUtc).TotalMinutes);
            if (allOk && got == expected)
            {
                bar = (O, H, L, C);
                return true;
            }
            System.Threading.Thread.Sleep(delayMs);
        }
        return false;
    }

    // ====== 1m moteur ======
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
            a = new Ohlc { O = open, H = high, L = low, C = close, OpenUtc = openUtcTF, CloseUtc = closeUtcTF, Samples = 1 };
        }
        else
        {
            a.H = Math.Max(a.H, high);
            a.L = Math.Min(a.L, low);
            a.C = close;
            a.Samples++;
        }
        aggTF[openUtcTF] = a;

        // ▶️ TRAITER UNIQUEMENT LES SEAU(X) DONT LA CLÔTURE EST PASSÉE (à l'ouverture de la minute suivante)
        var readyBuckets = aggTF.Values
            .Where(v => !validatedBuckets.Contains(v.CloseUtc) && barOpenUtc >= v.CloseUtc)
            .OrderBy(v => v.CloseUtc)
            .ToList();

        foreach (var bucket in readyBuckets)
        {
            validatedBuckets.Add(bucket.CloseUtc);

            if (bucket.Samples < m)
                continue;

            if (!allowTrading || !boxReady || breakFound || dayFinished)
                continue;

            var bucketCloseLon = TimeZoneInfo.ConvertTimeFromUtc(bucket.CloseUtc, tzLon);
            var entryStart = new TimeSpan(EntryStartHour, EntryStartMinute, 0);
            var entryCut = new TimeSpan(EntryCutoffHour, EntryCutoffMinute, 0);

            // ⛔ Ignore la bougie qui clôture la box (et toute clôture avant)
            if (bucket.CloseUtc <= boxFrozenCloseUtc)
                continue;

            // ⏱️ Fenêtre d’entrée évaluée sur l'heure de CLÔTURE de la bougie TF
            if (!IsInRange(bucketCloseLon.TimeOfDay, entryStart, entryCut) || !DayMonthAllowed(bucketCloseLon))
                continue;

            // ---- Bougie TF courante (celle qui vient de se clôturer) depuis OFFICIAL 1m ----
            if (!GetOfficialTFBarAggregated(bucket.OpenUtc, bucket.CloseUtc, out var tfBar))
            {
                Log($"[SKIP] {m}m courante indisponible pour {bucket.CloseUtc:HH:mm} UTC.");
                continue;
            }

            double o = tfBar.O, h = tfBar.H, l = tfBar.L, c = tfBar.C;
            Log($"[PREV {m}m LOG] {bucket.OpenUtc:HH:mm}→{bucket.CloseUtc:HH:mm} UTC | O={o:F5} H={h:F5} L={l:F5} C={c:F5}");

            // Cassure avec cette barre TF
            string candidate = null;
            bool closeAbove = c > boxHigh;
            bool closeBelow = c < boxLow;
            if (closeAbove && SideAllowed("long")) candidate = "long";
            else if (closeBelow && SideAllowed("short")) candidate = "short";
            else continue;

            if (!PassBreakFiltersAndLog(candidate, boxHigh, boxLow, o, h, l, c,
                                       out double bodyOutsidePct, out double rangeVsBoxPct, out double wickPct))
            {
                breakFound = true;
                dayFinished = true;
                Log("[SESSION CLOSED] Break filters failed → strategy halted for the day.");
                return;
            }

            double effTickTrade = EffectiveTickSize();
            double boxHeight = Math.Max(1e-12, boxHigh - boxLow);
            double stopOffset = Math.Max(0.0, SL_FRAC_BOX * boxHeight);
            double entryEdge = (candidate == "long") ? boxHigh : boxLow;
            double rawStop = (candidate == "long") ? (entryEdge - stopOffset) : (entryEdge + stopOffset);
            double stop = RoundToTick(rawStop, effTickTrade);
            double entryPrice = entryEdge;
            double risk = Math.Abs(entryPrice - stop);
            if (risk <= 0) continue;

            side = candidate;
            entryPx = entryPrice;
            stopPx = stop;
            riskPts = risk;
            tpPx = ComputeTakeProfit(side, entryPx, riskPts);

            var cutoffLon = new DateTime(bucketCloseLon.Year, bucketCloseLon.Month, bucketCloseLon.Day, MaxTradeEndHour, MaxTradeEndMinute, 0);
            cutoffParis = ParisAt(cutoffLon);
            beArmed = (BE_R >= 0 && BE_R < TP_R);
            beActivated = false;

            breakFound = true;

            var retestExpiryLon = bucketCloseLon.AddMinutes(Math.Max(0, RETEST_MIN));
            retestExpiryUtc = TimeZoneInfo.ConvertTimeToUtc(retestExpiryLon, tzLon);
            retestLevel = entryEdge;
            breakClosePrice = c;
            breakCloseUtc = bucket.CloseUtc;
            signalArmed = true;
            maxPreRetestHigh = c;
            minPreRetestLow = c;

            string entryMode = SimulateOnly ? (UseLimitOrder ? "LIMIT" : "MARKET") : "LIMIT";
            double widthForLog = boxHeight;
            Log($"[BREAK VALID (CLOSE)] {side.ToUpper()} @ {bucketCloseLon:HH:mm:ss} LON | Entry({entryMode})={entryPx:F5} SL={stopPx:F5} TP={tpPx:F5} | bodyOut={bodyOutsidePct:F1}% (min {BODY_OUTSIDE_PCT_MIN:F1}%) | range/box={rangeVsBoxPct:F1}% (min {RANGE_VS_BOX_PCT_MIN:F1}%) | wickOut={wickPct:F1}% (max {WICK_OUTSIDE_PCT_MAX:F1}%) | boxWidth={widthForLog:F5}");
            entryForBE = entryPx;

            if (!SimulateOnly)
            {
                if (!PlaceRetestLimitOrderImmediate())
                {
                    signalArmed = false;
                    tradePlaced = false;
                    dayFinished = true;
                    Log("[ORDER FAIL] Unable to place retest LIMIT order → strategy halted for the day.", StrategyLoggingLevel.Error);
                    return;
                }
            }

            Log($"[BREAK ARMED] Retest≤{ParisAt(retestExpiryLon):HH:mm} Paris | RETEST_MIN={RETEST_MIN} | OverextMult={OVEREXT_MULT:F2}");
        }

        if (signalArmed)
        {
            if (barOpenUtc > breakCloseUtc)
            {
                if (barOpenUtc >= retestExpiryUtc)
                {
                    CancelRetestLimitOrder("expiry reached before retest");
                    Log($"[RETEST EXPIRED] No touch of {retestLevel:F5} before {TimeZoneInfo.ConvertTimeFromUtc(retestExpiryUtc, tzParis):HH:mm} Paris.");
                    signalArmed = false;
                    tradePlaced = false;
                    dayFinished = true;
                }
                else
                {
                    bool touched = false;
                    if (side == "long" && low <= retestLevel) touched = true;
                    else if (side == "short" && high >= retestLevel) touched = true;

                    if (touched)
                    {
                        if (!OverextensionOk(out double overMove, out double overLimit))
                        {
                            CancelRetestLimitOrder("overextension before retest");
                            signalArmed = false;
                            tradePlaced = false;
                            dayFinished = true;
                            Log("[SESSION CLOSED] Overextension filter failed before retest → strategy halted for the day.");
                        }
                        else if (allowTrading)
                        {
                            entryPx = retestLevel;
                            entryForBE = entryPx;
                            beActivated = false;
                            if (SimulateOnly)
                            {
                                if (PlaceInitialOrder())
                                {
                                    tradePlaced = true;
                                    signalArmed = false;
                                    Log($"[RETEST ENTRY] {side.ToUpper()} touch at {retestLevel:F5} | window until {TimeZoneInfo.ConvertTimeFromUtc(retestExpiryUtc, tzParis):HH:mm} Paris | preMove={overMove:F5} (limit {overLimit:F5})");
                                }
                                else
                                {
                                    signalArmed = false;
                                    tradePlaced = false;
                                    dayFinished = true;
                                    Log("[ORDER FAIL] Unable to place initial order → strategy halted for the day.", StrategyLoggingLevel.Error);
                                }
                            }
                            else
                            {
                                signalArmed = false;
                                tradePlaced = true;
                                childrenPlaced = true;
                                retestOrderActive = false;
                                retestLimitOrder = null;
                                Log($"[RETEST ORDER EXECUTED] LIMIT {side.ToUpper()} x{Math.Max(1, retestOrderQty)} @ {retestLevel:F5} | preMove={overMove:F5} (limit {overLimit:F5})");
                                retestOrderQty = 0;
                            }
                        }
                        else
                        {
                            signalArmed = false;
                            dayFinished = true;
                        }
                    }
                    else
                    {
                        if (side == "long")
                            maxPreRetestHigh = Math.Max(maxPreRetestHigh, high);
                        else
                            minPreRetestLow = Math.Min(minPreRetestLow, low);
                    }
                }
            }
        }

        if (!allowTrading)
            return;

        if (!tradePlaced)
            return;

        // BE / Cutoff gestion
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

        var nowParis = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc.AddMinutes(1), tzParis);
        if (nowParis >= cutoffParis)
        {
            if (SimulateOnly) Log("[SIM EXIT] Time cutoff → flat");
            else CloseAnyOpenPosition();
            tradePlaced = false;
        }
        CancelRemainingChildrenIfFlat();
    }

    private void CancelIfAlive(Order ord)
    {
        if (ord == null) return;
        try { Core.Instance.CancelOrder(ord); } catch { /* ignore */ }
    }

    private void CancelRemainingChildrenIfFlat()
    {
        if (!childrenPlaced) return;

        // Suis-je flat ?
        bool flat = true;
        foreach (var p in Core.Instance.Positions)
            if (p.Symbol == this.Symbol && p.Account == this.Account && p.Quantity != 0)
            { flat = false; break; }

        if (!flat) return;

        // plus de position => un enfant a exécuté -> on annule le jumeau restant
        try
        {
            // si on n’a pas de ref, tente de retrouver par Comment/price/symbol/account
            if (slChild == null || tpChild == null)
            {
                foreach (var o in Core.Instance.Orders)
                {
                    if (o.Symbol != this.Symbol || o.Account != this.Account) continue;
                    var c = (o.Comment ?? "");
                    if (c.EndsWith(" SL")) slChild = o;
                    if (c.EndsWith(" TP")) tpChild = o;
                }
            }
            CancelIfAlive(slChild);
            CancelIfAlive(tpChild);
        }
        catch { /* ignore */ }

        // reset état local
        slChild = null;
        tpChild = null;
        childrenPlaced = false;
        tradePlaced = false;

        Log("[OCO SIM] Flat detected → canceled remaining child order(s).", StrategyLoggingLevel.Info);
    }

    // ====== Orders (SL/TP en ABSOLU) ======
    private bool EnsureNoActiveExposure()
    {
        bool hasPosition = false;
        bool hasOrder = false;

        try
        {
            foreach (var pos in Core.Instance.Positions)
            {
                if (pos.Account == this.Account && pos.Symbol == this.Symbol && pos.Quantity != 0)
                {
                    hasPosition = true;
                    break;
                }
            }

            if (!hasPosition)
            {
                foreach (var ord in Core.Instance.Orders)
                {
                    if (ord.Account == this.Account && ord.Symbol == this.Symbol)
                    {
                        hasOrder = true;
                        break;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log("[CHECK ERROR] Impossible de vérifier les expositions actives: " + ex.Message, StrategyLoggingLevel.Error);
            dayFinished = true;
            breakFound = true;
            signalArmed = false;
            return false;
        }

        if (hasPosition || hasOrder)
        {
            Log("[ORDER BLOCKED] Active position/order detected on same account+symbol → session closed.", StrategyLoggingLevel.Info);
            dayFinished = true;
            breakFound = true;
            signalArmed = false;
            return false;
        }

        Log("[CHECK OK] Pas de soucis, la voie est libre sur account & symbole.", StrategyLoggingLevel.Info);
        return true;
    }

    private int ComputeRiskQuantity(double effTick, double stopAbs)
    {
        int stopTicksForSize = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopAbs) / effTick));
        int qtyByRisk = Math.Max(1, (int)Math.Floor(RiskCurrency / (stopTicksForSize * Math.Max(1e-9, TickValue))));
        return Math.Max(1, Math.Min(MaxContracts, qtyByRisk));
    }

    private Order FindRetestLimitOrder()
    {
        try
        {
            foreach (var ord in Core.Instance.Orders)
            {
                if (ord.Symbol != this.Symbol || ord.Account != this.Account) continue;
                var comment = ord.Comment ?? string.Empty;
                if (string.Equals(comment, RetestOrderComment, StringComparison.OrdinalIgnoreCase))
                    return ord;
            }
        }
        catch
        {
            // ignore lookup issues
        }
        return null;
    }

    private void CancelRetestLimitOrder(string reason)
    {
        try
        {
            var matches = new Dictionary<string, Order>();


            foreach (var ord in Core.Instance.Orders)
            {
                if (ord == null) continue;
                if (ord.Symbol != this.Symbol || ord.Account != this.Account) continue;
                string comment = ord.Comment ?? string.Empty;
                if (string.Equals(comment, RetestOrderComment, StringComparison.OrdinalIgnoreCase))
                {
                    try { matches[ord.Id] = ord; }
                    catch { }
                }
            }

            if (matches.Count == 0)
            {
                Log($"[RETEST ORDER CANCELED] {reason} (no matching order found)", StrategyLoggingLevel.Info);
            }
            else
            {
                foreach (var ord in matches.Values)
                {
                    try
                    {
                        Core.Instance.CancelOrder(ord);
                        Log($"[RETEST ORDER CANCELED] {reason} | id={ord.Id}", StrategyLoggingLevel.Info);
                    }
                    catch (Exception inner)
                    {
                        Log($"[RETEST ORDER CANCEL ERROR] id={ord.Id} reason={reason} error={inner.Message}", StrategyLoggingLevel.Error);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log("[RETEST ORDER CANCEL ERROR] " + ex.Message, StrategyLoggingLevel.Error);
        }

        retestLimitOrder = null;
        retestOrderActive = false;
        retestOrderQty = 0;
    }

    private bool PlaceRetestLimitOrderImmediate()
    {
        if (!EnsureNoActiveExposure())
            return false;

        double effTick = EffectiveTickSize();
        double orderPrice = RoundToTick(retestLevel, effTick);
        double stopAbs = RoundToTick(stopPx, effTick);
        double tpAbs = RoundToTick(tpPx, effTick);

        int qty = ComputeRiskQuantity(effTick, stopAbs);
        retestOrderQty = qty;

        int slTicks = Math.Max(1, (int)Math.Round(Math.Abs(orderPrice - stopAbs) / effTick));
        int tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tpAbs - orderPrice) / effTick));

        var stopHolder = SlTpHolder.CreateSL(slTicks, PriceMeasurement.Offset);
        var tpHolder = SlTpHolder.CreateTP(tpTicks, PriceMeasurement.Offset);

        var req = new PlaceOrderRequestParameters()
        {
            Account = this.Account,
            Symbol = this.Symbol,
            Side = (side == "long") ? Side.Buy : Side.Sell,
            Quantity = qty,
            OrderTypeId = OrderType.Limit,
            Comment = RetestOrderComment,
            StopLoss = stopHolder,
            TakeProfit = tpHolder,
            Price = orderPrice
        };

        var res = Core.Instance.PlaceOrder(req);
        if (res.Status != TradingOperationResultStatus.Success)
        {
            Log("Retest LIMIT order failed: " + res.Message, StrategyLoggingLevel.Error);
            retestOrderQty = 0;
            return false;
        }

        retestOrderActive = true;
        retestLimitOrder = FindRetestLimitOrder();

        double slApprox = (side == "long") ? orderPrice - slTicks * effTick : orderPrice + slTicks * effTick;
        double tpApprox = (side == "long") ? orderPrice + tpTicks * effTick : orderPrice - tpTicks * effTick;
        string expiryParis = TimeZoneInfo.ConvertTimeFromUtc(retestExpiryUtc, tzParis).ToString("HH:mm");
        Log($"[RETEST ORDER PLACED] LIMIT {side.ToUpper()} x{qty} @ {orderPrice:F5} | SL≈{slApprox:F5} TP≈{tpApprox:F5} | expires {expiryParis} Paris");
        Log("[RETEST ORDER BRACKET] OCO armed.", StrategyLoggingLevel.Info);
        return true;
    }

    private bool PlaceInitialOrder()
    {
        if (!EnsureNoActiveExposure())
            return false;

        // ⚙️ TICK EFFECTIF : on prend celui du symbole (sinon fallback sur l'input)
        double effTick = EffectiveTickSize();

        // Prix cibles calculés par ta logique (inchangé)
        double stopAbs = RoundToTick(stopPx, effTick);
        double tpAbs = RoundToTick(tpPx, effTick);

        // Taille (risk-based) identique
        int qty = ComputeRiskQuantity(effTick, stopAbs);

        var sideOrd = (side == "long") ? Side.Buy : Side.Sell;
        var orderType = UseLimitOrder ? OrderType.Limit : OrderType.Market;
        double orderPrice = RoundToTick(entryPx, effTick);

        if (SimulateOnly)
        {
            Log($"[SIM ORDER] {orderType} {side.ToUpper()} x{qty} | Entry={orderPrice:F5} SL={stopAbs:F5} TP={tpAbs:F5}");
            return true;
        }

        // ➕ Convertit les prix absolus voulus en offsets *en ticks* par rapport au fill (MARKET)
        int slTicks = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopAbs) / effTick));
        int tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tpAbs - entryPx) / effTick));

        // 🧲 Bracket en OFFSET (toujours supporté sur MARKET) → OCO auto par Quantower
        var stopHolder = SlTpHolder.CreateSL(slTicks, PriceMeasurement.Offset);
        var tpHolder = SlTpHolder.CreateTP(tpTicks, PriceMeasurement.Offset);

        var req = new PlaceOrderRequestParameters()
        {
            Account = this.Account,
            Symbol = this.Symbol,
            Side = sideOrd,
            Quantity = qty,
            OrderTypeId = orderType,
            Comment = Tag,
            StopLoss = stopHolder,       // ✅ OCO (SL)
            TakeProfit = tpHolder,         // ✅ OCO (TP)
            Price = UseLimitOrder ? orderPrice : 0.0
        };

        var res = Core.Instance.PlaceOrder(req);
        if (res.Status != TradingOperationResultStatus.Success)
        {
            Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
            return false;
        }
        else
        {
            childrenPlaced = true;
            Log($"[ORDER] {orderType} {side.ToUpper()} x{qty} @ {orderPrice:F5} | " +
                $"SL≈{stopAbs:F5} TP≈{tpAbs:F5}");
            Log("[ORDER BRACKET] OCO armed.", StrategyLoggingLevel.Info);
            return true;
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

        if (BoxFilterEnable)
        {
            double width = Math.Max(0.0, boxHigh - boxLow);
            if (!BoxWidthPassesFilter(width, out string filterMsg))
            {
                if (logReady)
                    Log($"[BOX FILTER FAIL] width={width:F5} {filterMsg} → session halted for the day.", StrategyLoggingLevel.Info);
                boxReady = true;
                dayFinished = true;
                return;
            }
            else if (logReady)
            {
                Log($"[BOX FILTER OK] width={width:F5} mode={NormalizeBoxFilterMode()}", StrategyLoggingLevel.Info);
            }
        }

        if (logReady)
            Log($"[BOX READY] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} @ {lonNow:HH:mm} LON");

        // 🔐 mémorise la clôture exacte de la box en UTC
        boxFrozenCloseUtc = TimeZoneInfo.ConvertTimeToUtc(
            new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, BoxEndHour, BoxEndMinute, 0, lonNow.Kind),
            tzLon
        );

        boxReady = true;
    }

    // ====== Réconciliation box avec les 1m officielles ======
    private void ReconcileBoxWithOfficial1m()
    {
        try
        {
            var utcStart = TimeZoneInfo.ConvertTimeToUtc(
                new DateTime(currentLonDate.Year, currentLonDate.Month, currentLonDate.Day,
                             BoxStartHour, BoxStartMinute, 0),
                tzLon);

            var utcEnd = TimeZoneInfo.ConvertTimeToUtc(
                new DateTime(currentLonDate.Year, currentLonDate.Month, currentLonDate.Day,
                             BoxEndHour, BoxEndMinute, 0),
                tzLon);

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
            double effTick = EffectiveTickSize();
            int dHiTicks = (int)Math.Round(Math.Abs(hi - boxHigh) / Math.Max(1e-12, effTick));
            int dLoTicks = (int)Math.Round(Math.Abs(lo - boxLow) / Math.Max(1e-12, effTick));

            if (dHiTicks <= tol && dLoTicks <= tol)
            {
                Log($"[BOX RECONCILE] confirmé par historique (Δhi={dHiTicks}t, Δlo={dLoTicks}t, tol={tol}t)", StrategyLoggingLevel.Info);
                return;
            }

            double oldHi = boxHigh, oldLo = boxLow;
            boxHigh = hi; boxLow = lo;

            Log($"[BOX CORRECTED] officiel hi/lo=({hi:F5}/{lo:F5}) remplace live ({oldHi:F5}/{oldLo:F5}) Δhi={dHiTicks}t Δlo={dLoTicks}t tol={tol}t",
                StrategyLoggingLevel.Info);
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
            if (dayFinished)
                return;

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
            Log($"[BOX BACKFILL PARTIAL] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} minutes={usedMinutes} @ {nowLon:HH:mm} LON", StrategyLoggingLevel.Info);
        }
        else if (!wasReady && boxReady)
        {
            Log($"[BOX BACKFILL READY] low={boxLow:F5} high={boxHigh:F5} mid={boxMid:F5} (frozen @ {BoxEndHour:00}:{BoxEndMinute:00} LON)", StrategyLoggingLevel.Info);
        }
    }

    // ====== Utils ======
    private void ResetDay(DateTime nowUtc)
    {
        var lon = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzLon);
        currentLonDate = lon.Date;

        CancelRetestLimitOrder("day reset");

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxMid = 0.0; boxReady = false;
        aggTF.Clear(); validatedBuckets.Clear();

        // reset du garde-fou box
        boxFrozenCloseUtc = DateTime.MinValue;

        breakFound = false;
        tradePlaced = false;
        beActivated = false;
        beArmed = false;
        dayFinished = false;
        signalArmed = false;
        retestExpiryUtc = DateTime.MinValue;
        retestLevel = double.NaN;
        breakClosePrice = double.NaN;
        breakCloseUtc = DateTime.MinValue;
        maxPreRetestHigh = double.MinValue;
        minPreRetestLow = double.MaxValue;
        retestLimitOrder = null;
        retestOrderActive = false;
        retestOrderQty = 0;
    }

    private static bool IsInRange(TimeSpan t, TimeSpan a, TimeSpan b) => t >= a && t <= b;

    private DateTime ParisAt(DateTime tLon)
    {
        var utc = TimeZoneInfo.ConvertTimeToUtc(tLon, tzLon);
        return TimeZoneInfo.ConvertTimeFromUtc(utc, tzParis);
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

    private static TimeZoneInfo GetParisTz()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("Europe/Paris"); }
        catch { return TimeZoneInfo.FindSystemTimeZoneById("Romance Standard Time"); }
    }

    private static string TryGet(object obj, string name)
    {
        var p = obj?.GetType().GetProperty(name);
        return p == null ? null : Convert.ToString(p.GetValue(obj));
    }
    private static long? ParseLong(string s) => long.TryParse(s, out var r) ? r : (long?)null;
    private static DateTime? ParseDate(string s) => DateTime.TryParse(s, out var d) ? (DateTime?)d : null;

    private bool PassBreakFiltersAndLog(string candidate, double bh, double bl, double o, double h, double l, double c,
                                        out double bodyOutsidePct, out double rangeVsBoxPct, out double wickPct)
    {
        double bodyHi = Math.Max(o, c);
        double bodyLo = Math.Min(o, c);
        double bodyLen = Math.Max(1e-12, bodyHi - bodyLo);
        double rangeLen = Math.Max(1e-12, h - l);
        double boxHeight = Math.Max(1e-12, bh - bl);

        double bodyOutsideLen;
        if (candidate == "long")
        {
            double clamp = Math.Max(bodyLo, bh);
            bodyOutsideLen = (bodyHi > bh) ? Math.Max(0.0, bodyHi - clamp) : 0.0;
        }
        else
        {
            bodyOutsideLen = (bodyLo < bl) ? Math.Max(0.0, Math.Min(bodyHi, bl) - bodyLo) : 0.0;
        }

        double bodyOutsideFrac = bodyOutsideLen / bodyLen;
        double rangeVsBoxFrac = rangeLen / boxHeight;
        double wickOutside = (candidate == "long") ? Math.Max(0.0, h - bodyHi) : Math.Max(0.0, bodyLo - l);
        double wickFrac = wickOutside / rangeLen;

        double bodyLimit = BODY_OUTSIDE_PCT_MIN / 100.0;
        double rangeLimit = RANGE_VS_BOX_PCT_MIN / 100.0;
        double wickLimit = WICK_OUTSIDE_PCT_MAX / 100.0;

        if (bodyOutsideFrac < bodyLimit)
        {
            Log($"[FILTER FAIL] bodyOutside={bodyOutsideFrac * 100.0:F1}% < min {BODY_OUTSIDE_PCT_MIN:F1}%");
            bodyOutsidePct = bodyOutsideFrac * 100.0;
            rangeVsBoxPct = rangeVsBoxFrac * 100.0;
            wickPct = wickFrac * 100.0;
            return false;
        }
        if (rangeVsBoxFrac < rangeLimit)
        {
            Log($"[FILTER FAIL] range/box={rangeVsBoxFrac * 100.0:F1}% < min {RANGE_VS_BOX_PCT_MIN:F1}%");
            bodyOutsidePct = bodyOutsideFrac * 100.0;
            rangeVsBoxPct = rangeVsBoxFrac * 100.0;
            wickPct = wickFrac * 100.0;
            return false;
        }
        if (wickFrac > wickLimit)
        {
            Log($"[FILTER FAIL] wick={wickFrac * 100.0:F1}% > max {WICK_OUTSIDE_PCT_MAX:F1}%");
            bodyOutsidePct = bodyOutsideFrac * 100.0;
            rangeVsBoxPct = rangeVsBoxFrac * 100.0;
            wickPct = wickFrac * 100.0;
            return false;
        }

        bodyOutsidePct = bodyOutsideFrac * 100.0;
        rangeVsBoxPct = rangeVsBoxFrac * 100.0;
        wickPct = wickFrac * 100.0;

        Log($"[FILTERS OK] bodyOutside={bodyOutsidePct:F1}% (min {BODY_OUTSIDE_PCT_MIN:F1}%) | range/box={rangeVsBoxPct:F1}% (min {RANGE_VS_BOX_PCT_MIN:F1}%) | wick={wickPct:F1}% (max {WICK_OUTSIDE_PCT_MAX:F1}%) | boxHeight={boxHeight:F5}");
        return true;
    }

    private void CheckOverextensionRealtime()
    {
        if (!signalArmed)
            return;

        if (OVEREXT_MULT <= 0 || double.IsNaN(OVEREXT_MULT) || double.IsInfinity(OVEREXT_MULT))
            return;

        double distFromMid = Math.Abs(breakClosePrice - boxMid);
        if (distFromMid <= 0)
            return;

        double move = (side == "long")
            ? Math.Max(0.0, maxPreRetestHigh - breakClosePrice)
            : Math.Max(0.0, breakClosePrice - minPreRetestLow);
        double limit = OVEREXT_MULT * distFromMid;

        if (move <= limit)
            return;

        CancelRetestLimitOrder("overextension exceeded (tick)");

        if (!SimulateOnly)
            CloseAnyOpenPosition();

        retestOrderActive = false;
        retestLimitOrder = null;
        retestOrderQty = 0;

        signalArmed = false;
        tradePlaced = false;
        dayFinished = true;

        Log($"[OVEREXT AUTO-CANCEL] runup/rundown dépassé → ordre annulé avant retest. (move={move:F5} limit={limit:F5})", StrategyLoggingLevel.Info);
    }

    private bool OverextensionOk(out double move, out double limit)
    {
        move = 0.0;
        limit = 0.0;

        if (!signalArmed)
            return true;

        if (OVEREXT_MULT <= 0 || double.IsNaN(OVEREXT_MULT) || double.IsInfinity(OVEREXT_MULT))
            return true;

        double distFromMid = Math.Abs(breakClosePrice - boxMid);
        if (distFromMid <= 0)
            return true;

        limit = OVEREXT_MULT * distFromMid;
        if (side == "long")
        {
            move = Math.Max(0.0, maxPreRetestHigh - breakClosePrice);
            if (move > limit)
            {
                Log($"[OVEREXT FAIL] runup={move:F5} > limit={limit:F5} (distMid={distFromMid:F5})");
                return false;
            }
        }
        else
        {
            move = Math.Max(0.0, breakClosePrice - minPreRetestLow);
            if (move > limit)
            {
                Log($"[OVEREXT FAIL] rundown={move:F5} > limit={limit:F5} (distMid={distFromMid:F5})");
                return false;
            }
        }

        return true;
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

    private string NormalizeBoxFilterMode()
    {
        string mode = (BoxFilterMode ?? string.Empty).Trim().ToLowerInvariant();
        return mode switch
        {
            "max" => "max",
            "min" => "min",
            "band" => "band",
            _ => "max"
        };
    }

    private bool BoxWidthPassesFilter(double width, out string message)
    {
        message = string.Empty;
        if (!BoxFilterEnable)
            return true;

        string mode = NormalizeBoxFilterMode();
        switch (mode)
        {
            case "max":
                if (width > BoxWidthMax)
                {
                    message = $"> max {BoxWidthMax:F5}";
                    return false;
                }
                return true;
            case "min":
                if (width < BoxWidthMin)
                {
                    message = $"< min {BoxWidthMin:F5}";
                    return false;
                }
                return true;
            case "band":
                if (width < BoxBandMin)
                {
                    message = $"< bandMin {BoxBandMin:F5}";
                    return false;
                }
                if (width > BoxBandMax)
                {
                    message = $"> bandMax {BoxBandMax:F5}";
                    return false;
                }
                return true;
            default:
                return true;
        }
    }

    private bool SideAllowed(string s)
    {
        string mode = (SIDE_MODE ?? "both").Trim().ToLowerInvariant();
        if (mode == "both" || string.IsNullOrEmpty(mode)) return true;
        if (mode == "long" && s == "long") return true;
        if (mode == "short" && s == "short") return true;
        return false;
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

    private double EffectiveTickSize()
        => (Symbol?.TickSize ?? 0) > 0 ? Symbol.TickSize : Math.Max(TickSize, 1e-9);

    private static double RoundToTick(double price, double tick)
        => (tick > 0) ? Math.Round(price / tick) * tick : price;

    private double ComputeTakeProfit(string candidate, double entry, double riskPts)
    {
        double effTick = EffectiveTickSize();
        double baseTarget = (candidate == "long") ? entry + TP_R * riskPts : entry - TP_R * riskPts;
        double rounded = RoundToTick(baseTarget, effTick);

        double minDistance = effTick * 0.5;
        if (effTick <= 0)
            return rounded;

        if (Math.Abs(rounded - entry) < minDistance)
        {
            double adjustment = (candidate == "long") ? effTick : -effTick;
            rounded = entry + adjustment;
        }
        return rounded;
    }
}
