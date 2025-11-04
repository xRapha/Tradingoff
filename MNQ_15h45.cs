using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TradingPlatform.BusinessLayer;

public class SBA_LondonBreak_EMA50_6E_Live : Strategy
{
    // ====== Inputs ======
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    [InputParameter("Tick Size", 30, 0.0000001, 1.0, 0.0000001)] public double TickSize = 0.25;
    [InputParameter("Tick Value", 40)] public double TickValue = 0.5;

    // === RISK INPUTS (AUTO + FALLBACK) ===
    [InputParameter("Auto risk from account", 55)]
    public bool AutoRiskFromAccount = true;

    [InputParameter("Auto risk % (0..1)", 56, 0, 1, 0.01)]
    public double AutoRiskPct = 0.10;

    [InputParameter("Max risk per trade (USD)", 57, 0, 100000, 10)]
    public double MaxRiskCurrency = 200;

    [InputParameter("Manual risk (USD, fallback)", 58, 0, 100000, 10)]
    public double ManualRiskCurrency = 100;

    [InputParameter("Min account balance override (USD, -1 = auto)", 59)]
    public double MinAccBalOverride = -1;


    // === Helpers pour lire Balance et trouver "Min account balance" ===
    private double ReadAccDouble(string name)
    {
        if (Account == null) return double.NaN;
        try
        {
            var val = TryGet(Account, name);
            return double.TryParse(val, out var v) ? v : double.NaN;
        }
        catch { return double.NaN; }
    }
    // === Trouve "Min account balance" même si le nom de propriété diffère ===
    // === Trouve "Min account balance" même si le nom de propriété diffère (UNE SEULE COPIE !) ===
    // ========= DUMP & LECTURE DU "MIN ACCOUNT BALANCE" =========

    // Logge tous les AdditionalInfo de l'account pour repérer l'étiquette exacte.
    private void DumpAccountAdditionalInfo()
    {
        try
        {
            if (Account?.AdditionalInfo == null || Account.AdditionalInfo.Count == 0)
            {
                Log("[ACC] Aucun AdditionalInfo sur cet account.");
                return;
            }

            Log("========== ACCOUNT.AdditionalInfo ==========");
            foreach (var it in Account.AdditionalInfo)
            {
                string k = it?.NameKey ?? "?";
                string v = it?.Value?.ToString() ?? "?";
                string grp = it?.GroupInfo ?? "";
                Log($"[ACC-ADD] {k} = {v}  (group={grp})");
            }
            Log("============================================");
        }
        catch (System.Exception ex)
        {
            Log("[ACC] Dump AdditionalInfo error: " + ex.Message, StrategyLoggingLevel.Error);
        }
    }

    // Lecture robuste du "Min account balance" depuis AdditionalInfo.
    // Renvoie double.NaN si introuvable.
    private double ReadMinAccountBalance()
    {
        // 1) Candidats usuels (varie selon le broker/panel)
        string[] candidates =
        {
        "Min account balance",
        "Min Account Balance",
        "Minimum account balance",
        "Min margin balance",
        "Minimal account balance",
        "Min balance"
    };

        // 2) Essaye un accès direct par identifiant (NameKey)
        try
        {
            if (Account?.AdditionalInfo != null)
            {
                foreach (var key in candidates)
                {
                    if (Account.AdditionalInfo.TryGetItem(key, out var item))
                    {
                        if (double.TryParse(item.Value?.ToString(), out var v))
                            return v;
                    }
                }

                // 3) Sinon, scanne tous les items et cherche un libellé approchant
                foreach (var it in Account.AdditionalInfo)
                {
                    string name = (it?.NameKey ?? "").ToLowerInvariant();
                    if (name.Contains("min") && name.Contains("account") && name.Contains("balance"))
                    {
                        if (double.TryParse(it.Value?.ToString(), out var v))
                            return v;
                    }
                }
            }
        }
        catch { /* ignore */ }

        // 4) Introuvable → laisse le fallback (override manuel) agir plus haut
        return double.NaN;
    }


    // === 10% de (Balance - MinAccBal), plafonné à 200$, fallback manuel si besoin (UNE SEULE COPIE !) ===
    private double EffectiveRiskCurrency()
    {
        string src = "fallback";
        // 1) pas d’auto → manuel plafonné
        if (!AutoRiskFromAccount)
        {
            double r = Math.Min(MaxRiskCurrency, Math.Max(0.0, ManualRiskCurrency));
            Log($"[RISK] Source={src} (AutoRiskFromAccount=False) → {r:F2}$");
            return r;
        }

        double bal = ReadAccDouble("Balance");
        double minBal = double.NaN;

        // 2) override ?
        if (MinAccBalOverride >= 0)
        {
            minBal = MinAccBalOverride;
            src = "override";
        }
        else
        {
            // 3) essai via AdditionalInfo
            try
            {
                double autoMin = ReadMinAccountBalance(); // ta méthode qui scanne AdditionalInfo
                if (!double.IsNaN(autoMin))
                {
                    minBal = autoMin;
                    src = "additionalInfo";
                }
            }
            catch { }
        }

        if (double.IsNaN(bal) || double.IsNaN(minBal))
        {
            double r = Math.Min(MaxRiskCurrency, Math.Max(0.0, ManualRiskCurrency));
            Log($"[RISK] Source={src} (Balance/Min introuvable) → fallback {r:F2}$");
            return r;
        }

        double free = Math.Max(0.0, bal - minBal);
        double risk = AutoRiskPct * free;
        if (risk > MaxRiskCurrency) risk = MaxRiskCurrency;

        Log($"[RISK] Source={src} | Risk calculé: {risk:F2}$ (Balance {bal:F2} – Min {minBal:F2}, {AutoRiskPct:P0} du free).");
        return risk;
    }



    [InputParameter("Max Contracts", 60)] public int MaxContracts = 10;
    [InputParameter("Order Tag", 70)] public string Tag = "SBA-LON-6E";

    [InputParameter("TP in R", 80)] public double TP_R = 2.0;
    [InputParameter("Enter intrabar on break", 82)] public bool EnterIntrabar = true;
    [InputParameter("SL % box height", 84, 0.01, 1.00, 0.01)] public double SL_FRAC_BOX = 0.20;
    [InputParameter("Retest minutes (expiry)", 86, 1, 600, 1)] public int RETEST_MIN = 120;
    [InputParameter("Body outside % min", 88, 0, 100, 1)] public double BODY_OUTSIDE_PCT_MIN = 35.0;
    [InputParameter("Range vs box % min", 90, 0, 100, 1)] public double RANGE_VS_BOX_PCT_MIN = 20.0;
    [InputParameter("Wick outside % max", 92, 0, 100, 1)] public double WICK_OUTSIDE_PCT_MAX = 70.0;
    [InputParameter("Overextension multiple", 94, 0, 10, 0.05)] public double OVEREXT_MULT = 1.5;
    [InputParameter("BE_R (neg = off)", 96)] public double BE_R = -1.0;
    [InputParameter("Break TF minutes", 100)] public int BreakTFmin = 15;
    [InputParameter("Side mode (both/long/short)", 120)] public string SIDE_MODE = "both";

    [InputParameter("Enabled weekdays (0=Mon..6=Sun)", 130)] public string EnabledWeekdays = "0,1,2,3,4";
    [InputParameter("Inactive months (1..12)", 140)] public string InactiveMonths = "";

    // Filtres de taille de box
    [InputParameter("Box filter enable", 300)] public bool BoxFilterEnable = true;
    [InputParameter("Box filter mode (max/min/band)", 310)] public string BoxFilterMode = "band";
    [InputParameter("Box width max", 320)] public double BoxWidthMax = 0.0;
    [InputParameter("Box width min", 330)] public double BoxWidthMin = 0.0;
    [InputParameter("Box band min", 340)] public double BoxBandMin = 25.0;
    [InputParameter("Box band max", 350)] public double BoxBandMax = 240.0;

    // Box Londres (FIN EXCLUSIVE à HH:MM:00)
    [InputParameter("Box Start Hour LON", 150)] public int BoxStartHour = 14;
    [InputParameter("Box Start Minute LON", 160)] public int BoxStartMinute = 30;
    [InputParameter("Box End Hour LON", 170)] public int BoxEndHour = 14;
    [InputParameter("Box End Minute LON", 180)] public int BoxEndMinute = 45;

    // Fenêtre d’entrée Londres
    [InputParameter("Entry Start Hour LON", 190)] public int EntryStartHour = 14;
    [InputParameter("Entry Start Minute LON", 200)] public int EntryStartMinute = 45;
    [InputParameter("Entry Cutoff Hour LON", 210)] public int EntryCutoffHour = 16;
    [InputParameter("Entry Cutoff Minute LON", 220)] public int EntryCutoffMinute = 29;

    // Fin max gestion (Londres)
    [InputParameter("Max Trade End Hour LON", 230)] public int MaxTradeEndHour = 21;
    [InputParameter("Max Trade End Minute LON", 240)] public int MaxTradeEndMinute = 44;

    [InputParameter("Use LIMIT order (else MARKET)", 250)] public bool UseLimitOrder = true;
    [InputParameter("Simulate only (no real orders)", 260)] public bool SimulateOnly = false;

    // Réconciliation box
    [InputParameter("Reconcile box with history", 270)] public bool ReconcileBox = true;
    [InputParameter("Reconcile tolerance (ticks)", 280, 0, 50, 1)] public int ReconcileTolTicks = 0;

    // ====== Internes ======
    private Order slChild;
    private Order tpChild;
    private bool childrenPlaced;
    private HistoricalData hd1m;
    private TimeZoneInfo tzLon;
    private TimeZoneInfo tzParis;
    private bool lastTfBarWasPartial = false;

    // Box
    private DateTime currentLonDate;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;
    private bool boxReady;
    private TimeSpan BoxStartSpan, BoxEndExact;
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
    private bool maxTradeEndDone;   // évite de retrigger après l’heure
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
    private string retestOrderId;
    private long retestOrderUid = -1;   // UniqueId natif de l’ordre LIMIT de retest

    private DateTime nextLookupUtc = DateTime.MinValue;
    private const int RetestLookupSeconds = 2;

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

        // Log du risque calculé au démarrage
        double bal0 = ReadAccDouble("Balance");
        double minBal0 = ReadAccDouble("MinAccountBalance");
        double effRisk0 = EffectiveRiskCurrency();
        Log($"🧾 [INIT] Balance={bal0:F2} | MinAccBal={minBal0:F2} | Risk final={effRisk0:F2}$ | Auto={AutoRiskFromAccount} ({AutoRiskPct:P0}) | Max={MaxRiskCurrency:F0}$ | Fallback={ManualRiskCurrency:F0}$");

        tzLon = GetLondonTz();
        tzParis = GetParisTz();

        BoxStartSpan = new TimeSpan(BoxStartHour, BoxStartMinute, 0);
        BoxEndExact = new TimeSpan(BoxEndHour, BoxEndMinute, 0);

        ResetDay(DateTime.UtcNow);

        var lonNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
        var lonMid = new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, 0, 0, 0, lonNow.Kind);
        int tfMinutes = Math.Max(1, BreakTFmin);
        int warmupMinutes = Math.Max(1, tfMinutes * 5);
        var lonWarmStart = lonMid.AddMinutes(-warmupMinutes);
        var utcFrom = TimeZoneInfo.ConvertTimeToUtc(lonWarmStart, tzLon);

        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, utcFrom);
        Log("[INIT] HistoryType=Last 1m preload=" + (hd1m?.Count ?? 0));
        if (hd1m != null) hd1m.NewHistoryItem += OnNewBar;

        Symbol.NewLast += OnNewLast;
        TryBackfillBoxFromHistory();

        Log("SBA_LondonBreak_EMA50_6E_Live started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        //CancelRetestLimitOrder("strategy stop");
        Log("SBA_LondonBreak_EMA50_6E_Live stopped.");
    }

    // ====== Ticks → 1m LIVE cache ======
    private void OnNewLast(Symbol s, Last last)
    {
        if (s != this.Symbol) return;

        if (signalArmed && retestOrderActive && string.IsNullOrEmpty(retestOrderId) && DateTime.UtcNow >= nextLookupUtc)
            TryResolveRetestOrderReference();

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
    }

    private void CloseCurrentLiveBar(DateTime closeUtc)
        => live1mAtClose[closeUtc] = (liveO, liveH, liveL, liveC, liveV);

    // ====== OFFICIAL 1m fetchers ======
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

    private bool GetOfficialTFBarAggregated(DateTime openTfUtc, DateTime closeTfUtc,
                                            out (double O, double H, double L, double C) bar)
    {
        bar = (double.NaN, double.NaN, double.NaN, double.NaN);
        lastTfBarWasPartial = false;
        if (closeTfUtc <= openTfUtc) return false;

        const int maxRetries = 10;
        const int delayMs = 150;

        for (int k = 0; k < maxRetries; k++)
        {
            double O = double.NaN, H = double.MinValue, L = double.MaxValue, C = double.NaN;
            int got = 0;

            for (DateTime t = openTfUtc; t < closeTfUtc; t = t.AddMinutes(1))
            {
                if (TryFetchOfficial1m(t, out var m1, out _))
                {
                    if (got == 0) { O = m1.O; H = m1.H; L = m1.L; C = m1.C; }
                    else { H = Math.Max(H, m1.H); L = Math.Min(L, m1.L); C = m1.C; }
                    got++;
                }
            }

            int expected = (int)Math.Round((closeTfUtc - openTfUtc).TotalMinutes);
            if (got >= 1)
            {
                bar = (O, H, L, C);
                lastTfBarWasPartial = (got < expected);
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
        var bucketLon = new DateTime(lon.Year, lon.Month, lon.Day).AddHours(lon.Hour).AddMinutes((lon.Minute / m) * m);
        var openUtcTF = TimeZoneInfo.ConvertTimeToUtc(bucketLon, tzLon);
        var closeUtcTF = openUtcTF.AddMinutes(m);

        if (!aggTF.TryGetValue(openUtcTF, out var a))
            a = new Ohlc { O = open, H = high, L = low, C = close, OpenUtc = openUtcTF, CloseUtc = closeUtcTF, Samples = 1 };
        else
        {
            a.H = Math.Max(a.H, high);
            a.L = Math.Min(a.L, low);
            a.C = close;
            a.Samples++;
        }
        aggTF[openUtcTF] = a;

        var readyBuckets = aggTF.Values
            .Where(v => !validatedBuckets.Contains(v.CloseUtc) && barOpenUtc >= v.CloseUtc)
            .OrderBy(v => v.CloseUtc)
            .ToList();

        foreach (var bucket in readyBuckets)
        {
            validatedBuckets.Add(bucket.CloseUtc);

            if (bucket.Samples < m)
            {
                Log($"[WARN] {m}m bucket incomplet {bucket.OpenUtc:HH:mm}->{bucket.CloseUtc:HH:mm} " +
                    $"({bucket.Samples}/{m} min) — tentative via officiel.");
                // Ne pas faire 'continue' ici : on laisse GetOfficialTFBarAggregated() tenter la reconstitution
            }


            if (!allowTrading || !boxReady || breakFound || dayFinished)
                continue;

            var bucketCloseLon = TimeZoneInfo.ConvertTimeFromUtc(bucket.CloseUtc, tzLon);
            var entryStart = new TimeSpan(EntryStartHour, EntryStartMinute, 0);
            var entryCut = new TimeSpan(EntryCutoffHour, EntryCutoffMinute, 0);

            if (bucket.CloseUtc <= boxFrozenCloseUtc)
                continue;

            // 🕒 Vérification horaire et jour/mois actifs
            if (!IsInRange(bucketCloseLon.TimeOfDay, entryStart, entryCut))
            {
                Log($"[FILTER] Heure non autorisée — {bucketCloseLon:HH:mm} LON hors fenêtre {EntryStartHour:00}:{EntryStartMinute:00}–{EntryCutoffHour:00}:{EntryCutoffMinute:00}.", StrategyLoggingLevel.Info);
                continue;
            }

            if (!DayMonthAllowed(bucketCloseLon))
            {
                Log($"[FILTER] Jour ou mois inactif — {bucketCloseLon:dddd dd MMM} non autorisé → journée stoppée.", StrategyLoggingLevel.Info);
                dayFinished = true;
                breakFound = true;
                return;
            }


            if (!GetOfficialTFBarAggregated(bucket.OpenUtc, bucket.CloseUtc, out var tfBar))
            {
                Log($"[SKIP] {m}m courante indisponible pour {bucket.CloseUtc:HH:mm} UTC.");
                continue;
            }

            double o = tfBar.O, h = tfBar.H, l = tfBar.L, c = tfBar.C;

            string prefix = lastTfBarWasPartial ? "[PREV 5m LOG SPECIAL]" : $"[PREV {m}m LOG]";
            Log($"{prefix} {bucket.OpenUtc:HH:mm}→{bucket.CloseUtc:HH:mm} UTC | O={o:F5} H={h:F5} L={l:F5} C={c:F5}");
            // Cassure avec cette barre TF
            string candidate = null;
            bool closeAbove = c > boxHigh;
            bool closeBelow = c < boxLow;

            // === Détection de cassure & validation du side autorisé ===
            string breakout = null;
            if (closeAbove) breakout = "long";
            else if (closeBelow) breakout = "short";

            // 1️⃣ Aucune cassure → on continue (close dans la box)
            if (breakout == null)
                continue;

            // 2️⃣ Cassure dans un sens interdit → log + journée stoppée
            if (!SideAllowed(breakout))
            {
                Log($"[FILTER] Signal {breakout.ToUpper()} détecté mais interdit par SIDE_MODE='{SIDE_MODE}' → journée stoppée.",
                    StrategyLoggingLevel.Info);
                dayFinished = true;
                breakFound = true;
                return;
            }

            // 3️⃣ Cassure autorisée → on valide le candidat
            candidate = breakout;



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
                                retestOrderId = null;
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

        // === MAX TRADE END : CANCEL (limit/stop) PUIS FLATTEN EN MARKET (UNE SEULE FOIS, À L’HEURE EXACTE) ===
        {
            // heure actuelle Paris (horloge système, pas la bougie)
            var nowParis = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzParis);

            // cut du jour courant (LON) re-projeté à Paris — recalculé à chaque minute
            var lonNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
            var cutoffParisNow = TimeZoneInfo.ConvertTimeFromUtc(
                TimeZoneInfo.ConvertTimeToUtc(new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, MaxTradeEndHour, MaxTradeEndMinute, 0), tzLon),
                tzParis
            );

            if (!maxTradeEndDone && nowParis >= cutoffParisNow)
            {
                if (SimulateOnly)
                {
                    Log("[SIM EXIT] MaxTradeEnd → CANCEL ALL (limit/stop) + FLATTEN MARKET (acc+sym)");
                }
                else
                {
                    // 1) annule d’abord tous les LIMIT/STOP (même account+symbol)
                    CancelNonMarketOrdersForThisAccSym("max trade end pre");

                    // 2) FLATTEN en MARKET la position résiduelle (même account+symbol)
                    FlattenAccSymViaMarket("MAX_TRADE_END");

                    // 3) attend brièvement que la position tombe à 0
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    while (!IsFlatAccSym_Tolerant() && sw.ElapsedMilliseconds < 2500)
                        System.Threading.Thread.Sleep(50);

                    // 4) ménage final si flat
                    if (IsFlatAccSym_Tolerant())
                        CancelAllOrdersForThisAccSym("max trade end post", 2000);
                    else
                        Log("[WARN] MaxTradeEnd: position encore ouverte après ~2.5s → pas de cancel-all immédiat.");
                }

                // reset interne puis one-shot
                signalArmed = false;
                retestOrderActive = false;
                tradePlaced = false;
                dayFinished = true;
                maxTradeEndDone = true;
                return;
            }
        }

    }

    // ---------- Helpers tolérants + SIGNÉS (Account + Symbol) ----------
    private static string _N(string s) => (s ?? "").Trim().ToUpperInvariant();

    private bool AccountMatches(object posAccount, Account acc)
    {
        var a = _N(posAccount?.ToString());
        var b = _N(acc?.ToString());
        return a == b || a.StartsWith(b) || b.StartsWith(a);
    }

    private object GetPosSymbolObj(Position p)
    {
        if (p == null) return null;
        var t = p.GetType();
        var prop = t.GetProperty("Symbol") ?? t.GetProperty("Symbole");
        return prop?.GetValue(p);
    }

    private string SymbolNameOf(object symObj)
    {
        if (symObj == null) return "";
        var t = symObj.GetType();
        var nameProp = t.GetProperty("Name");
        if (nameProp != null)
        {
            var n = nameProp.GetValue(symObj) as string;
            if (!string.IsNullOrEmpty(n)) return n;
        }
        return symObj.ToString();
    }

    private string SymbolRoot(string name)
    {
        name = _N(name);
        int i = 0;
        while (i < name.Length && name[i] >= 'A' && name[i] <= 'Z') i++;
        return (i > 0) ? name.Substring(0, i) : name;
    }

    private bool SymbolMatches(object posSymbolObj, Symbol sym)
    {
        var a = _N(SymbolNameOf(posSymbolObj));            // ex: MNQZ5
        var b = _N(sym?.Name ?? sym?.ToString());          // ex: MNQ
        if (a == b) return true;
        if (a.StartsWith(b) || b.StartsWith(a)) return true;
        var ra = SymbolRoot(a);
        var rb = SymbolRoot(b);
        return ra == rb && ra.Length >= 2;
    }

    // >>> NOUVEAU : quantité SIGNÉE d'une Position (SHORT négatif, LONG positif)
    private double SignedQty(Position p)
    {
        // quantité absolue
        double abs = Math.Abs(p.Quantity);
        try
        {
            var sideProp = p.GetType().GetProperty("Side")
                        ?? p.GetType().GetProperty("PositionSide")
                        ?? p.GetType().GetProperty("Direction");
            if (sideProp != null)
            {
                var sideVal = sideProp.GetValue(p);
                var s = sideVal?.ToString()?.ToUpperInvariant();
                if (s != null)
                {
                    if (s.Contains("SELL") || s.Contains("SHORT"))
                        return -abs;           // SHORT -> négatif
                    if (s.Contains("BUY") || s.Contains("LONG"))
                        return +abs;           // LONG  -> positif
                }
            }
        }
        catch { /* ignore */ }

        // fallback : si la plateforme met déjà le signe dans Quantity, on le respecte
        return p.Quantity >= 0 ? +abs : -abs;
    }

    // Somme nette SIGNÉE (tolérante) pour ce couple account+symbol
    private double NetQty_AccSym_Tolerant()
    {
        double q = 0;
        foreach (var p in Core.Instance.Positions)
        {
            if (p == null) continue;
            if (AccountMatches(p.Account, this.Account) && SymbolMatches(GetPosSymbolObj(p), this.Symbol))
                q += SignedQty(p);
        }
        return q;
    }

    private bool IsFlatAccSym_Tolerant() => Math.Abs(NetQty_AccSym_Tolerant()) < 1e-9;

    // Annule tout ordre NON-MARKET (limit/stop/stoplimit…) pour ce compte+symbole
    private void CancelNonMarketOrdersForThisAccSym(string reason)
    {
        int canceled = 0;
        foreach (var o in Core.Instance.Orders)
        {
            if (o == null) continue;

            // filtre acc+sym
            var accTxt = o.Account?.ToString() ?? "";
            var symTxt = o.Symbol?.Name ?? o.Symbol?.ToString() ?? "";
            if (!AccountMatches(accTxt, this.Account)) continue;
            if (!SymbolMatches(o.Symbol, this.Symbol)) continue;

            // états terminaux à ignorer
            var status = (o.Status + " " + o.State).ToLowerInvariant();
            if (status.Contains("filled") || status.Contains("cancel")
                || status.Contains("rejected") || status.Contains("expired")
                || status.Contains("done") || status.Contains("completed"))
                continue;

            // ignorer les MARKET
            var typeTxt = o.OrderTypeId?.ToString()?.ToUpperInvariant() ?? "";
            if (typeTxt.Contains("MARKET")) continue;

            try { Core.Instance.CancelOrder(o); canceled++; }
            catch (Exception ex) { Log($"[CANCEL NON-MKT][ERR] {ex.Message}", StrategyLoggingLevel.Error); }
        }
        if (canceled > 0)
            Log($"[CANCEL NON-MKT] {canceled} ordre(s) limit/stop annulé(s) ({reason}) | sym={this.Symbol?.Name} acc={this.Account}");
    }


    // Envoie un MARKET opposé pour fermer la position (tolérant + signé)
    // + annule IMMÉDIATEMENT les LIMIT/STOP du même acc+sym pour éviter les collisions
    private void FlattenAccSymViaMarket(string comment)
    {
        try
        {
            double net = NetQty_AccSym_Tolerant();   // SIGNÉ (long>0, short<0)

            // récupère une position correspondante (pour Symbol objet exact)
            Position anyPos = null;
            foreach (var p in Core.Instance.Positions)
            {
                if (p == null) continue;
                if (AccountMatches(p.Account, this.Account) && SymbolMatches(GetPosSymbolObj(p), this.Symbol) && Math.Abs(p.Quantity) > 1e-9)
                {
                    anyPos = p; break;
                }
            }

            if (Math.Abs(net) < 1e-9 && anyPos == null)
            {
                Log("[FLATTEN POS] déjà flat (acc+sym).");
                return;
            }

            // si net=0 mais une position existe, utilise la quantité SIGNÉE de cette position
            if (Math.Abs(net) < 1e-9 && anyPos != null)
                net = SignedQty(anyPos);

            // symbole objet : priorité position, sinon symbole de l’instance
            Symbol symObj = (anyPos != null) ? (GetPosSymbolObj(anyPos) as Symbol) : this.Symbol;

            var side = (net > 0) ? Side.Sell : Side.Buy;  // long -> SELL, short -> BUY
            var qty = Math.Abs(net);

            // (A) annule d'abord les LIMIT/STOP éventuels pour éviter qu'ils se déclenchent
            CancelNonMarketOrdersForThisAccSym("pre-flatten");

            // (B) place le MARKET opposé
            var req = new PlaceOrderRequestParameters()
            {
                Account = this.Account,
                Symbol = symObj,
                Side = side,
                Quantity = qty,
                OrderTypeId = OrderType.Market,
                Comment = comment,
                Price = 0.0
            };

            var res = Core.Instance.PlaceOrder(req);
            if (res.Status == TradingOperationResultStatus.Success)
                Log($"[FLATTEN POS] Sent MARKET {(side == Side.Sell ? "SELL" : "BUY")} qty={qty} on {SymbolNameOf(symObj)} / {this.Account}");
            else
                Log("[FLATTEN POS][ERR] PlaceOrder(MARKET) a échoué: " + res.Message, StrategyLoggingLevel.Error);
        }
        catch (Exception ex)
        {
            Log("[FLATTEN POS][ERR] " + ex.Message, StrategyLoggingLevel.Error);
        }
    }



    private void CancelIfAlive(Order ord)
    {
        if (ord == null) return;
        try { Core.Instance.CancelOrder(ord); } catch { /* ignore */ }
    }

    private void CancelRemainingChildrenIfFlat()
    {
        if (!childrenPlaced) return;

        bool flat = true;
        foreach (var p in Core.Instance.Positions)
            if (p.Symbol == this.Symbol && p.Account == this.Account && p.Quantity != 0)
            { flat = false; break; }

        if (!flat) return;

        try
        {
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

        slChild = null;
        tpChild = null;
        childrenPlaced = false;
        tradePlaced = false;

        Log("[OCO SIM] Flat detected → canceled remaining child order(s).", StrategyLoggingLevel.Info);
    }

    private bool IsFlatAccSym()
    {
        foreach (var p in Core.Instance.Positions)
            if (p != null && p.Symbol == this.Symbol && p.Account == this.Account && Math.Abs(p.Quantity) > 1e-9)
                return false;
        return true;
    }


    // ====== Orders (SL/TP en ABSOLU) ======
    // ====== Orders/Positions guard (version compatible universelle) ======
    // ====== Orders/Positions guard (version minimale) ======
    // ====== Orders/Positions guard (version debug) ======
    // ====== Orders/Positions guard (DEBUG MATCHES) ======
    private bool EnsureNoActiveExposure()
    {
        // --- clés de la stratégie
        string symId = this.Symbol?.Id ?? "";
        string symName = this.Symbol?.Name ?? "";
        string symKey = QT_NormSym(!string.IsNullOrEmpty(symId) ? symId : symName);
        string symRoot = QT_FutRoot(symKey);

        string accId = this.Account?.Id ?? "";
        string accStr = this.Account?.ToString() ?? "";
        string accKey = QT_NormAcc(!string.IsNullOrEmpty(accId) ? accId : accStr);

        Log($"[KEYS] symKey='{symKey}' root='{symRoot}' (Id='{symId}' Name='{symName}'), accKey='{accKey}' (Id='{accId}' ToStr='{accStr}')");

        int posChecked = 0, posMatched = 0, ordChecked = 0, ordMatched = 0;
        int logs = 0, LOG_MAX = 80; // évite d'inonder
        var conflicts = new System.Collections.Generic.List<string>();

        try
        {
            // === POSITIONS ===
            foreach (var pos in Core.Instance.Positions)
            {
                posChecked++; if (pos == null) continue;

                string pSymId = pos.Symbol?.Id ?? "";
                string pSymName = pos.Symbol?.Name ?? "";
                string psym = QT_NormSym(!string.IsNullOrEmpty(pSymId) ? pSymId : pSymName);
                string pRoot = QT_FutRoot(psym);

                string pAccId = pos.Account?.Id ?? "";
                string pAccStr = pos.Account?.ToString() ?? "";
                string pacc = QT_NormAcc(!string.IsNullOrEmpty(pAccId) ? pAccId : pAccStr);

                bool symMatch = QT_SameSymbol(psym, symKey);    // tolère root vs échéance
                bool accMatch = (pacc == accKey);

                if (symMatch && accMatch)
                {
                    posMatched++;
                    double qty = pos.Quantity;
                    if (logs++ < LOG_MAX)
                        Log($"[POS MATCH #{posMatched}] psym='{psym}' root='{pRoot}' (Id='{pSymId}' Name='{pSymName}'), pacc='{pacc}' (Id='{pAccId}') qty={qty}");

                    if (Math.Abs(qty) > 1e-9)
                        conflicts.Add($"POS qty={qty}");
                }
                else
                {
                    if (logs++ < LOG_MAX)
                        Log($"[POS NO-MATCH #{posChecked}] psym='{psym}' root='{pRoot}', pacc='{pacc}' | symMatch={symMatch}, accMatch={accMatch}");
                }
            }

            // === ORDRES ===
            foreach (var ord in Core.Instance.Orders)
            {
                ordChecked++; if (ord == null) continue;

                string oSymId = ord.Symbol?.Id ?? "";
                string oSymName = ord.Symbol?.Name ?? "";
                string osym = QT_NormSym(!string.IsNullOrEmpty(oSymId) ? oSymId : oSymName);
                string oRoot = QT_FutRoot(osym);

                string oAccId = ord.Account?.Id ?? "";
                string oAccStr = ord.Account?.ToString() ?? "";
                string oacc = QT_NormAcc(!string.IsNullOrEmpty(oAccId) ? oAccId : oAccStr);

                bool symMatch = QT_SameSymbol(osym, symKey);
                bool accMatch = (oacc == accKey);

                string type = (QT_TryGetStr(ord, "OrderTypeId") ?? QT_TryGetStr(ord, "OrderType") ?? "?");
                string status = (QT_TryGetStr(ord, "Status") ?? "");
                string state = (QT_TryGetStr(ord, "State") ?? "");
                string sAll = (status + " " + state).Trim();

                if (symMatch && accMatch)
                {
                    ordMatched++;
                    bool isLimit = QT_IsLimit(ord);
                    bool isActive = !QT_IsTerminal(ord);

                    if (logs++ < LOG_MAX)
                        Log($"[ORD MATCH #{ordMatched}] osym='{osym}' root='{oRoot}' (Id='{oSymId}' Name='{oSymName}'), oacc='{oacc}' (Id='{oAccId}'), type='{type}', status/state='{sAll}', isLimit={isLimit}, isActive={isActive}");

                    if (isLimit && isActive)
                        conflicts.Add($"ORD LIMIT actif [{sAll}]");
                }
                else
                {
                    if (logs++ < LOG_MAX)
                        Log($"[ORD NO-MATCH #{ordChecked}] osym='{osym}' root='{oRoot}', oacc='{oacc}', type='{type}', status/state='{sAll}' | symMatch={symMatch}, accMatch={accMatch}");
                }
            }

            Log($"[CHECK SUMMARY] posChecked={posChecked}, posMatched={posMatched}, ordChecked={ordChecked}, ordMatched={ordMatched}");

            if (conflicts.Count == 0)
            {
                Log("[CHECK OK] aucune exposition ni LIMIT actif pour ce compte+symbole.");
                return true;
            }

            foreach (var c in conflicts) Log("[BLOCK] " + c);
            Log($"[ORDER BLOCKED] Exposition ou LIMIT actif détecté pour {accKey}/{symKey}.");
            dayFinished = true; breakFound = true; signalArmed = false;
            return false;
        }
        catch (Exception ex)
        {
            Log("[CHECK ERROR] " + ex.Message);
            return false;
        }
    }



    // Helpers sûrs (noms uniques)
    private static string QT_TryGetStr(object obj, string name)
    {
        try
        {
            var p = obj?.GetType().GetProperty(name);
            return p == null ? null : Convert.ToString(p.GetValue(obj));
        }
        catch { return null; }
    }

    private static double? QT_ParseDouble(string s)
    {
        if (string.IsNullOrEmpty(s)) return null;
        if (double.TryParse(s, System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture, out var d)) return d;
        if (double.TryParse(s, out d)) return d;
        return null;
    }

    // ---- Helpers pour normaliser & tester le statut ----
    private static string QT_NormSym(string s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        s = s.Trim();
        int i = s.IndexOfAny(new[] { '@', ' ' }); // enlève @CME, etc.
        if (i > 0) s = s.Substring(0, i);
        return s.ToUpperInvariant();
    }
    private static string QT_NormAcc(string s)
    {
        return string.IsNullOrEmpty(s) ? "" : s.Trim().ToUpperInvariant();
    }

    // Extrait la racine future : "MNQZ5" -> "MNQ", "ESM4" -> "ES"
    private static readonly string QT_MonthLetters = "FGHJKMNQUVXZ";
    private static readonly string[] QT_MonthNames3 = { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };

    // Extrait la racine future d’un symbole CME : garde le produit sans le mois/année.
    // Exemples :
    //   MNQZ5    -> MNQ
    //   ESM24    -> ES
    //   6SZ5     -> 6S
    //   6EDEC25  -> 6E
    //   CLX2025  -> CL
    //   MNQ      -> MNQ (inchangé)
    private static string QT_FutRoot(string s)
    {
        s = QT_NormSym(s);
        if (string.IsNullOrEmpty(s)) return s;

        string up = s.ToUpperInvariant();

        // 1️⃣ Cas type "MNQZ5" ou "6SZ5" (lettre mois + chiffre année à la fin)
        if (up.Length >= 2 && char.IsDigit(up[^1]) && QT_MonthLetters.IndexOf(up[^2]) >= 0)
            return up.Substring(0, up.Length - 2);

        // 2️⃣ Cas type "ESM24" (lettre mois + 2 chiffres)
        if (up.Length >= 3 && char.IsDigit(up[^1]) && char.IsDigit(up[^2]) && QT_MonthLetters.IndexOf(up[^3]) >= 0)
            return up.Substring(0, up.Length - 3);

        // 3️⃣ Cas type "CLX2025" (lettre mois + 4 chiffres)
        if (up.Length >= 5 && char.IsDigit(up[^1]) && char.IsDigit(up[^2]) && char.IsDigit(up[^3]) && char.IsDigit(up[^4]) && QT_MonthLetters.IndexOf(up[^5]) >= 0)
            return up.Substring(0, up.Length - 5);

        // 4️⃣ Cas mois complet : "6EDEC25"
        foreach (var m3 in QT_MonthNames3)
        {
            if (up.EndsWith(m3 + "25") || up.EndsWith(m3 + "5") || up.EndsWith(m3 + "24") || up.EndsWith(m3 + "4"))
            {
                int idx = up.LastIndexOf(m3);
                if (idx > 0) return up.Substring(0, idx);
            }
        }

        // 5️⃣ Retire les chiffres finaux s’il en reste
        int i = up.Length - 1;
        while (i >= 0 && char.IsDigit(up[i])) i--;
        return up.Substring(0, i + 1);
    }


    // Compare symbole stratégie vs symbole ordre/position en tolérant root/échéance
    private static bool QT_SameSymbol(string a, string b)
    {
        if (string.IsNullOrEmpty(a) || string.IsNullOrEmpty(b)) return false;
        a = QT_NormSym(a);
        b = QT_NormSym(b);
        if (a == b) return true;                   // match exact (même month)
        return QT_FutRoot(a) == QT_FutRoot(b);     // match par racine (MNQ == MNQZ5)
    }

    private static bool QT_IsTerminal(object ord)
    {
        // on combine Status + State (chez Rithmic/Lucid 'State' est souvent 'Normal')
        string status = (QT_TryGetStr(ord, "Status") ?? "");
        string state = (QT_TryGetStr(ord, "State") ?? "");
        string s = (status + " " + state).ToLowerInvariant();

        return s.Contains("filled") || s.Contains("canceled") || s.Contains("cancelled")
            || s.Contains("rejected") || s.Contains("expired") || s.Contains("done")
            || s.Contains("completed");
    }
    private static bool QT_IsLimit(object ord)
    {
        string t = (QT_TryGetStr(ord, "OrderTypeId") ?? QT_TryGetStr(ord, "OrderType") ?? "").ToLowerInvariant();
        return t.Contains("limit");
    }




    // *** UNIQUE ComputeRiskQuantity (utilise le risque effectif) ***
    private int ComputeRiskQuantity(double effTick, double stopAbs)
    {
        int stopTicksForSize = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopAbs) / effTick));
        double riskCurrencyEff = EffectiveRiskCurrency(); // auto + plafond

        int qtyByRisk = Math.Max(1, (int)Math.Floor(
            riskCurrencyEff / (stopTicksForSize * Math.Max(1e-9, TickValue))
        ));
        return Math.Max(1, Math.Min(MaxContracts, qtyByRisk));
    }

    private Order FindRetestLimitOrder()
    {
        try
        {
            foreach (var ord in Core.Instance.Orders)
            {
                if (ord == null) continue;
                if (ord.Symbol != this.Symbol || ord.Account != this.Account) continue;

                string idStr = Convert.ToString(ord.Id);
                if (!string.IsNullOrEmpty(retestOrderId) && idStr == retestOrderId)
                    return ord;

                if (string.IsNullOrEmpty(retestOrderId))
                {
                    double priceTol = Math.Max(6 * EffectiveTickSize(), 1e-9);
                    var expectedSide = (side == "long") ? Side.Buy : Side.Sell;
                    if (ord.OrderTypeId == OrderType.Limit && ord.Side == expectedSide)
                    {
                        bool priceOk = Math.Abs(ord.Price - retestLevel) <= priceTol;
                        bool commentOk =
                            (!string.IsNullOrEmpty(RetestOrderComment) && (ord.Comment ?? "").IndexOf(RetestOrderComment, StringComparison.OrdinalIgnoreCase) >= 0) ||
                            (!string.IsNullOrEmpty(Tag) && (ord.Comment ?? "").IndexOf(Tag, StringComparison.OrdinalIgnoreCase) >= 0);
                        if (priceOk || commentOk)
                            return ord;
                    }
                }
            }
        }
        catch { }
        return null;
    }

    // Helper UniqueId (tolérant au connecteur)
    // --- Helpers IDs ultra-robustes ---
    private static long GetOrderUniqueId(Order o)
    {
        if (o == null) return -1;

        // 1) Propriétés courantes
        try
        {
            var p = o.GetType().GetProperty("UniqueId");
            if (p != null)
            {
                var v = p.GetValue(o);
                if (v != null && long.TryParse(v.ToString(), out var uid)) return uid;
            }
        }
        catch { }

        // 2) Variantes d'orthographe possibles
        string[] altProps = { "UniqueID", "Uniqueid", "UID", "OrderUniqueId", "OrderUID" };
        foreach (var name in altProps)
        {
            try
            {
                var p = o.GetType().GetProperty(name);
                if (p != null)
                {
                    var v = p.GetValue(o);
                    if (v != null && long.TryParse(v.ToString(), out var uid)) return uid;
                }
            }
            catch { }
        }

        // 3) Champs publics éventuels
        string[] fields = { "UniqueId", "UniqueID", "UID" };
        foreach (var f in fields)
        {
            try
            {
                var fi = o.GetType().GetField(f, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
                if (fi != null)
                {
                    var v = fi.GetValue(o);
                    if (v != null && long.TryParse(v.ToString(), out var uid)) return uid;
                }
            }
            catch { }
        }

        return -1;
    }

    private static string GetOrderIdStr(Order o)
    {
        if (o == null) return "?";
        try
        {
            var p = o.GetType().GetProperty("Id");
            if (p != null)
            {
                var v = p.GetValue(o);
                if (v != null) return v.ToString();
            }
        }
        catch { }
        return "?";
    }

    // Dump pour vérifier ce que l'on voit côté Strategy Manager
    private void DumpAccSymLimits()
    {
        try
        {
            string symKey = QT_NormSym(this.Symbol?.Id ?? this.Symbol?.Name ?? "");
            string accKey = QT_NormAcc(this.Account?.Id ?? this.Account?.ToString() ?? "");

            int k = 0;
            foreach (var o in Core.Instance.Orders)
            {
                if (o == null) continue;

                string osym = QT_NormSym(o.Symbol?.Id ?? o.Symbol?.Name ?? "");
                string oacc = QT_NormAcc(o.Account?.Id ?? o.Account?.ToString() ?? "");
                if (!(QT_SameSymbol(osym, symKey) && oacc == accKey)) continue;
                if (!QT_IsLimit(o)) continue;

                string id = GetOrderIdStr(o);
                long uid = GetOrderUniqueId(o);
                string st = (QT_TryGetStr(o, "Status") ?? "") + " " + (QT_TryGetStr(o, "State") ?? "");
                Log($"[LIMIT CAND #{++k}] sym={osym} acc={oacc} price={o.Price} side={o.Side} Id={id} UniqueId={uid} status/state='{st.Trim()}'");
            }
            if (k == 0) Log("[LIMIT CAND] (aucune LIMIT trouvée pour ce compte+symbole)");
        }
        catch (Exception ex)
        {
            Log("[LIMIT CAND ERROR] " + ex.Message);
        }
    }



    private void CancelRetestLimitOrder(string reason)
    {
        try
        {
            int canceledCount = 0;
            string symKey = QT_NormSym(this.Symbol?.Id ?? this.Symbol?.Name ?? "");
            string accKey = QT_NormAcc(this.Account?.Id ?? this.Account?.ToString() ?? "");

            foreach (var o in Core.Instance.Orders)
            {
                if (o == null) continue;

                string osym = QT_NormSym(o.Symbol?.Id ?? o.Symbol?.Name ?? "");
                string oacc = QT_NormAcc(o.Account?.Id ?? o.Account?.ToString() ?? "");
                if (!QT_SameSymbol(osym, symKey)) continue;
                if (oacc != accKey) continue;

                if (!QT_IsLimit(o)) continue;
                if (QT_IsTerminal(o)) continue;

                try
                {
                    Core.Instance.CancelOrder(o);
                    Log($"[CANCEL] LIMIT actif annulé ({reason}) | sym={osym} acc={oacc}", StrategyLoggingLevel.Info);
                    canceledCount++;
                }
                catch (Exception ex)
                {
                    Log($"[CANCEL ERROR] Impossible d'annuler LIMIT sym={osym} acc={oacc} ({ex.Message})", StrategyLoggingLevel.Error);
                }
            }

            if (canceledCount == 0)
                Log($"[CANCEL] Aucun LIMIT actif trouvé pour {symKey}/{accKey} ({reason})", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[CANCEL ERROR] " + ex.Message, StrategyLoggingLevel.Error);
        }

        // Reset complet de l’état local
        retestLimitOrder = null;
        retestOrderActive = false;
        retestOrderQty = 0;
        retestOrderId = null;
        retestOrderUid = -1;
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

        // === [RISK CONTROL] refuse si 1 contrat dépasse MaxRiskCurrency ===
        double oneLotRiskCurrency = slTicks * Math.Max(1e-9, TickValue); // $/tick * ticks SL
        if (qty == 1 && oneLotRiskCurrency > MaxRiskCurrency + 1e-9)
        {
            Log($"[RISK BLOCK] Retest LIMIT: 1 lot risk = {oneLotRiskCurrency:F2}$ > MaxRisk {MaxRiskCurrency:F2}$ → session close.");
            // reset local retest state pour être propre
            retestOrderActive = false;
            retestLimitOrder = null;
            retestOrderQty = 0;
            retestOrderId = null;
            retestOrderUid = -1;

            tradePlaced = false;
            dayFinished = true;   // journée terminée
            breakFound = true;
            return false;         // on NE place PAS l’ordre
        }


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

        // ✅ Pas de lookup d’Id/UniqueId, pas d’attente
        retestOrderActive = true;
        retestLimitOrder = null;
        retestOrderId = null;
        retestOrderUid = -1;
        nextLookupUtc = DateTime.UtcNow.AddSeconds(RetestLookupSeconds);

        double slApprox = (side == "long") ? orderPrice - slTicks * effTick : orderPrice + slTicks * effTick;
        double tpApprox = (side == "long") ? orderPrice + tpTicks * effTick : orderPrice - tpTicks * effTick;
        string expiryParis = TimeZoneInfo.ConvertTimeFromUtc(retestExpiryUtc, tzParis).ToString("HH:mm");

        Log($"[RETEST ORDER PLACED] LIMIT {side.ToUpper()} x{qty} @ {orderPrice:F5} | SL≈{slApprox:F5} TP≈{tpApprox:F5} | expires {expiryParis} Paris");
        Log("[RETEST ORDER BRACKET] OCO armed.", StrategyLoggingLevel.Info);
        return true;
    }



    private void TryResolveRetestOrderReference()
    {
        if (!retestOrderActive || !signalArmed) return;

        double effTick = EffectiveTickSize();
        double priceTol = Math.Max(6 * effTick, effTick);

        var expectedSide = (side == "long") ? Side.Buy : Side.Sell;
        string tag1 = RetestOrderComment ?? string.Empty;
        string tag2 = Tag ?? string.Empty;

        foreach (var ord in Core.Instance.Orders)
        {
            if (ord == null) continue;
            if (ord.Symbol != this.Symbol || ord.Account != this.Account) continue;
            if (ord.OrderTypeId != OrderType.Limit) continue;
            if (ord.Side != expectedSide) continue;

            bool byPrice = Math.Abs(ord.Price - retestLevel) <= priceTol;
            bool byComment =
                (!string.IsNullOrEmpty(tag1) && (ord.Comment ?? "").IndexOf(tag1, StringComparison.OrdinalIgnoreCase) >= 0) ||
                (!string.IsNullOrEmpty(tag2) && (ord.Comment ?? "").IndexOf(tag2, StringComparison.OrdinalIgnoreCase) >= 0);
            bool byQty = false;
            if (retestOrderQty > 0)
            {
                var qStr = TryGet(ord, "Quantity");
                if (double.TryParse(qStr, out var q))
                    byQty = Math.Abs(q - retestOrderQty) < 1e-9;
            }

            if (byPrice || byComment || byQty)
            {
                retestLimitOrder = ord;
                retestOrderId = Convert.ToString(ord.Id);
                retestOrderUid = GetOrderUniqueId(ord);  // <<<<<<<<< mémorise UniqueId
                var qStrLog = TryGet(retestLimitOrder, "Quantity") ?? "?";
                Log($"[ORDERS SNAPSHOT AFTER PLACEMENT] linked id={retestOrderId} uid={retestOrderUid} price={retestLimitOrder.Price:F5} qty={qStrLog}", StrategyLoggingLevel.Info);
                break;
            }
        }

        nextLookupUtc = DateTime.UtcNow.AddSeconds(RetestLookupSeconds);
    }


    private bool PlaceInitialOrder()
    {
        if (!EnsureNoActiveExposure())
            return false;

        double effTick = EffectiveTickSize();
        double stopAbs = RoundToTick(stopPx, effTick);
        double tpAbs = RoundToTick(tpPx, effTick);

        int qty = ComputeRiskQuantity(effTick, stopAbs);

        var sideOrd = (side == "long") ? Side.Buy : Side.Sell;
        var orderType = UseLimitOrder ? OrderType.Limit : OrderType.Market;
        double orderPrice = RoundToTick(entryPx, effTick);

        if (SimulateOnly)
        {
            Log($"[SIM ORDER] {orderType} {side.ToUpper()} x{qty} | Entry={orderPrice:F5} SL={stopAbs:F5} TP={tpAbs:F5}");
            return true;
        }

        int slTicks = Math.Max(1, (int)Math.Round(Math.Abs(entryPx - stopAbs) / effTick));
        int tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tpAbs - entryPx) / effTick));
        // === [RISK CONTROL] refuse si 1 contrat dépasse MaxRiskCurrency ===
        double oneLotRiskCurrency = slTicks * Math.Max(1e-9, TickValue);
        if (qty == 1 && oneLotRiskCurrency > MaxRiskCurrency + 1e-9)
        {
            Log($"[RISK BLOCK] Initial {orderType}: 1 lot risk = {oneLotRiskCurrency:F2}$ > MaxRisk {MaxRiskCurrency:F2}$ → session close.");
            tradePlaced = false;
            dayFinished = true;
            breakFound = true;
            return false;
        }


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
            StopLoss = stopHolder,
            TakeProfit = tpHolder,
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
            Log($"[ORDER] {orderType} {side.ToUpper()} x{qty} @ {orderPrice:F5} | SL≈{stopAbs:F5} TP≈{tpAbs:F5}");
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

        boxFrozenCloseUtc = TimeZoneInfo.ConvertTimeToUtc(
            new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, BoxEndHour, BoxEndMinute, 0, lonNow.Kind),
            tzLon
        );

        boxReady = true;
    }

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

    // Annule TOUS les ordres actifs (limit/stop/market working/oco...) du même compte+symbole.
    // retryMs=0 => pas de retry. Mets 2000 si tu veux 2s de retry.
    private void CancelAllOrdersForThisAccSym(string reason, int retryMs = 0, int stepMs = 200)
    {
        try
        {
            string symKey = QT_NormSym(this.Symbol?.Id ?? this.Symbol?.Name ?? "");
            string accKey = QT_NormAcc(this.Account?.Id ?? this.Account?.ToString() ?? "");

            int canceled = 0, waited = 0;

            do
            {
                foreach (var o in Core.Instance.Orders)
                {
                    if (o == null) continue;

                    string osym = QT_NormSym(o.Symbol?.Id ?? o.Symbol?.Name ?? "");
                    string oacc = QT_NormAcc(o.Account?.Id ?? o.Account?.ToString() ?? "");
                    if (!QT_SameSymbol(osym, symKey)) continue;
                    if (oacc != accKey) continue;
                    if (QT_IsTerminal(o)) continue; // déjà Filled/Cancelled/etc.

                    try
                    {
                        Core.Instance.CancelOrder(o);
                        canceled++;
                        Log($"[CANCEL ALL] ordre actif annulé ({reason}) | sym={osym} acc={oacc}", StrategyLoggingLevel.Info);
                    }
                    catch (Exception ex)
                    {
                        Log($"[CANCEL ALL ERROR] sym={osym} acc={oacc} → {ex.Message}", StrategyLoggingLevel.Error);
                    }
                }

                if (retryMs <= 0 || canceled > 0) break;
                try { System.Threading.Thread.Sleep(stepMs); } catch { }
                waited += stepMs;

            } while (waited < retryMs);

            if (canceled == 0)
                Log($"[CANCEL ALL] Aucun ordre actif trouvé pour {symKey}/{accKey} ({reason})", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[CANCEL ALL ERROR] " + ex.Message, StrategyLoggingLevel.Error);
        }
    }

    // ====== Utils ======
    private void ResetDay(DateTime nowUtc)
    {
        var lon = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzLon);
        currentLonDate = lon.Date;

        //CancelRetestLimitOrder("day reset");

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxMid = 0.0; boxReady = false;
        aggTF.Clear(); validatedBuckets.Clear();

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
        retestOrderId = null;
        retestOrderUid = -1;
        nextLookupUtc = DateTime.MinValue;
        maxTradeEndDone = false;
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

        // 1) on annule d’abord tous les LIMIT (même compte + même symbole)
        CancelRetestLimitOrder("overextension exceeded (tick)");

        // micro-flush pour laisser le broker propager l’annulation
        try { System.Threading.Thread.Sleep(200); } catch { }

        // 2) on ferme uniquement les positions (on ne retouche plus aux ordres ici)
        if (!SimulateOnly)
            ClosePositionsOnly();

        // reset local
        retestOrderActive = false;
        retestLimitOrder = null;
        retestOrderQty = 0;

        signalArmed = false;
        tradePlaced = false;
        dayFinished = true;

        Log($"[OVEREXT AUTO-CANCEL] runup/rundown dépassé → ordre annulé avant retest. (move={move:F5} limit={limit:F5})", StrategyLoggingLevel.Info);
        ;
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
        int wdPy = ((int)lon.DayOfWeek + 6) % 7;
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
            string targetSym = this.Symbol?.Id ?? this.Symbol?.Name ?? "";
            string targetAcc = this.Account?.Id ?? "";

            Log($"[FORCE FLATTEN] Begin for {targetSym} / {targetAcc}", StrategyLoggingLevel.Info);

            foreach (var pos in Core.Instance.Positions)
            {
                if (pos == null) continue;

                string symId = pos.Symbol?.Id ?? pos.Symbol?.Name ?? "";
                string accId = pos.Account?.Id ?? "";

                if (!string.Equals(symId, targetSym, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (!string.Equals(accId, targetAcc, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (pos.Quantity == 0) continue;

                var sideToClose = pos.Quantity > 0 ? Side.Sell : Side.Buy;
                double qty = Math.Abs(pos.Quantity);

                var req = new PlaceOrderRequestParameters
                {
                    Account = this.Account,
                    Symbol = this.Symbol,
                    Side = sideToClose,
                    Quantity = qty,
                    OrderTypeId = OrderType.Market,
                    Comment = Tag + "-FORCEFLAT"
                };

                var res = Core.Instance.PlaceOrder(req);
                Log($"[FLATTEN] Sent MARKET {sideToClose} x{qty} to close position id={pos.Id} → {res.Status}", StrategyLoggingLevel.Info);
            }

            foreach (var ord in Core.Instance.Orders)
            {
                if (ord == null) continue;

                string symId = ord.Symbol?.Id ?? ord.Symbol?.Name ?? "";
                string accId = ord.Account?.Id ?? "";

                if (!string.Equals(symId, targetSym, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (!string.Equals(accId, targetAcc, StringComparison.OrdinalIgnoreCase))
                    continue;

                try
                {
                    Core.Instance.CancelOrder(ord);
                    Log($"[FLATTEN] Canceled order id={ord.Id} price={ord.Price:F5} comment={ord.Comment}", StrategyLoggingLevel.Info);
                }
                catch (Exception inner)
                {
                    Log($"[FLATTEN ERROR] Cancel {ord.Id}: {inner.Message}", StrategyLoggingLevel.Error);
                }
            }

            Log($"[FORCE FLATTEN COMPLETE] {targetSym}/{targetAcc} flattened.", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[FORCE FLATTEN ERROR] " + ex.Message, StrategyLoggingLevel.Error);
        }
    }

    private void ClosePositionsOnly()
    {
        try
        {
            string targetSym = this.Symbol?.Id ?? this.Symbol?.Name ?? "";
            string targetAcc = this.Account?.Id ?? "";

            Log($"[FORCE FLATTEN POS] Begin for {targetSym} / {targetAcc}", StrategyLoggingLevel.Info);

            foreach (var pos in Core.Instance.Positions)
            {
                if (pos == null) continue;

                string symId = pos.Symbol?.Id ?? pos.Symbol?.Name ?? "";
                string accId = pos.Account?.Id ?? "";

                if (!string.Equals(symId, targetSym, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (!string.Equals(accId, targetAcc, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (pos.Quantity == 0) continue;

                var sideToClose = pos.Quantity > 0 ? Side.Sell : Side.Buy;
                double qty = Math.Abs(pos.Quantity);

                var req = new PlaceOrderRequestParameters
                {
                    Account = this.Account,
                    Symbol = this.Symbol,
                    Side = sideToClose,
                    OrderTypeId = OrderType.Market,
                    Quantity = qty,
                    Comment = Tag + "-FORCEFLAT-POS"
                };

                var res = Core.Instance.PlaceOrder(req);
                Log($"[FLATTEN POS] Sent MARKET {sideToClose} x{qty} to close position id={pos.Id} → {res.Status}", StrategyLoggingLevel.Info);
            }

            Log($"[FORCE FLATTEN POS COMPLETE] {targetSym}/{targetAcc} positions flattened.", StrategyLoggingLevel.Info);
        }
        catch (Exception ex)
        {
            Log("[FORCE FLATTEN POS ERROR] " + ex.Message, StrategyLoggingLevel.Error);
        }
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
