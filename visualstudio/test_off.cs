using System;
using System.Collections.Generic;
using TradingPlatform.BusinessLayer;

public class OPR_TestBox_Simulator : Strategy
{
    // ===== Inputs =====
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    // Heures NY de ta box (ex: 09:30 → 09:44)
    [InputParameter("Box Start Hour NY", 30)] public int BoxStartHour = 9;
    [InputParameter("Box Start Minute NY", 40)] public int BoxStartMinute = 30;
    [InputParameter("Box End Hour NY", 50)] public int BoxEndHour = 9;
    [InputParameter("Box End Minute NY", 60)] public int BoxEndMinute = 44;

    // Trading / simu
    [InputParameter("Place real order?", 70)] public bool PlaceRealOrder = false; // sinon: simulation
    [InputParameter("Scan past break on start?", 80)] public bool ScanPastBreakOnStart = true;

    // Valeurs de marché (à ajuster par symbole si tu veux des ticks/logs en ticks)
    [InputParameter("Tick Size", 90)] public double TickSize = 1.0;
    [InputParameter("Tick Value", 100)] public double TickValueUSD = 5.0;
    [InputParameter("Max Contracts", 110)] public int MaxContracts = 1;

    // ===== Internes =====
    private HistoricalData hd1m; // 1m moteur (Last)
    private TimeZoneInfo tzNy;

    // Box
    private DateTime currentNyDate;
    private TimeSpan BoxStartSpan, BoxEndSpan;
    private bool boxReady;
    private double boxHigh = double.MinValue, boxLow = double.MaxValue, boxMid;

    // 1m LIVE (ticks→bar) pour une précision max sur la minute de cassure
    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC;
    private long liveV;
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    // Cassure / trade 1m
    private bool breakDoneToday;
    private readonly HashSet<DateTime> validated1m = new(); // minutes 1m déjà traitées

    // ===== Lifecycle =====
    protected override void OnRun()
    {
        if (Symbol == null) { Log("Select a Symbol.", StrategyLoggingLevel.Error); return; }

        tzNy = GetNyTz();
        BoxStartSpan = new TimeSpan(BoxStartHour, BoxStartMinute, 0);
        BoxEndSpan = new TimeSpan(BoxEndHour, BoxEndMinute, 59);

        ResetDay(DateTime.UtcNow);

        // 1m moteur en Last (TRADES)
        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, DateTime.UtcNow.AddHours(-12));
        hd1m.NewHistoryItem += OnNewEngineBar;

        // Backfill box (et éventuellement cassure si on démarre tard)
        TryBackfillBoxFromHistory();

        // Flux LIVE ticks → 1m
        Symbol.NewLast += OnNewLast;

        Log("OPR_TestBox_Simulator started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewEngineBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue)
            CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));

        Log("OPR_TestBox_Simulator stopped.");
    }

    // ===== Ticks → 1m live =====
    private void OnNewLast(Symbol symbol, Last last)
    {
        if (symbol != this.Symbol) return;

        double price = last.Price;
        DateTime? tickTime = ParseDate(TryGet(last, "ServerTime") ?? TryGet(last, "Time"));
        DateTime tUtc = tickTime.HasValue ? DateTime.SpecifyKind(tickTime.Value, DateTimeKind.Utc) : DateTime.UtcNow;
        DateTime start = new DateTime(tUtc.Year, tUtc.Month, tUtc.Day, tUtc.Hour, tUtc.Minute, 0, DateTimeKind.Utc);

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
        // pas de log 1m pour éviter le spam
    }

    // ===== Moteur 1m: box + cassure 1m =====
    private void OnNewEngineBar(object s, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar bar) return;

        DateTime barOpenUtc = DateTime.SpecifyKind(bar.TimeLeft, DateTimeKind.Utc); // si besoin -> TimeRight
        var ny = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzNy);

        if (ny.Date != currentNyDate) ResetDay(barOpenUtc);

        // 1) Construire la box sur la fenêtre choisie
        if (!boxReady)
        {
            if (IsInRange(ny.TimeOfDay, BoxStartSpan, BoxEndSpan))
            {
                UpdateBoxWith(bar.High, bar.Low);
                TryMergeLiveMinute(barOpenUtc); // merge LIVE si dispo
            }
            if (ny.TimeOfDay > BoxEndSpan) FinalizeBoxIfAny(ny);
        }

        // 2) Cassure 1m après la fin de box (si pas déjà faite)
        if (boxReady && !breakDoneToday && ny.TimeOfDay > BoxEndSpan)
            CheckBreakAndSimTrade(bar, barOpenUtc);
    }

    // ===== Box helpers =====
    private void UpdateBoxWith(double hi, double lo)
    {
        if (hi == double.MinValue || lo == double.MaxValue) return;
        if (boxHigh == double.MinValue) { boxHigh = hi; boxLow = lo; }
        else { boxHigh = Math.Max(boxHigh, hi); boxLow = Math.Min(boxLow, lo); }
    }

    private void TryMergeLiveMinute(DateTime engineBarOpenUtc)
    {
        var closeUtc = engineBarOpenUtc.AddMinutes(1);
        if (live1mAtClose.TryGetValue(closeUtc, out var lv))
            UpdateBoxWith(lv.H, lv.L);
    }

    private void FinalizeBoxIfAny(DateTime nyNow)
    {
        if (boxHigh == double.MinValue || boxLow == double.MaxValue) return;
        boxMid = 0.5 * (boxHigh + boxLow);
        boxReady = true;
        Log($"[BOX READY] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} @ {nyNow:HH:mm} NY");
    }

    // ===== Cassure & simu =====
    private void CheckBreakAndSimTrade(HistoryItemBar bar, DateTime barOpenUtc)
    {
        var ny = TimeZoneInfo.ConvertTimeFromUtc(barOpenUtc, tzNy);
        var closeUtc = barOpenUtc.AddMinutes(1);
        if (validated1m.Contains(closeUtc)) return;

        // Remplacer par LIVE si dispo à la minute
        double o = bar.Open, h = bar.High, l = bar.Low, c = bar.Close;
        bool usedLive = false;
        if (live1mAtClose.TryGetValue(closeUtc, out var lv))
        {
            o = lv.O; h = lv.H; l = lv.L; c = lv.C; usedLive = true;
        }

        // Première close en dehors ⇒ cassure validée
        string side = null;
        if (c > boxHigh) side = "long";
        else if (c < boxLow) side = "short";
        if (side == null) return;

        validated1m.Add(closeUtc);
        Log($"[BREAK OK] side={side} src={(usedLive ? "LIVE1m" : "ENGINE1m")} O={o:F2} H={h:F2} L={l:F2} C={c:F2} @ {ny:HH:mm} NY");

        // Simu: Entry = bord de box ; SL = mid ; TP = 1R
        double entry = (side == "long") ? boxHigh : boxLow;
        double sl = boxMid;
        double risk = Math.Abs(entry - sl);
        if (risk <= 0) { breakDoneToday = true; return; }
        double tp = (side == "long") ? (entry + risk) : (entry - risk);

        int qty = Math.Max(1, Math.Min(MaxContracts, 1)); // simple
        string tag = "TESTBOX";

        if (PlaceRealOrder && Account != null)
        {
            // place un ORDRE MARKET + SL/TP en offset (en ticks)
            int slTicks = Math.Max(1, (int)Math.Round(Math.Abs(entry - sl) / Math.Max(1e-12, TickSize)));
            int tpTicks = Math.Max(1, (int)Math.Round(Math.Abs(tp - entry) / Math.Max(1e-12, TickSize)));

            var sideOrd = (side == "long") ? Side.Buy : Side.Sell;
            var req = new PlaceOrderRequestParameters()
            {
                Account = this.Account,
                Symbol = this.Symbol,
                Side = sideOrd,
                Quantity = qty,
                OrderTypeId = OrderType.Market,
                StopLoss = SlTpHolder.CreateSL(slTicks, PriceMeasurement.Offset),
                TakeProfit = SlTpHolder.CreateTP(tpTicks, PriceMeasurement.Offset),
                Comment = tag
            };
            var res = Core.Instance.PlaceOrder(req);
            if (res.Status != TradingOperationResultStatus.Success)
                Log("Order failed: " + res.Message, StrategyLoggingLevel.Error);
            else
                Log($"[TRADE] {side.ToUpper()} x{qty} Entry={entry:F2} SL={sl:F2} ({slTicks}t) TP={tp:F2} ({tpTicks}t) R=1.00");
        }
        else
        {
            Log($"[SIM TRADE] {side.ToUpper()} x{qty} Entry={entry:F2} SL={sl:F2} TP={tp:F2} R=1.00");
        }

        breakDoneToday = true; // 1 setup / jour
    }

    // ===== Backfill =====
    private void TryBackfillBoxFromHistory()
    {
        if (hd1m == null) return;

        var nowNy = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzNy);
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
            if (t >= BoxStartSpan && t <= BoxEndSpan)
            {
                hi = (hi == double.MinValue) ? b.High : Math.Max(hi, b.High);
                lo = (lo == double.MaxValue) ? b.Low : Math.Min(lo, b.Low);
                used++;
            }
        }

        if (used > 0)
        {
            boxHigh = hi; boxLow = lo; boxMid = 0.5 * (boxHigh + boxLow);

            if (nowNy.TimeOfDay <= BoxEndSpan)
            {
                // On est avant ou pendant la fenêtre : laisser se compléter en live.
                Log($"[BOX BACKFILL PARTIAL] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2}  minutes={used}  @ start {nowNy:HH:mm} NY");
            }
            else
            {
                // Fenêtre finie : fige box
                boxReady = true;
                Log($"[BOX BACKFILL READY] low={boxLow:F2} high={boxHigh:F2} mid={boxMid:F2} (frozen @ {BoxEndHour:00}:{BoxEndMinute:00} NY)");

                // Option: scanner la cassure passée pour déclencher tout de suite
                if (ScanPastBreakOnStart && !breakDoneToday)
                {
                    for (int i = 0; i < hd1m.Count; i++)
                    {
                        if (hd1m[i] is not HistoryItemBar b) continue;
                        DateTime openUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
                        var ny = TimeZoneInfo.ConvertTimeFromUtc(openUtc, tzNy);
                        if (ny.Date != nyDate) continue;
                        if (ny.TimeOfDay <= BoxEndSpan) continue;

                        // first 1m close outside
                        if (b.Close > boxHigh || b.Close < boxLow)
                        {
                            CheckBreakAndSimTrade(b, openUtc); // réutilise la logique (remplacera par LIVE si dispo)
                            break;
                        }
                    }
                }
            }
        }
    }

    // ===== Utils =====
    private void ResetDay(DateTime nowUtc)
    {
        var ny = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzNy);
        currentNyDate = ny.Date;

        boxHigh = double.MinValue; boxLow = double.MaxValue; boxMid = 0.0; boxReady = false;
        validated1m.Clear();
        breakDoneToday = false;
    }

    private static bool IsInRange(TimeSpan t, TimeSpan a, TimeSpan b) => t >= a && t <= b;

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
