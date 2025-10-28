using System;
using System.Linq;
using System.Collections.Generic;
using TradingPlatform.BusinessLayer;

public class SBA_1m_OHLC_Trace : Strategy
{
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("Account", 20)] public Account Account;

    private HistoricalData hd1m;
    private TimeZoneInfo tzLon;

    // Live tick -> 1m cache (for comparison)
    private DateTime? liveBarStartUtc;
    private double liveO, liveH, liveL, liveC; private long liveV;
    private readonly Dictionary<DateTime, (double O, double H, double L, double C, long V)> live1mAtClose = new();

    protected override void OnRun()
    {
        if (Symbol == null) { Log("Select Symbol.", StrategyLoggingLevel.Error); return; }

        tzLon = GetLondonTz();

        // Load 1m history from today's midnight London minus a buffer
        var lonNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tzLon);
        var lonMid = new DateTime(lonNow.Year, lonNow.Month, lonNow.Day, 0, 0, 0, lonNow.Kind);
        var lonFrom = lonMid.AddHours(-2);
        var utcFrom = TimeZoneInfo.ConvertTimeToUtc(lonFrom, tzLon);

        hd1m = Symbol.GetHistory(Period.MIN1, HistoryType.Last, utcFrom);
        Log($"[TRACE INIT] preload count={hd1m?.Count ?? 0} from {utcFrom:yyyy-MM-dd HH:mm:ss}Z");

        if (hd1m != null) hd1m.NewHistoryItem += OnNewBar;
        Symbol.NewLast += OnNewLast;

        Log("SBA_1m_OHLC_Trace started.");
    }

    protected override void OnStop()
    {
        if (hd1m != null) hd1m.NewHistoryItem -= OnNewBar;
        try { Symbol.NewLast -= OnNewLast; } catch { }
        if (liveBarStartUtc.HasValue) CloseCurrentLiveBar(liveBarStartUtc.Value.AddMinutes(1));
        Log("SBA_1m_OHLC_Trace stopped.");
    }

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

    private void OnNewBar(object sender, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar b) return;

        // On cette plateforme, TimeLeft = CLOTURE
        DateTime closeUtc = DateTime.SpecifyKind(b.TimeLeft, DateTimeKind.Utc);
        DateTime openUtc = closeUtc.AddMinutes(-1);
        var lonClose = TimeZoneInfo.ConvertTimeFromUtc(closeUtc, tzLon);

        Log($"[NEWBAR] 1m close={closeUtc:HH:mm:ss}Z (LON {lonClose:HH:mm:ss}) | O={b.Open:F5} H={b.High:F5} L={b.Low:F5} C={b.Close:F5}");

        // --- Method A: select by OPEN time (wrong if TimeLeft=close) ---
        var openSelect = FindBarByOpen(openUtc);
        if (openSelect.found)
            Log($"[A-byOPEN]  openUtc={openUtc:HH:mm:ss}Z -> O={openSelect.bar.Open:F5} H={openSelect.bar.High:F5} L={openSelect.bar.Low:F5} C={openSelect.bar.Close:F5} (EXPECT: same as NEWBAR if TimeLeft=open)");
        else
            Log($"[A-byOPEN]  openUtc={openUtc:HH:mm:ss}Z -> NOT FOUND");

        // --- Method B: select by CLOSE time (TimeLeft) ---
        var closeSelect = FindBarByClose(closeUtc);
        if (closeSelect.found)
            Log($"[B-byCLOSE] closeUtc={closeUtc:HH:mm:ss}Z -> O={closeSelect.bar.Open:F5} H={closeSelect.bar.High:F5} L={closeSelect.bar.Low:F5} C={closeSelect.bar.Close:F5} (EXPECT: same as NEWBAR if TimeLeft=close)");
        else
            Log($"[B-byCLOSE] closeUtc={closeUtc:HH:mm:ss}Z -> NOT FOUND");

        // --- Neighbors (prev/next by close) ---
        var prev = FindBarByClose(closeUtc.AddMinutes(-1));
        var next = FindBarByClose(closeUtc.AddMinutes(1));
        Log($"[NEIGHBORS] prevClose={closeUtc.AddMinutes(-1):HH:mm:ss}Z -> {(prev.found ? $"O={prev.bar.Open:F5} H={prev.bar.High:F5} L={prev.bar.Low:F5} C={prev.bar.Close:F5}" : "n/a")} | nextClose={closeUtc.AddMinutes(1):HH:mm:ss}Z -> {(next.found ? $"O={next.bar.Open:F5} H={next.bar.High:F5} L={next.bar.Low:F5} C={next.bar.Close:F5}" : "n/a")}");

        // --- Live-built 1m (tick aggregation) at that minute close ---
        if (live1mAtClose.TryGetValue(closeUtc, out var lv))
            Log($"[LIVE-AGG]   closeUtc={closeUtc:HH:mm:ss}Z -> O={lv.O:F5} H={lv.H:F5} L={lv.L:F5} C={lv.C:F5} (from ticks)");
        else
            Log($"[LIVE-AGG]   closeUtc={closeUtc:HH:mm:ss}Z -> n/a");

        // --- Summary: which method equals NEWBAR? ---
        bool eqA = openSelect.found && EqBar(b, openSelect.bar);
        bool eqB = closeSelect.found && EqBar(b, closeSelect.bar);
        Log($"[SUMMARY] matchA(byOPEN)={eqA} | matchB(byCLOSE)={eqB}");
        Log("----------------------------------------------------------------");
    }

    private (bool found, HistoryItemBar bar) FindBarByOpen(DateTime openUtc)
    {
        DateTime targetOpen = TruncToMinute(openUtc);
        for (int i = 0; i < hd1m.Count; i++)
        {
            if (hd1m[i] is not HistoryItemBar bb) continue;
            DateTime barClose = TruncToMinute(DateTime.SpecifyKind(bb.TimeLeft, DateTimeKind.Utc));
            DateTime barOpen = barClose.AddMinutes(-1);
            if (barOpen == targetOpen) return (true, bb);
        }
        return (false, null);
    }

    private (bool found, HistoryItemBar bar) FindBarByClose(DateTime closeUtc)
    {
        DateTime targetClose = TruncToMinute(closeUtc);
        for (int i = 0; i < hd1m.Count; i++)
        {
            if (hd1m[i] is not HistoryItemBar bb) continue;
            DateTime barClose = TruncToMinute(DateTime.SpecifyKind(bb.TimeLeft, DateTimeKind.Utc));
            if (barClose == targetClose) return (true, bb);
        }
        return (false, null);
    }

    private static bool EqBar(HistoryItemBar a, HistoryItemBar b)
        => a.Open == b.Open && a.High == b.High && a.Low == b.Low && a.Close == b.Close;

    private static DateTime TruncToMinute(DateTime dt) => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, DateTimeKind.Utc);

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
}
