using System;
using System.Collections.Generic;
using TradingPlatform.BusinessLayer;

public class Test_EMA50_Last50Seed : Strategy
{
    [InputParameter("Symbol", 10)] public Symbol Symbol;
    [InputParameter("TF minutes", 20)] public int TFmin = 5;
    [InputParameter("Period", 30)] public int Period = 50;
    [InputParameter("PriceType", 40)] public PriceType PriceType = PriceType.Close;

    private HistoricalData hd;
    private double ema;            // EMA roulante
    private bool emaInit;
    private double alpha;

    // buffer des 50 dernières closes pour seed + debug
    private readonly Queue<double> lastN = new();

    protected override void OnRun()
    {
        if (Symbol == null)
        {
            Log("Select Symbol!", StrategyLoggingLevel.Error);
            return;
        }

        alpha = 2.0 / (Period + 1.0);

        TradingPlatform.BusinessLayer.Period p = TFmin switch
        {
            1 => TradingPlatform.BusinessLayer.Period.MIN1,
            5 => TradingPlatform.BusinessLayer.Period.MIN5,
            10 => TradingPlatform.BusinessLayer.Period.MIN10,
            15 => TradingPlatform.BusinessLayer.Period.MIN15,
            30 => TradingPlatform.BusinessLayer.Period.MIN30,
            60 => TradingPlatform.BusinessLayer.Period.HOUR1,
            _ => TradingPlatform.BusinessLayer.Period.MIN5
        };

        // On prend juste assez d'historique pour remplir la fenêtre (ex: 200 barres, large)
        var from = DateTime.UtcNow.AddMinutes(-TFmin * Math.Max(Period * 4, 200));
        hd = Symbol.GetHistory(p, HistoryType.Last, from);
        if (hd == null || hd.Count == 0)
        {
            Log("No historical data!", StrategyLoggingLevel.Error);
            return;
        }

        // Warm-up silencieux : remplir le buffer des 50 dernières et SEEDER l'EMA avec SMA(last 50)
        for (int i = 0; i < hd.Count; i++)
        {
            if (hd[i] is not HistoryItemBar b) continue;
            double x = SelectPrice(b, PriceType);
            PushLastN(x);

            if (!emaInit && lastN.Count == Period)
            {
                ema = SMA(lastN);
                emaInit = true;

                // Debug : bornes de la fenêtre de seed
                (double lo, double hi) = MinMax(lastN);
                Log($"[SEED] EMA{Period} = SMA(last {Period}) = {ema:F5} | window[min={lo:F5}, max={hi:F5}]");
            }
        }

        Log($"[INIT] Ready — TF={TFmin}m | preload={hd.Count} bars. Logging NEW bars only.");
        hd.NewHistoryItem += OnNewBar;
    }

    protected override void OnStop()
    {
        if (hd != null) hd.NewHistoryItem -= OnNewBar;
        Log("[STOP] done.");
    }

    private void OnNewBar(object s, HistoryEventArgs e)
    {
        if (e.HistoryItem is not HistoryItemBar b) return;

        double x = SelectPrice(b, PriceType);
        PushLastN(x);

        if (!emaInit && lastN.Count == Period)
        {
            ema = SMA(lastN);
            emaInit = true;

            (double lo, double hi) = MinMax(lastN);
            Log($"[SEED LIVE] EMA{Period} = {ema:F5} | window[min={lo:F5}, max={hi:F5}]");
        }
        else if (emaInit)
        {
            // EMA t = α*x + (1-α)*EMA_{t-1}
            ema = alpha * x + (1 - alpha) * ema;

            // log 1 ligne par nouvelle bougie
            (double lo, double hi) = MinMax(lastN);
            Log($"[{b.TimeLeft:HH:mm}] src={x:F5} | EMA(last50-seeded)={ema:F5} | win[min={lo:F5}, max={hi:F5}]");
        }
        else
        {
            Log($"[{b.TimeLeft:HH:mm}] collecting bars... {lastN.Count}/{Period}");
        }
    }

    // ===== helpers =====
    private void PushLastN(double x)
    {
        lastN.Enqueue(x);
        while (lastN.Count > Period) lastN.Dequeue();
    }

    private static double SMA(IEnumerable<double> xs)
    {
        double s = 0; int n = 0;
        foreach (var v in xs) { s += v; n++; }
        return n > 0 ? s / n : 0.0;
    }

    private static (double lo, double hi) MinMax(IEnumerable<double> xs)
    {
        double lo = double.PositiveInfinity, hi = double.NegativeInfinity;
        foreach (var v in xs) { if (v < lo) lo = v; if (v > hi) hi = v; }
        if (double.IsInfinity(lo)) lo = 0;
        if (double.IsInfinity(hi)) hi = 0;
        return (lo, hi);
    }

    private static double SelectPrice(HistoryItemBar b, PriceType pt) => pt switch
    {
        PriceType.Open => b.Open,
        PriceType.High => b.High,
        PriceType.Low => b.Low,
        PriceType.Median => 0.5 * (b.High + b.Low),
        PriceType.Typical => (b.High + b.Low + b.Close) / 3.0,
        PriceType.Weighted => (b.Open + b.High + b.Low + b.Close) / 4.0,
        _ => b.Close
    };
}
