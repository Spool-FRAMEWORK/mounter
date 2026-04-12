package software.spool.mounter;

public record MarketSummary(
        long samples,
        double avgOpen,
        double avgClose,
        double highestHigh,
        double lowestLow,
        double avgSpread,
        double avgSpreadBps,
        double avgVolatility,
        double avgMomentumPct
) {}