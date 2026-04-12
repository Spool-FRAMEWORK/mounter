package software.spool.mounter.api.adapter;

import software.spool.mounter.MarketSummary;
import software.spool.mounter.api.port.MountAggregator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class MarketSummaryAggregator implements MountAggregator<Set<Map.Entry<String, Object>>, MarketSummary> {

    @Override
    public Stream<MarketSummary> aggregate(Stream<Set<Map.Entry<String, Object>>> records) {
        record Accumulator(
                long count,
                double sumOpen, double sumClose,
                double maxHigh, double minLow,
                double sumSpread, double sumSpreadBps,
                double sumVolatility, double sumMomentum
        ) {
            static Accumulator empty() {
                return new Accumulator(0, 0, 0, Double.MIN_VALUE, Double.MAX_VALUE, 0, 0, 0, 0);
            }

            Accumulator combine(Set<Map.Entry<String, Object>> entry) {
                double open   = getDouble(entry, "open");
                double close  = getDouble(entry, "close");
                double high   = getDouble(entry, "high");
                double low    = getDouble(entry, "low");
                double bid    = getDouble(entry, "bid");
                double ask    = getDouble(entry, "ask");

                @SuppressWarnings("unchecked")
                List<Double> changes = ((List<String>) getValue(entry, "changes"))
                        .stream()
                        .map(Double::parseDouble)
                        .toList();

                double spread    = ask - bid;
                double mid       = (bid + ask) / 2.0;
                double spreadBps = mid > 0 ? (spread / mid) * 10_000 : 0;

                double mean   = changes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double stdDev = Math.sqrt(changes.stream()
                        .mapToDouble(v -> Math.pow(v - mean, 2))
                        .average().orElse(0));

                double first    = changes.isEmpty() ? open : changes.getFirst();
                double last     = changes.isEmpty() ? close : changes.getLast();
                double momentum = first > 0 ? ((last - first) / first) * 100 : 0;

                return new Accumulator(
                        count + 1,
                        sumOpen + open, sumClose + close,
                        Math.max(maxHigh, high), Math.min(minLow, low),
                        sumSpread + spread, sumSpreadBps + spreadBps,
                        sumVolatility + stdDev, sumMomentum + momentum
                );
            }

            Accumulator merge(Accumulator other) {
                return new Accumulator(
                        count + other.count(),
                        sumOpen + other.sumOpen(), sumClose + other.sumClose(),
                        Math.max(maxHigh, other.maxHigh()), Math.min(minLow, other.minLow()),
                        sumSpread + other.sumSpread(), sumSpreadBps + other.sumSpreadBps(),
                        sumVolatility + other.sumVolatility(), sumMomentum + other.sumMomentum()
                );
            }

            MarketSummary toSummary() {
                return new MarketSummary(
                        count,
                        sumOpen       / count,
                        sumClose      / count,
                        maxHigh,
                        minLow,
                        sumSpread     / count,
                        sumSpreadBps  / count,
                        sumVolatility / count,
                        sumMomentum   / count
                );
            }
        }

        Accumulator acc = records.reduce(
                Accumulator.empty(),
                Accumulator::combine,
                Accumulator::merge
        );

        return acc.count() == 0 ? Stream.empty() : Stream.of(acc.toSummary());
    }

    private static double getDouble(Set<Map.Entry<String, Object>> entries, String key) {
        return Double.parseDouble(getValue(entries, key).toString());
    }

    private static Object getValue(Set<Map.Entry<String, Object>> entries, String key) {
        return entries.stream()
                .filter(e -> e.getKey().equals(key))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(() -> new IllegalArgumentException("Missing key: " + key));
    }
}