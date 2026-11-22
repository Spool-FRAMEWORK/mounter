package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.PartitionPeriod;
import software.spool.mounter.api.port.PartitionWindowPolicy;

import java.time.Clock;
import java.time.Duration;

/**
 * Closes a partition once its last date entry has ended and a margin has passed, so records that arrive late to it
 * are still read. A partition without a date is always closed.
 */
public class DatedPartitionWindowPolicy implements PartitionWindowPolicy {
    private final Clock clock;
    private final Duration margin;

    public DatedPartitionWindowPolicy(Clock clock, Duration margin) {
        if (margin.isNegative()) {
            throw new IllegalArgumentException("Closing margin must not be negative: " + margin);
        }
        this.clock = clock;
        this.margin = margin;
    }

    @Override
    public boolean isClosed(PartitionKey partitionKey) {
        return PartitionPeriod.of(partitionKey)
                .map(period -> !clock.instant().isBefore(period.end().plus(margin)))
                .orElse(true);
    }
}
