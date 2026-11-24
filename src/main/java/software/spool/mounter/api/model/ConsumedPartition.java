package software.spool.mounter.api.model;

import software.spool.core.model.vo.PartitionKey;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * A partition of silver a data mart has consumed, with how many files it had when it was read (ADR-037). The number
 * is unknown for a partition read by a discovery that does not count files, or before the number was kept.
 *
 * @param partition the partition of silver
 * @param files     how many files it had when it was read, if known
 */
public record ConsumedPartition(PartitionKey partition, OptionalLong files) {
    public ConsumedPartition {
        Objects.requireNonNull(partition, "Consumed partition without partition");
        Objects.requireNonNull(files, "Consumed partition without its number of files");
    }

    public static ConsumedPartition of(PartitionKey partition, OptionalLong files) {
        return new ConsumedPartition(partition, files);
    }

    public static ConsumedPartition withoutFiles(PartitionKey partition) {
        return new ConsumedPartition(partition, OptionalLong.empty());
    }

    /** Whether the partition has now a different number of files than when it was read, when both are known. */
    public boolean hasChanged(OptionalLong filesNow) {
        return files.isPresent() && filesNow.isPresent() && files.getAsLong() != filesNow.getAsLong();
    }
}
