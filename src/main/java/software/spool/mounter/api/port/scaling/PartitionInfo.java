package software.spool.mounter.api.port.scaling;

import software.spool.core.model.vo.PartitionKey;

import java.util.OptionalLong;

public record PartitionInfo(PartitionKey key, OptionalLong estimatedRecords, OptionalLong estimatedBytes) {
    public static PartitionInfo of(PartitionKey key) {
        return new PartitionInfo(key, OptionalLong.empty(), OptionalLong.empty());
    }

    public static PartitionInfo of(PartitionKey key, long records, long bytes) {
        return new PartitionInfo(key, OptionalLong.of(records), OptionalLong.of(bytes));
    }
}
