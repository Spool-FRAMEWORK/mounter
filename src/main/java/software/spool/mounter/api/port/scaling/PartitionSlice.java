package software.spool.mounter.api.port.scaling;

import software.spool.core.model.vo.PartitionKey;

public record PartitionSlice(PartitionKey key, long offset, long limit) {}
