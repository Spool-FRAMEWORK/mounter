package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public record PartitionSlice(PartitionKey key, long offset, long limit) {}
