package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public record PartitionedRecord<I>(PartitionKey partitionKey, I record) {}