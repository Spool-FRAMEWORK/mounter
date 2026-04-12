package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public interface PartitionWindowPolicy {
    boolean isClosed(PartitionKey partitionKey);
}
