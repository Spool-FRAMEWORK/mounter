package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.PartitionWindowPolicy;

public class AlwaysClosedWindowPolicy implements PartitionWindowPolicy {
    public boolean isClosed(PartitionKey partitionKey) { return true; }
}