package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

@FunctionalInterface
public interface PartitionKeyExtractor<O> {
    PartitionKey extract(O payload);
}