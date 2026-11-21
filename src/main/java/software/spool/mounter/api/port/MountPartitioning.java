package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

/**
 * Decides the partition a mount result is written to, looking only at the result.
 */
@FunctionalInterface
public interface MountPartitioning<O> {
    PartitionKey of(PartitionedRecord<? extends O> result);
}
