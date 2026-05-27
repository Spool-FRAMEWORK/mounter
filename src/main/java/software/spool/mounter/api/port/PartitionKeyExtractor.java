package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;

@FunctionalInterface
public interface PartitionKeyExtractor<O> {
    PartitionKey extract(GenericRecord input, O output, MountTarget target);
}