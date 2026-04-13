package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

import java.util.List;
import java.util.Set;

public interface IncrementalDataMartReader<I, O> extends DataMartReader<I> {
    List<PartitionKey> pendingPartitions(PartitionKey scope, Set<PartitionKey> alreadyProcessed);
    O readCurrent(PartitionKey targetKey);
}