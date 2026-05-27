package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;

import java.util.List;
import java.util.Set;

public interface IncrementalDataMartReader<O> extends DataMartReader {
    List<PartitionKey> pendingPartitions(PartitionKey scope, Set<PartitionKey> alreadyProcessed);
    O readCurrent(PartitionKey targetKey);
}