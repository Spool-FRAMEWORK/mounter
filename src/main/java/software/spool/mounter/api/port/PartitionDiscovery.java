package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

import java.util.List;

public interface PartitionDiscovery {
    List<PartitionInfo> discover(PartitionKey scope);
}
