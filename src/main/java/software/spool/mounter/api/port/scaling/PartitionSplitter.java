package software.spool.mounter.api.port.scaling;

import java.util.List;

public interface PartitionSplitter {
    List<PartitionSlice> split(PartitionInfo partition, SplitHint hint);
}
