package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.PartitionedRecord;

public class SameAsSourcePartitioning<O> implements MountPartitioning<O> {
    @Override
    public PartitionKey of(PartitionedRecord<? extends O> result) {
        return result.partitionKey();
    }
}
