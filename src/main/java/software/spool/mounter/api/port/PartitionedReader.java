package software.spool.mounter.api.port;

import software.spool.mounter.api.model.GenericRecord;

import java.util.List;

public interface PartitionedReader {
    List<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget);
}
