package software.spool.mounter.api.port;

import software.spool.mounter.api.model.GenericRecord;

import java.util.stream.Stream;

public interface PartitionedReader {
    Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget);
}
