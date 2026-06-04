package software.spool.mounter.api.port;

import software.spool.mounter.api.model.GenericRecord;

import java.util.stream.Stream;

public interface StreamingPartitionedReader extends PartitionedReader {
    Stream<PartitionedRecord<GenericRecord>> stream(MountTarget mountTarget);
}
