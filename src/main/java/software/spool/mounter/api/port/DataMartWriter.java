package software.spool.mounter.api.port;

import java.util.stream.Stream;

public interface DataMartWriter<O> {
    void write(MountTarget target, Stream<PartitionedRecord<O>> result);
}
