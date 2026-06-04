package software.spool.mounter.api.port;

import java.util.stream.Stream;

public interface DataMartWriter {
    void write(MountTarget target, Stream<PartitionedRecord<?>> result);
}
