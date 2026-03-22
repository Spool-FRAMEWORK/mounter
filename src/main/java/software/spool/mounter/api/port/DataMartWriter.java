package software.spool.mounter.api.port;

import software.spool.core.model.PartitionKey;

import java.util.stream.Stream;

public interface DataMartWriter<R> {
    void write(PartitionKey partitionKey, Stream<R> result);
}
