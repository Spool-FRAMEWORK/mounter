package software.spool.mounter.api.port;

import software.spool.core.model.PartitionKey;

import java.util.stream.Stream;

public interface DataLakeReader<I> {
    Stream<I> read(PartitionKey partitionKey);
}
