package software.spool.mounter.api.adapter;

import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.port.DataLakeReader;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryDataLakeReader<I> implements DataLakeReader<I> {
    private final Map<PartitionKey, List<I>> store;

    public InMemoryDataLakeReader(Map<PartitionKey, List<I>> store) {
        this.store = store;
    }

    @Override
    public Stream<I> read(PartitionKey partitionKey) {
        return store.getOrDefault(partitionKey, List.of()).stream();
    }
}
