package software.spool.mounter.api.adapter;

import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.port.DataMartWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryDataMartWriter<O> implements DataMartWriter<O> {
    private final Map<PartitionKey, List<O>> store = new HashMap<>();

    @Override
    public void write(PartitionKey partitionKey, Stream<O> result) {
        store.put(partitionKey, result.toList());
    }

    public List<O> getWritten(PartitionKey partitionKey) {
        return store.getOrDefault(partitionKey, List.of());
    }

    public Map<PartitionKey, List<O>> getAll() {
        return store;
    }
}
