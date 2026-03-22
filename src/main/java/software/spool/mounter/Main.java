package software.spool.mounter;

import software.spool.core.adapter.InMemoryEventBus;
import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.adapter.InMemoryDataLakeReader;
import software.spool.mounter.api.adapter.InMemoryDataMartWriter;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.builder.MounterBuilderFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<PartitionKey, List<Object>> store = new HashMap<>();
        MounterBuilderFactory.reactive(new InMemoryEventBus())
                .readingWith(new InMemoryDataLakeReader<>(store))
                .writingWith(new InMemoryDataMartWriter<>())
                .aggregatingWith(new PassThroughMountAggregator<>())
                .build();
    }
}
