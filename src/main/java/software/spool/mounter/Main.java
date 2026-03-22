package software.spool.mounter;

import software.spool.core.adapter.InMemoryEventBus;
import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.InMemoryDataLakeReader;
import software.spool.mounter.api.adapter.InMemoryDataMartWriter;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.utils.PollingPolicy;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<PartitionKey, List<Object>> store = new HashMap<>();

        Mounter mounter = MounterBuilderFactory.polling(new InMemoryDataLakeReader<>(store))
                .onPartition(new PartitionKey("gemini"))
                .aggregatingWith(new PassThroughMountAggregator<>())
                .writingWith(new InMemoryDataMartWriter<>())
                .pollingWith(PollingPolicy.every(Duration.ofSeconds(10)))
                .emittingWith(new InMemoryEventBus())
                .build();

        mounter.startMounting();
    }
}
