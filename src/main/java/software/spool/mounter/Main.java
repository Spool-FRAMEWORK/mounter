package software.spool.mounter;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.otel.OTELConfig;
import software.spool.core.model.spool.SpoolModule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.InMemoryDataLakeReader;
import software.spool.mounter.api.adapter.MarketSummaryAggregator;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountTarget;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException {
        OTELConfig.init("mounter");
        String pk = "year=2026_month=04_day=12_gemini-market_symbol=BTCEUR";
        PartitionKey partitionKey = new PartitionKey("year=2026_month=04_day=12_gemini-market_symbol=BTCEUR");
        InMemoryDataLakeReader<Set<Map.Entry<String, Object>>> reader = new InMemoryDataLakeReader<>(
                path -> {
                    try {
                        return Files.lines(path)
                                .map(line -> PayloadDeserializerFactory.json().asMap().deserialize(line).entrySet());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
        );

        Mounter mounter = MounterBuilderFactory.polling(reader)
                .aggregatingWith(new MarketSummaryAggregator())
                .onTarget(MountTarget.of("Test", partitionKey))
                .pollingWith(PollingPolicy.ONCE)
                .partitionWindowPolicy(new AlwaysClosedWindowPolicy())
                .checkpoint(new NoOpMountCheckpoint())
                .writingWith((t, r) -> System.out.println(t + ": " + r.toList()))
                .emittingWith(System.out::println)
                .build();

        SpoolNode.create()
                .register(mounter)
                .start();
    }
}
