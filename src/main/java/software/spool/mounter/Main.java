package software.spool.mounter;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.port.MountPartitionSchema;
import software.spool.mounter.api.port.MountTarget;

public class Main {
    public static void main(String[] args) {
        MounterBuilderFactory.polling(new S3DatalakeReader(buildS3Client(), "spool-datalake", ""))
                .aggregatingWith(new SECSharesTrendAggregator())
                .pollingWith(PollingPolicy.ONCE)
                .checkpoint(new NoOpMountCheckpoint())
                .onTarget(MountTarget.aggregation("sec-shares", new PartitionKey("year=2026")))
                .partitioningWith(MountPartitionSchema.of(SECShareTrendStats.class, "entityName"))
                .partitionWindowPolicy(new AlwaysClosedWindowPolicy())
                .writingWith(new S3DataMartWriter(buildS3Client(), "spool-datalake", ""))
                .emittingWith(TraceEventBusEmitter.of(new KafkaEventBusEmitter(new KafkaEventBusConfig("host.docker.internal:9092"))).with(new OpenTelemetryTracedEventBus()))
                .build();
    }
}