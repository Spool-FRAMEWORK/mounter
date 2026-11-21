package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.memory.InMemoryQuarantineRepository;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.AttributesPartitioning;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.adapter.SameAsSourcePartitioning;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.quarantine.QuarantiningMissingPartitionAttributeHandler;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MountUnitHandlerTest {

    private static final PartitionKey FIRST_PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "payments").with("hour", "10").build();
    private static final PartitionKey SECOND_PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "payments").with("hour", "11").build();
    private static final PartitionKey SCOPE = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();
    private static final MountTarget SILVER = MountTarget.transformation("payments", SCOPE);
    private static final MountTarget GOLD = MountTarget.aggregation("daily-totals", SCOPE);

    record DailyTotal(String day, long cents) {}

    private final Map<IdempotencyKey, PartitionedRecord<GenericRecord>> bronzeFiles = new LinkedHashMap<>();
    private final List<PartitionedRecord<?>> written = new ArrayList<>();
    private final InMemoryQuarantineRepository quarantineRepository = new InMemoryQuarantineRepository();
    private final List<Event> published = new ArrayList<>();
    private final List<Exception> routed = new ArrayList<>();
    private final ErrorRouter errorRouter = new ErrorRouter().orElse((exception, context) -> routed.add(exception));

    @Test
    void handle_withoutAggregator_writesEachRecordInThePartitionItWasReadFrom() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"), bronze(SECOND_PARTITION, "k2", "{\"country\":\"FR\"}"));

        handlerWith(new PassThroughMountAggregator(), new SameAsSourcePartitioning<>()).handle(SILVER);

        assertThat(writtenPartitionsByKey()).containsExactly(Map.entry("k1", FIRST_PARTITION), Map.entry("k2", SECOND_PARTITION));
    }

    @Test
    void handle_withAnAggregator_writesEachResultInThePartitionTheAggregatorGaveIt() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"), bronze(SECOND_PARTITION, "k2", "{\"country\":\"FR\"}"));
        PartitionKey day = PartitionKey.ofEntriesWithoutDate().with("day", "2026-11-21").build();
        MountAggregator<DailyTotal> dailyTotals = records -> Stream.of(new PartitionedRecord<>(day, new DailyTotal("2026-11-21", records.count())));

        handlerWith(dailyTotals, new SameAsSourcePartitioning<>()).handle(GOLD);

        assertThat(written).singleElement().satisfies(result -> {
            assertThat(result.partitionKey()).isEqualTo(day);
            assertThat(result.record()).isEqualTo(new DailyTotal("2026-11-21", 2));
        });
    }

    @Test
    void handle_partitioningByAttributes_writesEachRecordInThePartitionBuiltFromIt() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"));

        handlerWith(new PassThroughMountAggregator(), AttributesPartitioning.of("payload.country")).handle(SILVER);

        assertThat(written).singleElement().satisfies(result -> assertThat(result.partitionKey())
                .isEqualTo(PartitionKey.ofEntriesWithoutDate().with("payload.country", "ES").build()));
    }

    @Test
    void handle_silverRecordWithoutAPartitionAttribute_writesTheOthersAndQuarantinesIt() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"), bronze(FIRST_PARTITION, "k2", "{\"day\":\"2026-11-21\"}"));

        handlerWith(new PassThroughMountAggregator(), AttributesPartitioning.of("payload.country")).handle(SILVER);

        assertThat(writtenPartitionsByKey()).containsOnlyKeys("k1");
        QuarantinedRecord quarantined = quarantineRepository.find(IdempotencyKey.of("k2")).orElseThrow();
        assertThat(quarantined.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
        assertThat(quarantined.violations()).anyMatch(violation -> violation.contains("payload.country"));
        assertThat(quarantined.location()).isEqualTo(QuarantineLocation.bronze(FIRST_PARTITION));
        assertThat(routed).isEmpty();
    }

    @Test
    void handle_silverRecordWithoutAPartitionAttribute_removesItFromTheDataLakeAndPublishesThatItIsQuarantined() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"), bronze(FIRST_PARTITION, "k2", "{\"day\":\"2026-11-21\"}"));

        handlerWith(new PassThroughMountAggregator(), AttributesPartitioning.of("payload.country")).handle(SILVER);

        assertThat(bronzeFiles.keySet()).containsExactly(IdempotencyKey.of("k1"));
        assertThat(published).singleElement().isInstanceOfSatisfying(EnvelopeQuarantined.class, event -> {
            assertThat(event.idempotencyKey()).isEqualTo(IdempotencyKey.of("k2"));
            assertThat(event.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
        });
    }

    @Test
    void handle_silverResultThatIsNoLongerTheEnvelopeAndLacksAPartitionAttribute_routesItsErrorAndLeavesItInTheDataLake() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"));
        MountAggregator<DailyTotal> transformed = records -> records.map(read -> new PartitionedRecord<>(read.partitionKey(), new DailyTotal(null, 1)));

        handlerWith(transformed, AttributesPartitioning.of("day")).handle(SILVER);

        assertThat(written).isEmpty();
        assertThat(routed).singleElement().isInstanceOf(MissingPartitionAttributeException.class);
        assertThat(quarantineRepository.findAll()).isEmpty();
        assertThat(bronzeFiles.keySet()).containsExactly(IdempotencyKey.of("k1"));
    }

    @Test
    void handle_goldResultWithoutAPartitionAttribute_writesTheOthersAndRoutesItsErrorNamingTheAttribute() {
        stored(bronze(FIRST_PARTITION, "k1", "{\"country\":\"ES\"}"));
        MountAggregator<DailyTotal> dailyTotals = records -> Stream.of(
                new PartitionedRecord<>(SCOPE, new DailyTotal("2026-11-21", 1)),
                new PartitionedRecord<>(SCOPE, new DailyTotal(null, 1)));

        handlerWith(dailyTotals, AttributesPartitioning.of("day")).handle(GOLD);

        assertThat(written).singleElement().satisfies(result -> assertThat(result.record()).isEqualTo(new DailyTotal("2026-11-21", 1)));
        assertThat(routed).singleElement().isInstanceOfSatisfying(MissingPartitionAttributeException.class,
                exception -> assertThat(exception.attribute()).isEqualTo("day"));
        assertThat(quarantineRepository.findAll()).isEmpty();
    }

    private <O> MountUnitHandler<O> handlerWith(MountAggregator<O> aggregator, MountPartitioning<? super O> partitioning) {
        PartitionedReader reader = target -> List.copyOf(bronzeFiles.values()).stream();
        DataLakeEnvelopeRemover remover = (partition, idempotencyKey) -> {
            if (bronzeFiles.get(idempotencyKey).partitionKey().equals(partition)) bronzeFiles.remove(idempotencyKey);
        };
        EventPublisher publisher = new EventPublisher() {
            @Override
            public <E extends Event> void publish(E event) {
                published.add(event);
            }
        };
        RoutingMissingPartitionAttributeHandler routing = new RoutingMissingPartitionAttributeHandler(errorRouter);
        DataLakeRecordQuarantine quarantine = new DataLakeRecordQuarantine(quarantineRepository, remover, publisher, (value, attributes) -> {});
        return new MountUnitHandler<>(reader, aggregator, (target, results) -> results.forEach(written::add),
                new AlwaysClosedWindowPolicy(), new NoOpMountCheckpoint(), partitioning,
                new QuarantiningMissingPartitionAttributeHandler(quarantine, routing));
    }

    private Map<String, PartitionKey> writtenPartitionsByKey() {
        return written.stream().collect(Collectors.toMap(
                result -> ((GenericRecord) result.record()).getNested("idempotencyKey").getString("value"),
                PartitionedRecord::partitionKey, (first, second) -> first, LinkedHashMap::new));
    }

    @SafeVarargs
    private void stored(PartitionedRecord<GenericRecord>... records) {
        for (PartitionedRecord<GenericRecord> bronzeRecord : records) {
            bronzeFiles.put(IdempotencyKey.of(bronzeRecord.record().getNested("idempotencyKey").getString("value")), bronzeRecord);
        }
    }

    private static PartitionedRecord<GenericRecord> bronze(PartitionKey partition, String key, String payload) {
        Envelope envelope = new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.PERSISTED, 0,
                Instant.now(), Instant.now());
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope)));
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(payload.getBytes(StandardCharsets.UTF_8)));
        return new PartitionedRecord<>(partition, GenericRecord.of(fields));
    }
}
