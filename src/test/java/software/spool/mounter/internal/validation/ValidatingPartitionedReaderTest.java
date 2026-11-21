package software.spool.mounter.internal.validation;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.memory.InMemoryQuarantineRepository;
import software.spool.core.exception.BronzeRemovalException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantineStatus;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.mounter.api.fixture.validation.PositivePriceValidator;
import software.spool.mounter.api.fixture.validation.SkuNotBlankValidator;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatingPartitionedReaderTest {

    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "products").build();
    private static final MountTarget TARGET = MountTarget.transformation("products", PARTITION);

    private final InMemoryQuarantineRepository repository = new InMemoryQuarantineRepository();
    private final Map<IdempotencyKey, PartitionedRecord<GenericRecord>> bronzeFiles = new LinkedHashMap<>();
    private final List<IdempotencyKey> failingDeletes = new ArrayList<>();
    private final List<Event> published = new ArrayList<>();
    private final List<Map<String, String>> rejected = new ArrayList<>();

    @Test
    void read_batchWithAValidAndAnInvalidRecord_passesOnlyTheValidOne() {
        List<PartitionedRecord<GenericRecord>> read = readerOver(bronze("k1", "{\"sku\":\"A-1\",\"price\":10}"),
                bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        assertThat(read).extracting(record -> record.record().getNested("idempotencyKey").getString("value")).containsExactly("k1");
    }

    @Test
    void read_recordsWithTheirPayloadAlreadyDecoded_passesTheValidOneAndQuarantinesTheOther() {
        List<PartitionedRecord<GenericRecord>> read = readerOver(decodedBronze("k1", "{\"sku\":\"A-1\",\"price\":10}"),
                decodedBronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        assertThat(read).extracting(record -> record.record().getNested("idempotencyKey").getString("value")).containsExactly("k1");
        assertThat(repository.find(IdempotencyKey.of("k2"))).isPresent();
    }

    @Test
    void read_recordThatFailsACriticalValidator_keepsItsQuarantineRecord() {
        readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        QuarantinedRecord quarantinedRecord = repository.find(IdempotencyKey.of("k2")).orElseThrow();
        assertThat(quarantinedRecord.envelope().payload()).isEqualTo("{\"sku\":\"\",\"price\":10}".getBytes(StandardCharsets.UTF_8));
        assertThat(quarantinedRecord.envelope().metadata().get(EventMetadataKey.SOURCE)).isEqualTo("products");
        assertThat(quarantinedRecord.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
        assertThat(quarantinedRecord.violations()).anyMatch(violation -> violation.contains("SKU must not be blank"));
    }

    @Test
    void read_recordThatFailsACriticalValidator_recordsThatItStayedInItsBronzePartition() {
        readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        QuarantinedRecord quarantinedRecord = repository.find(IdempotencyKey.of("k2")).orElseThrow();
        assertThat(quarantinedRecord.location()).isEqualTo(QuarantineLocation.bronze(PARTITION));
        assertThat(quarantinedRecord.status()).isEqualTo(QuarantineStatus.PENDING);
    }

    @Test
    void read_recordThatFailsACriticalValidator_publishesEnvelopeQuarantinedSayingItStayedInBronze() {
        readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        assertThat(published).singleElement().isInstanceOfSatisfying(EnvelopeQuarantined.class, event -> {
            assertThat(event.idempotencyKey()).isEqualTo(IdempotencyKey.of("k2"));
            assertThat(event.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
            assertThat(event.violations()).anyMatch(violation -> violation.contains("SKU must not be blank"));
            assertThat(event.location()).isEqualTo(QuarantineLocation.bronze(PARTITION));
        });
    }

    @Test
    void read_recordThatFailsACriticalValidator_removesItFromBronze() {
        readerOver(bronze("k1", "{\"sku\":\"A-1\",\"price\":10}"), bronze("k2", "{\"sku\":\"\",\"price\":10}"))
                .read(TARGET).toList();

        assertThat(bronzeFiles.keySet()).containsExactly(IdempotencyKey.of("k1"));
    }

    @Test
    void read_whenRemovingFromBronzeFails_theQuarantineRecordIsAlreadyKept() {
        failingDeletes.add(IdempotencyKey.of("k2"));

        List<PartitionedRecord<GenericRecord>> read = readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        assertThat(read).isEmpty();
        assertThat(repository.find(IdempotencyKey.of("k2"))).isPresent();
    }

    @Test
    void read_samePartitionTwice_quarantinesTheRecordOnlyOnce() {
        PartitionedReader reader = readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}"));

        reader.read(TARGET).toList();
        reader.read(TARGET).toList();

        assertThat(published).hasSize(1);
        assertThat(repository.find(IdempotencyKey.of("k2")).orElseThrow().history()).hasSize(1);
    }

    @Test
    void read_recordThatFailsACriticalValidator_countsAsRejectedByTheMounter() {
        readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}")).read(TARGET).toList();

        assertThat(rejected).singleElement()
                .satisfies(attributes -> assertThat(attributes).containsEntry(SpoolMetrics.Attributes.MODULE, "mounter"));
    }

    @Test
    void read_recordThatOnlyFailsAWarning_passesAndIsNotQuarantined() {
        List<PartitionedRecord<GenericRecord>> read = readerOver(bronze("k3", "{\"sku\":\"A-3\",\"price\":-1}"))
                .read(TARGET).toList();

        assertThat(read).hasSize(1);
        assertThat(repository.findAll()).isEmpty();
        assertThat(published).isEmpty();
    }

    @Test
    void read_aggregationTarget_passesEveryRecordWithoutValidating() {
        MountTarget aggregation = MountTarget.aggregation("products", PARTITION);

        List<PartitionedRecord<GenericRecord>> read = readerOver(bronze("k2", "{\"sku\":\"\",\"price\":10}"))
                .read(aggregation).toList();

        assertThat(read).hasSize(1);
        assertThat(repository.findAll()).isEmpty();
    }

    private PartitionedReader readerOver(PartitionedRecord<GenericRecord>... records) {
        for (PartitionedRecord<GenericRecord> bronzeRecord : records) {
            bronzeFiles.put(IdempotencyKey.of(bronzeRecord.record().getNested("idempotencyKey").getString("value")), bronzeRecord);
        }
        PartitionedReader bronze = target -> List.copyOf(bronzeFiles.values()).stream();
        BronzeEnvelopeRemover bronzeEnvelopeRemover = (partition, idempotencyKey) -> {
            if (failingDeletes.contains(idempotencyKey)) throw new BronzeRemovalException("disk full", new IOException());
            if (partition.equals(PARTITION)) bronzeFiles.remove(idempotencyKey);
        };
        return new ValidatingPartitionedReader(bronze,
                new EnvelopeValidator(new ValidatorRegistry(List.of(new SkuNotBlankValidator(), new PositivePriceValidator()))),
                repository,
                bronzeEnvelopeRemover,
                new EventPublisher() {
                    @Override
                    public <E extends Event> void publish(E event) {
                        published.add(event);
                    }
                },
                (value, attributes) -> rejected.add(attributes));
    }

    private static PartitionedRecord<GenericRecord> bronze(String key, String payload) {
        Envelope envelope = new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, "products"),
                MediaType.of("application/json"), payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.PERSISTED, 0,
                Instant.now(), Instant.now());
        Map<String, Object> fields = PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope));
        return new PartitionedRecord<>(PARTITION, GenericRecord.of(fields));
    }

    private static PartitionedRecord<GenericRecord> decodedBronze(String key, String payload) {
        Map<String, Object> fields = new LinkedHashMap<>(bronze(key, payload).record().toMap());
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(payload.getBytes(StandardCharsets.UTF_8)));
        return new PartitionedRecord<>(PARTITION, GenericRecord.of(fields));
    }
}
