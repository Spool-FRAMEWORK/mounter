package software.spool.mounter.internal.quarantine;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.mounter.api.model.GenericRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataLakeRecordQuarantineTest {
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    @Test
    void envelopeOf_recordWithItsPayloadInBase64_givesTheEnvelopeBack() {
        Envelope envelope = envelope(MediaType.of("application/json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));

        assertThat(DataLakeRecordQuarantine.envelopeOf(recordWithPayload(envelope, null)).payload()).isEqualTo(envelope.payload());
    }

    @Test
    void envelopeOf_recordWithItsJsonPayloadAsAnObject_givesTheEnvelopeBack() {
        Envelope envelope = envelope(MediaType.of("application/json"), "{\"id\":1}".getBytes(StandardCharsets.UTF_8));

        Envelope rebuilt = DataLakeRecordQuarantine.envelopeOf(recordWithPayload(envelope, Map.of("id", 1)));

        assertThat(new String(rebuilt.payload(), StandardCharsets.UTF_8)).isEqualTo("{\"id\":1}");
    }

    @Test
    void envelopeOf_recordWithABinaryPayload_givesTheEnvelopeWithTheSameBytes() {
        byte[] pdf = {0x25, 0x50, 0x44, 0x46, (byte) 0xe2, (byte) 0xe3};
        Envelope envelope = envelope(MediaType.of("application/pdf"), pdf);

        Envelope rebuilt = DataLakeRecordQuarantine.envelopeOf(recordWithPayload(envelope, pdf));

        assertThat(rebuilt.payload()).isEqualTo(pdf);
        assertThat(rebuilt.idempotencyKey()).isEqualTo(envelope.idempotencyKey());
    }

    private static GenericRecord recordWithPayload(Envelope envelope, Object payload) {
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope)));
        if (payload != null) fields.put("payload", payload);
        return GenericRecord.of(fields);
    }

    private static Envelope envelope(MediaType mediaType, byte[] payload) {
        return new Envelope(IdempotencyKey.of("k1"), new EventMetadata().set(EventMetadataKey.SOURCE, "boe"), mediaType,
                payload, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}
