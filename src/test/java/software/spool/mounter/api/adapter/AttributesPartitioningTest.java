package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AttributesPartitioningTest {

    private static final PartitionKey SOURCE = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();

    record Amount(String currency, long cents) {}

    record Payment(String country, Amount amount) {}

    @Test
    void of_genericRecordWithNestedAttributes_buildsThePartitionFromTheirValuesInOrder() {
        GenericRecord payment = GenericRecord.of(Map.of("payload", Map.of("country", "ES", "day", "2026-11-21")));

        PartitionKey partition = AttributesPartitioning.<GenericRecord>of("payload.country", "payload.day")
                .of(new PartitionedRecord<>(SOURCE, payment));

        assertThat(partition).isEqualTo(PartitionKey.ofEntriesWithoutDate()
                .with("payload.country", "ES").with("payload.day", "2026-11-21").build());
    }

    @Test
    void of_javaRecordWithNestedAttributes_buildsThePartitionFromTheirValuesInOrder() {
        Payment payment = new Payment("ES", new Amount("EUR", 1000));

        PartitionKey partition = AttributesPartitioning.<Payment>of("country", "amount.currency")
                .of(new PartitionedRecord<>(SOURCE, payment));

        assertThat(partition).isEqualTo(PartitionKey.ofEntriesWithoutDate()
                .with("country", "ES").with("amount.currency", "EUR").build());
    }

    @Test
    void of_genericRecordWithoutTheAttribute_failsNamingTheAttribute() {
        GenericRecord payment = GenericRecord.of(Map.of("payload", Map.of("day", "2026-11-21")));

        assertThatThrownBy(() -> AttributesPartitioning.<GenericRecord>of("payload.country")
                .of(new PartitionedRecord<>(SOURCE, payment)))
                .isInstanceOfSatisfying(MissingPartitionAttributeException.class,
                        exception -> assertThat(exception.attribute()).isEqualTo("payload.country"))
                .hasMessageContaining("payload.country");
    }

    @Test
    void of_genericRecordWhoseAttributeIsNull_failsNamingTheAttribute() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("country", null);
        GenericRecord payment = GenericRecord.of(Map.of("payload", payload));

        assertThatThrownBy(() -> AttributesPartitioning.<GenericRecord>of("payload.country")
                .of(new PartitionedRecord<>(SOURCE, payment)))
                .isInstanceOfSatisfying(MissingPartitionAttributeException.class,
                        exception -> assertThat(exception.attribute()).isEqualTo("payload.country"));
    }

    @Test
    void of_javaRecordWithoutTheAttribute_failsNamingTheAttribute() {
        Payment payment = new Payment("ES", new Amount("EUR", 1000));

        assertThatThrownBy(() -> AttributesPartitioning.<Payment>of("amount.day")
                .of(new PartitionedRecord<>(SOURCE, payment)))
                .isInstanceOfSatisfying(MissingPartitionAttributeException.class,
                        exception -> assertThat(exception.attribute()).isEqualTo("amount.day"));
    }

    @Test
    void of_withoutAttributes_isRejected() {
        assertThatThrownBy(() -> AttributesPartitioning.of()).isInstanceOf(IllegalArgumentException.class);
    }
}
