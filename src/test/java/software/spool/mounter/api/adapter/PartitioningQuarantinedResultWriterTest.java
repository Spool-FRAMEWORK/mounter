package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.exception.QuarantinedResultRejectedException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitioningQuarantinedResultWriterTest {

    private static final PartitionKey SCOPE = PartitionKey.ofEntriesWithoutDate().with("source", "orders").build();
    private static final PartitionKey DAY = PartitionKey.ofEntriesWithoutDate().with("day", "2026-11-21").build();
    private static final MountTarget GOLD = MountTarget.aggregation("payments", SCOPE);

    private final List<PartitionedRecord<?>> written = new ArrayList<>();

    @Test
    void dataMart_isTheTargetDataMartWithItsLayer() {
        assertThat(writerPartitionedBy(AttributesPartitioning.of("country")).dataMart()).isEqualTo("gold/payments");
    }

    @Test
    void write_resultWithItsPartitionAttribute_writesItInThePartitionBuiltFromIt() {
        writerPartitionedBy(AttributesPartitioning.of("country")).write(DAY, json("{\"day\":\"2026-11-21\",\"country\":\"FR\",\"amount\":7}"));

        assertThat(written).singleElement().satisfies(result -> {
            assertThat(result.partitionKey()).isEqualTo(PartitionKey.ofEntriesWithoutDate().with("country", "FR").build());
            assertThat(((GenericRecord) result.record()).toMap()).containsEntry("country", "FR").containsEntry("amount", 7);
        });
    }

    @Test
    void write_partitionedAsItsSource_writesItInThePartitionItCameWith() {
        writerPartitionedBy(new SameAsSourcePartitioning<>()).write(DAY, json("{\"day\":\"2026-11-21\",\"amount\":7}"));

        assertThat(written).singleElement().satisfies(result -> assertThat(result.partitionKey()).isEqualTo(DAY));
    }

    @Test
    void write_resultStillWithoutItsPartitionAttribute_isRejectedNamingTheAttributeAndWritesNothing() {
        PartitioningQuarantinedResultWriter writer = writerPartitionedBy(AttributesPartitioning.of("country"));

        assertThatThrownBy(() -> writer.write(DAY, json("{\"day\":\"2026-11-21\",\"amount\":7}")))
                .isInstanceOf(QuarantinedResultRejectedException.class)
                .hasMessageContaining("country");
        assertThat(written).isEmpty();
    }

    private PartitioningQuarantinedResultWriter writerPartitionedBy(MountPartitioning<?> partitioning) {
        return new PartitioningQuarantinedResultWriter(GOLD, partitioning, (target, results) -> results.forEach(written::add));
    }

    private static byte[] json(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }
}
