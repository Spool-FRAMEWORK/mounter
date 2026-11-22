package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CombinedPartitionsReaderTest {
    private static final PartitionKey PAYMENTS = new PartitionKey("source=payments");
    private static final PartitionKey PAYMENT_EVENTS = new PartitionKey("source=payments::eventType=Payment");
    private static final PartitionKey REFUNDS = new PartitionKey("source=refunds");
    private static final PartitionKey PAYMENT_FOLDER = new PartitionKey("year=2026::source=payments::eventType=Payment");
    private static final PartitionKey CHARGEBACK_FOLDER = new PartitionKey("year=2026::source=payments::eventType=Chargeback");
    private static final PartitionKey REFUND_FOLDER = new PartitionKey("year=2026::source=refunds::eventType=Refund");

    private final PartitionedReader lake = target -> Map.of(
            PAYMENTS, List.of(record(PAYMENT_FOLDER, "payment-1"), record(PAYMENT_FOLDER, "payment-2"),
                    record(CHARGEBACK_FOLDER, "chargeback-1")),
            PAYMENT_EVENTS, List.of(record(PAYMENT_FOLDER, "payment-1"), record(PAYMENT_FOLDER, "payment-2")),
            REFUNDS, List.of(record(REFUND_FOLDER, "refund-1"))
    ).get(target.sourceKey()).stream();

    @Test
    void read_severalPartitions_givesTheRecordsOfEveryOne() {
        List<String> read = idsReadFrom(List.of(PAYMENT_EVENTS, REFUNDS));

        assertThat(read).containsExactly("payment-1", "payment-2", "refund-1");
    }

    @Test
    void read_partitionsThatOverlap_givesEachRecordOnce() {
        List<String> read = idsReadFrom(List.of(PAYMENTS, PAYMENT_EVENTS));

        assertThat(read).containsExactly("payment-1", "payment-2", "chargeback-1");
    }

    @Test
    void read_partitionThatOverlapsAnEarlierOne_stillGivesItsOtherRecords() {
        List<String> read = idsReadFrom(List.of(PAYMENT_EVENTS, PAYMENTS));

        assertThat(read).containsExactly("payment-1", "payment-2", "chargeback-1");
    }

    @Test
    void of_noPartitions_isRejected() {
        assertThatThrownBy(() -> new CombinedPartitionsReader(lake, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A combined read needs at least one partition");
    }

    private List<String> idsReadFrom(List<PartitionKey> partitions) {
        return new CombinedPartitionsReader(lake, partitions)
                .read(MountTarget.aggregation("totals", new PartitionKey("")))
                .map(record -> record.record().getString("id"))
                .toList();
    }

    private static PartitionedRecord<GenericRecord> record(PartitionKey folder, String id) {
        return new PartitionedRecord<>(folder, GenericRecord.of(Map.of("id", id)));
    }
}
