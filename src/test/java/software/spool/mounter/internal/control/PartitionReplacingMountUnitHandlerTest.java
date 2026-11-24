package software.spool.mounter.internal.control;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionReplacingMountUnitHandlerTest {
    private static final PartitionKey ALL = new PartitionKey("scope=all");
    private static final PartitionKey SPAIN = new PartitionKey("country=ES");
    private static final PartitionKey FRANCE = new PartitionKey("country=FR");
    private static final MountTarget TARGET = MountTarget.aggregation("payments-total", new PartitionKey("source=payments"));

    private final NamedDataMart dataMart = new NamedDataMart();

    @Test
    void handle_roundWithAChangedResult_leavesOnlyTheNewOne() {
        round(result(ALL, "total=35"));

        round(result(ALL, "total=40"));

        assertThat(dataMart.in(ALL)).containsExactly("total=40");
    }

    @Test
    void handle_roundThatFails_removesNothing() {
        round(result(ALL, "total=35"));

        assertThatThrownBy(() -> handlerWriting(List.of(result(ALL, "total=40")), true).handle(TARGET))
                .isInstanceOf(IllegalStateException.class);

        assertThat(dataMart.in(ALL)).contains("total=35");
    }

    @Test
    void handle_partitionTheRoundDoesNotWrite_isLeftAsItWas() {
        round(result(SPAIN, "payment-1"), result(FRANCE, "payment-2"));

        round(result(SPAIN, "payment-3"));

        assertThat(dataMart.in(SPAIN)).containsExactly("payment-3");
        assertThat(dataMart.in(FRANCE)).containsExactly("payment-2");
    }

    @Test
    void handle_roundAfterOneThatFailed_keepsOnlyWhatItWrites() {
        round(result(ALL, "total=35"));
        assertThatThrownBy(() -> handlerWriting(List.of(result(ALL, "total=40")), true).handle(TARGET))
                .isInstanceOf(IllegalStateException.class);

        round(result(ALL, "total=45"));

        assertThat(dataMart.in(ALL)).containsExactly("total=45");
    }

    @SafeVarargs
    private void round(PartitionedRecord<String>... results) {
        handlerWriting(List.of(results), false).handle(TARGET);
    }

    private Handler<MountTarget> handlerWriting(List<PartitionedRecord<String>> results, boolean failingAfterwards) {
        Handler<MountTarget> unit = target -> {
            dataMart.write(target, results.stream().map(result -> (PartitionedRecord<?>) result));
            if (failingAfterwards) throw new IllegalStateException("The aggregator failed after writing");
        };
        return new PartitionReplacingMountUnitHandler(unit, dataMart);
    }

    private static PartitionedRecord<String> result(PartitionKey partition, String content) {
        return new PartitionedRecord<>(partition, content);
    }

    private static final class NamedDataMart implements DataMartWriter {
        private final Map<PartitionKey, Set<Object>> stored = new HashMap<>();
        private final Map<MountTarget, Map<PartitionKey, Set<Object>>> written = new HashMap<>();

        @Override
        public void write(MountTarget target, Stream<PartitionedRecord<?>> results) {
            results.forEach(result -> {
                stored.computeIfAbsent(result.partitionKey(), ignored -> new HashSet<>()).add(result.record());
                written.computeIfAbsent(target, ignored -> new HashMap<>())
                        .computeIfAbsent(result.partitionKey(), ignored -> new HashSet<>()).add(result.record());
            });
        }

        @Override
        public void keepOnlyWhatWasWritten(MountTarget target) {
            written.getOrDefault(target, Map.of()).forEach((partition, kept) -> stored.get(partition).retainAll(kept));
            written.remove(target);
        }

        @Override
        public void forgetWhatWasWritten(MountTarget target) {
            written.remove(target);
        }

        Set<Object> in(PartitionKey partition) {
            return stored.getOrDefault(partition, Set.of());
        }
    }
}
