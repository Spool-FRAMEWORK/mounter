package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.InMemoryAggregateMetadataRepository;
import software.spool.mounter.api.adapter.SameAsSourcePartitioning;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IncrementalMountUnitHandlerTest {
    private static final PartitionKey SCOPE = new PartitionKey("source=payments");
    private static final PartitionKey TEN = new PartitionKey("source=payments::year=2026::month=11::day=17::hour=10");
    private static final PartitionKey ELEVEN = new PartitionKey("source=payments::year=2026::month=11::day=17::hour=11");
    private static final PartitionKey ALL = new PartitionKey("scope=all");
    private static final MountTarget GOLD = MountTarget.aggregation("totals", SCOPE);

    record Total(long sum) {}

    static class AmountTotal implements IncrementalMountAggregator<Total, Long> {
        public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) { return ALL; }
        public Total initial() { return new Total(0); }
        public Total add(Total state, GenericRecord record) { return new Total(state.sum() + record.getLong("amount")); }
        public Long result(Total state) { return state.sum(); }
        public Class<Total> stateType() { return Total.class; }
    }

    static class MarkingPublishedFailsOnce extends InMemoryAggregateMetadataRepository {
        private boolean failed;

        @Override
        public void save(AggregateMetadata metadata) {
            if (!failed && !metadata.isPublicationPending()) {
                failed = true;
                throw new IllegalStateException("The metadata store went away");
            }
            super.save(metadata);
        }
    }

    private final Map<PartitionKey, List<GenericRecord>> silver = new LinkedHashMap<>();
    private final Set<PartitionedRecord<?>> gold = new LinkedHashSet<>();

    @Test
    void handle_afterARoundCutBetweenWritingAndMarkingItPublished_leavesASingleResult() {
        IncrementalMountUnitHandler<Total, Long> handler = handlerWith(new MarkingPublishedFailsOnce());
        silver.put(TEN, List.of(payment(10)));
        assertThatThrownBy(() -> handler.handle(GOLD)).hasMessage("The metadata store went away");

        silver.put(ELEVEN, List.of(payment(5)));
        handler.handle(GOLD);

        assertThat(gold).containsExactly(new PartitionedRecord<>(ALL, 15L));
    }

    @Test
    void handle_twiceWithoutNewPartitions_writesNothingAgain() {
        IncrementalMountUnitHandler<Total, Long> handler = handlerWith(new InMemoryAggregateMetadataRepository());
        silver.put(TEN, List.of(payment(10)));

        handler.handle(GOLD);
        handler.handle(GOLD);

        assertThat(gold).containsExactly(new PartitionedRecord<>(ALL, 10L));
    }

    private IncrementalMountUnitHandler<Total, Long> handlerWith(InMemoryAggregateMetadataRepository metadata) {
        return new IncrementalMountUnitHandler<>(
                scope -> silver.keySet().stream().map(PartitionInfo::of).toList(),
                target -> silver.getOrDefault(target.sourceKey(), List.of()).stream()
                        .map(record -> new PartitionedRecord<>(target.sourceKey(), record)),
                new AmountTotal(), metadata, new AlwaysClosedWindowPolicy(),
                (target, results) -> results.forEach(gold::add),
                (target, result) -> gold.remove(result),
                new SameAsSourcePartitioning<>(),
                (target, result, exception) -> { throw exception; },
                Clock.systemUTC());
    }

    private static GenericRecord payment(long amount) {
        return GenericRecord.of(Map.of("amount", amount));
    }
}
