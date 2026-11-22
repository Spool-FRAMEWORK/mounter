package software.spool.mounter.api.builder;

import org.junit.jupiter.api.Test;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.adapter.InMemoryAggregateMetadataRepository;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class IncrementalMountBuilderTest {

    private static final PartitionKey SCOPE = new PartitionKey("source=payments");

    record Count(long records) {}

    static class RecordCount implements IncrementalMountAggregator<Count, Long> {
        public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) { return SCOPE; }
        public Count initial() { return new Count(0); }
        public Count add(Count state, GenericRecord record) { return new Count(state.records() + 1); }
        public Long result(Count state) { return state.records(); }
        public Class<Count> stateType() { return Count.class; }
    }

    @Test
    void build_withAggregateMetadataAndAnAggregatorThatIsNotIncremental_isRejected() {
        assertThatThrownBy(() -> incrementalMounter(new PassThroughMountAggregator(), MountTarget.aggregation("totals", SCOPE), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Aggregate metadata needs an incremental aggregator, and " + PassThroughMountAggregator.class.getName() + " is not");
    }

    @Test
    void build_withAggregateMetadataOnATransformation_isRejected() {
        assertThatThrownBy(() -> incrementalMounter(new RecordCount(), MountTarget.transformation("payments", SCOPE), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Aggregate metadata is only used by an AGGREGATION mount");
    }

    @Test
    void build_withAggregateMetadataAndNoRemover_isRejected() {
        assertThatThrownBy(() -> incrementalMounter(new RecordCount(), MountTarget.aggregation("totals", SCOPE), false))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mount().removingWith(...)");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void incrementalMounter(MountAggregator<?> aggregator, MountTarget target, boolean withRemover) {
        PollingMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>polling(ignored -> Stream.empty())
                .aggregatingWith((MountAggregator) aggregator);
        builder.mount().onTarget(target).writingWith(mock(DataMartWriter.class));
        if (withRemover) builder.mount().removingWith((ignored, result) -> { });
        builder.checkpoint().aggregatingIncrementally(new InMemoryAggregateMetadataRepository(),
                scope -> List.of(PartitionInfo.of(scope)));
        builder.scheduling().pollingWith(Schedule.every(Duration.ofMinutes(1)));
        builder.build();
    }
}
