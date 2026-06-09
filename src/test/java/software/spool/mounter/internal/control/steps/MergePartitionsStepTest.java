package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.IncrementalDataMartReader;
import software.spool.mounter.api.port.MergeableMountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import javax.management.AttributeNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MergePartitionsStepTest {

    private static final MountTarget TARGET = MountTarget.transformation("dm", new PartitionKey("scope"));

    @Test
    @SuppressWarnings("unchecked")
    void apply_multiplePartitions_recordsMergedCorrectly() throws AttributeNotFoundException {
        List<PartitionKey> partitions = List.of(new PartitionKey("p1"), new PartitionKey("p2"));
        MergePartitionsStep<String> step = new MergePartitionsStep<>(
            emptyReader(),
            mergingAggregator()
        );
        PipelineContext ctx = contextWith(TARGET, partitions, Optional.empty());

        PipelineContext result = step.apply(ctx);

        Optional<String> current = (Optional<String>) result.require(IncrementalContextKeys.CURRENT);
        assertThat(current).contains("first+second");
    }

    @Test
    @SuppressWarnings("unchecked")
    void apply_emptyPartitionList_currentUnchanged() throws AttributeNotFoundException {
        MergePartitionsStep<String> step = new MergePartitionsStep<>(
            emptyReader(),
            mergingAggregator()
        );
        PipelineContext ctx = contextWith(TARGET, List.of(), Optional.of("original"));

        PipelineContext result = step.apply(ctx);

        Optional<String> current = (Optional<String>) result.require(IncrementalContextKeys.CURRENT);
        assertThat(current).contains("original");
    }

    private static PipelineContext contextWith(MountTarget target, List<PartitionKey> partitions, Optional<String> current) {
        return PipelineContext.empty()
            .with(IncrementalContextKeys.TARGET, target)
            .with(IncrementalContextKeys.CLOSED_PENDING, partitions)
            .with(IncrementalContextKeys.CURRENT, current);
    }

    private static IncrementalDataMartReader<String> emptyReader() {
        return new IncrementalDataMartReader<>() {
            @Override public List<PartitionKey> pendingPartitions(PartitionKey scope, Set<PartitionKey> already) { return List.of(); }
            @Override public String readCurrent(PartitionKey targetKey) { return null; }
            @Override public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) { return Stream.of(); }
        };
    }

    private static MergeableMountAggregator<String> mergingAggregator() {
        return new MergeableMountAggregator<>() {
            @Override public Stream<String> merge(String prev, Stream<PartitionedRecord<GenericRecord>> records) {
                return Stream.of(prev == null ? "first" : prev + "+second");
            }
            @Override public Stream<AggregatedRecord<String>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
                return Stream.empty();
            }
        };
    }
}
