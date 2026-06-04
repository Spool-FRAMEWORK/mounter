package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.adapter.SequentialPartitionDispatcher;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.port.scaling.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class AtomicMountHandler<O> implements Handler<MountTarget> {
    private final PartitionedReader reader;
    private final MountAggregator<O> aggregator;
    private final DataMartWriter writer;
    private final EventPublisher publisher;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final PartitionKeyExtractor<O> keyExtractor;
    private final PartitionDiscovery discovery;
    private final PartitionSplitter splitter;
    private final ScalingPolicy policy;
    private final PartitionDispatcher localDispatcher;
    private final PartitionDispatcher distributedDispatcher;

    public AtomicMountHandler(PartitionedReader reader,
                              MountAggregator<O> aggregator,
                              DataMartWriter writer,
                              EventPublisher publisher,
                              PartitionWindowPolicy windowPolicy,
                              MountCheckpoint checkpoint,
                              PartitionKeyExtractor<O> keyExtractor) {
        this(reader, aggregator, writer, publisher, windowPolicy, checkpoint, keyExtractor, null, null, null, null, null);
    }

    public AtomicMountHandler(PartitionedReader reader,
                              MountAggregator<O> aggregator,
                              DataMartWriter writer,
                              EventPublisher publisher,
                              PartitionWindowPolicy windowPolicy,
                              MountCheckpoint checkpoint,
                              PartitionKeyExtractor<O> keyExtractor,
                              PartitionDiscovery discovery,
                              PartitionSplitter splitter,
                              ScalingPolicy policy,
                              PartitionDispatcher localDispatcher,
                              PartitionDispatcher distributedDispatcher) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.publisher = publisher;
        this.windowPolicy = windowPolicy;
        this.checkpoint = checkpoint;
        this.keyExtractor = keyExtractor;
        this.discovery = Objects.requireNonNullElse(discovery, scope -> List.of(PartitionInfo.of(scope)));
        this.splitter = splitter;
        this.policy = Objects.requireNonNullElse(policy, partitions -> ScalingPlan.sequential());
        PartitionDispatcher seq = Objects.requireNonNullElse(localDispatcher, new SequentialPartitionDispatcher());
        this.localDispatcher = seq;
        this.distributedDispatcher = Objects.requireNonNullElse(distributedDispatcher, seq);
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        List<PartitionInfo> discovered = discovery.discover(target.sourceKey());
        ScalingPlan plan = policy.resolve(discovered);
        List<MountTarget> units = buildUnits(target, discovered, plan);
        selectDispatcher(plan).dispatch(units, this::mountUnit);
    }

    private List<MountTarget> buildUnits(MountTarget scope, List<PartitionInfo> discovered, ScalingPlan plan) {
        List<MountTarget> units = new ArrayList<>();
        for (PartitionInfo info : discovered) {
            if (splitter != null && plan.splitHint() != null
                    && info.estimatedRecords().isPresent()
                    && info.estimatedRecords().getAsLong() > plan.splitHint().targetRecordsPerSlice()) {
                for (PartitionSlice slice : splitter.split(info, plan.splitHint())) {
                    units.add(scope.withSourceKey(slice.key()).withSlice(slice));
                }
            } else {
                units.add(scope.withSourceKey(info.key()));
            }
        }
        return units;
    }

    private PartitionDispatcher selectDispatcher(ScalingPlan plan) {
        return plan.mode() == ExecutionMode.DISTRIBUTED ? distributedDispatcher : localDispatcher;
    }

    private void mountUnit(MountTarget target) throws SpoolException {
        if (shouldSkip(target)) return;
        writeResult(target, aggregator.aggregate(reader.read(target)));
        markAsMounted(target);
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private void writeResult(MountTarget target, Stream<AggregatedRecord<O>> aggregated) {
        Stream<PartitionedRecord<?>> partitionedStream = aggregated.map(agg ->
                new PartitionedRecord<>(
                        keyExtractor.extract(agg.source(), agg.output(), target),
                        agg.output()
                )
        );
        writer.write(target, partitionedStream);
    }

    private void markAsMounted(MountTarget target) {
        checkpoint.markMounted(target);
    }
}
