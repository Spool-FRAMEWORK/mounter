package software.spool.mounter.internal.control.steps;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.adapter.CombinedPartitionsReader;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.model.PartitionPeriod;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Mounts an aggregation incrementally (ADR-027): each round reads only the closed partitions of silver the data mart
 * has not consumed, adds their records to the state each aggregate keeps in its metadata, and replaces the result of
 * every aggregate that changed. A scope with a partition without a date is aggregated again in full.
 *
 * <p>The metadata of the aggregates is saved before the partitions are marked consumed, and both before anything is
 * published, so a round that fails half way neither loses records nor counts them twice. A result that fails to be
 * published stays pending and is published in a later round.</p>
 */
public class IncrementalMountUnitHandler<S, O> implements Handler<MountTarget> {
    private final PartitionDiscovery discovery;
    private final PartitionedReader reader;
    private final IncrementalMountAggregator<S, O> aggregator;
    private final AggregateStateCodec<S> codec;
    private final AggregateMetadataRepository metadata;
    private final PartitionWindowPolicy windowPolicy;
    private final DataMartWriter writer;
    private final DataMartRemover remover;
    private final MountPartitioning<? super O> partitioning;
    private final MissingPartitionAttributeHandler missingAttributeHandler;
    private final Clock clock;

    public IncrementalMountUnitHandler(PartitionDiscovery discovery, PartitionedReader reader,
                                       IncrementalMountAggregator<S, O> aggregator, AggregateMetadataRepository metadata,
                                       PartitionWindowPolicy windowPolicy, DataMartWriter writer, DataMartRemover remover,
                                       MountPartitioning<? super O> partitioning,
                                       MissingPartitionAttributeHandler missingAttributeHandler, Clock clock) {
        this.discovery = discovery;
        this.reader = reader;
        this.aggregator = aggregator;
        this.codec = new AggregateStateCodec<>(aggregator);
        this.metadata = metadata;
        this.windowPolicy = windowPolicy;
        this.writer = writer;
        this.remover = remover;
        this.partitioning = partitioning;
        this.missingAttributeHandler = missingAttributeHandler;
        this.clock = clock;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        String dataMart = target.qualifiedDataMart();
        List<PartitionKey> partitions = discovery.discover(target.sourceKey()).stream().map(PartitionInfo::key).toList();
        boolean inFull = partitions.stream().anyMatch(partition -> PartitionPeriod.of(partition).isEmpty());
        List<PartitionKey> toRead = inFull ? partitions : closedAndNotConsumed(dataMart, partitions);
        fold(target, toRead, inFull).values().stream()
                .filter(AggregateFold::hasChanged)
                .forEach(changed -> metadata.save(changed.metadata(clock.instant())));
        if (!inFull) metadata.markConsumed(dataMart, Set.copyOf(toRead));
        metadata.findAll(dataMart).stream()
                .filter(AggregateMetadata::isPublicationPending)
                .forEach(pending -> publish(target, pending));
    }

    private List<PartitionKey> closedAndNotConsumed(String dataMart, List<PartitionKey> partitions) {
        Set<PartitionKey> consumed = metadata.consumedSources(dataMart);
        return partitions.stream()
                .filter(partition -> !consumed.contains(partition))
                .filter(windowPolicy::isClosed)
                .toList();
    }

    private Map<PartitionKey, AggregateFold<S>> fold(MountTarget target, List<PartitionKey> toRead, boolean inFull) {
        Map<PartitionKey, AggregateFold<S>> folds = new LinkedHashMap<>();
        if (toRead.isEmpty()) return folds;
        try (Stream<PartitionedRecord<GenericRecord>> records = new CombinedPartitionsReader(reader, toRead).read(target)) {
            records.forEach(record -> folds
                    .computeIfAbsent(aggregator.groupOf(record), aggregate -> foldOf(target, aggregate, inFull))
                    .add(record));
        }
        return folds;
    }

    private AggregateFold<S> foldOf(MountTarget target, PartitionKey aggregate, boolean inFull) {
        Optional<AggregateMetadata> kept = metadata.find(target.qualifiedDataMart(), aggregate);
        return inFull
                ? AggregateFold.startingOver(target.qualifiedDataMart(), aggregate, kept, aggregator, codec)
                : AggregateFold.continuing(target.qualifiedDataMart(), aggregate, kept, aggregator, codec);
    }

    private void publish(MountTarget target, AggregateMetadata pending) {
        pending.publishedState()
                .map(published -> resultOf(pending.aggregate(), published))
                .flatMap(this::partitionOf)
                .ifPresent(previous -> remover.remove(target, previous));
        write(target, resultOf(pending.aggregate(), pending.state()));
        metadata.save(pending.published());
    }

    private void write(MountTarget target, PartitionedRecord<O> result) {
        PartitionKey partition;
        try {
            partition = partitioning.of(result);
        } catch (MissingPartitionAttributeException e) {
            missingAttributeHandler.handle(target, result, e);
            return;
        }
        writer.write(target, Stream.<PartitionedRecord<?>>of(new PartitionedRecord<>(partition, result.record())));
    }

    private PartitionedRecord<O> resultOf(PartitionKey aggregate, String state) {
        return new PartitionedRecord<>(aggregate, aggregator.result(codec.decode(state)));
    }

    private Optional<PartitionedRecord<?>> partitionOf(PartitionedRecord<O> result) {
        try {
            return Optional.of(new PartitionedRecord<>(partitioning.of(result), result.record()));
        } catch (MissingPartitionAttributeException e) {
            return Optional.empty();
        }
    }
}
