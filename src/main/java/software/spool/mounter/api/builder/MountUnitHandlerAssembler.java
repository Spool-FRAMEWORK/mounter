package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.AggregateMetadataRepository;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.control.steps.IncrementalMountUnitHandler;
import software.spool.mounter.internal.control.steps.MissingPartitionAttributeHandler;
import software.spool.mounter.internal.control.steps.MountUnitHandler;
import software.spool.mounter.internal.control.steps.RoutingMissingPartitionAttributeHandler;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.decorator.SafeMountAggregator;
import software.spool.mounter.internal.decorator.SafePartitionDiscovery;
import software.spool.mounter.internal.decorator.SafePartitionedReader;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.quarantine.MountResultQuarantine;
import software.spool.mounter.internal.quarantine.QuarantiningMissingPartitionAttributeHandler;
import software.spool.mounter.internal.quarantine.QuarantiningMountResultHandler;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

class MountUnitHandlerAssembler {
    private MountUnitHandlerAssembler() {
    }

    @SuppressWarnings("unchecked")
    static <O> Handler<MountTarget> assemble(PartitionedReader reader, MountAggregator<O> aggregator, MountTarget target,
                                             MountFacet<?> mount, CheckpointFacet<?> checkpoint, ValidationFacet<?> validation,
                                             QuarantineFacet<?> quarantine, EventPublisher publisher, ErrorRouter router) {
        Optional<DataLakeRecordQuarantine> recordQuarantine = target.mode() == MountMode.TRANSFORMATION
                ? quarantine.dataLakeQuarantineFor(publisher)
                : Optional.empty();
        PartitionedReader safeReader = SafePartitionedReader.of(validation.readerFor(reader, target, recordQuarantine));
        MountPartitioning<? super O> partitioning = (MountPartitioning<? super O>) mount.partitioning;
        MissingPartitionAttributeHandler missingAttributeHandler =
                missingAttributeHandler(recordQuarantine, quarantine.resultQuarantineFor(publisher), router);
        Optional<AggregateMetadataRepository> aggregateMetadata = checkpoint.aggregateMetadata();
        if (aggregateMetadata.isPresent()) {
            return incremental(safeReader, aggregator, target, mount, checkpoint, aggregateMetadata.get(), partitioning,
                    missingAttributeHandler);
        }
        return new MountUnitHandler<>(safeReader, SafeMountAggregator.of(aggregator), SafeDataMartWriter.of(mount.writer),
                checkpoint.windowPolicy, checkpoint.checkpoint, partitioning, missingAttributeHandler);
    }

    private static <O> Handler<MountTarget> incremental(PartitionedReader reader, MountAggregator<O> aggregator,
                                                        MountTarget target, MountFacet<?> mount,
                                                        CheckpointFacet<?> checkpoint,
                                                        AggregateMetadataRepository aggregateMetadata,
                                                        MountPartitioning<? super O> partitioning,
                                                        MissingPartitionAttributeHandler missingAttributeHandler) {
        if (!(aggregator instanceof IncrementalMountAggregator<?, O> incrementalAggregator)) {
            throw new IllegalStateException("Aggregate metadata needs an incremental aggregator, and "
                    + aggregator.getClass().getName() + " is not");
        }
        if (target.mode() != MountMode.AGGREGATION) {
            throw new IllegalStateException("Aggregate metadata is only used by an AGGREGATION mount");
        }
        Objects.requireNonNull(mount.remover, "DataMartRemover required to aggregate incrementally; set it with mount().removingWith(...)");
        return new IncrementalMountUnitHandler<>(SafePartitionDiscovery.of(checkpoint.partitionDiscovery()), reader,
                incrementalAggregator, aggregateMetadata, checkpoint.incrementalWindowPolicy(),
                SafeDataMartWriter.of(mount.writer), mount.remover, partitioning, missingAttributeHandler,
                Clock.systemUTC());
    }

    private static MissingPartitionAttributeHandler missingAttributeHandler(Optional<DataLakeRecordQuarantine> recordQuarantine,
                                                                            Optional<MountResultQuarantine> resultQuarantine,
                                                                            ErrorRouter router) {
        MissingPartitionAttributeHandler forResults = resultQuarantine
                .<MissingPartitionAttributeHandler>map(QuarantiningMountResultHandler::new)
                .orElseGet(() -> new RoutingMissingPartitionAttributeHandler(router));
        return recordQuarantine
                .<MissingPartitionAttributeHandler>map(present -> new QuarantiningMissingPartitionAttributeHandler(present, forResults))
                .orElse(forResults);
    }
}
