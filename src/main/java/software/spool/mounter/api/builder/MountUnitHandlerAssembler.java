package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.control.steps.MissingPartitionAttributeHandler;
import software.spool.mounter.internal.control.steps.MountUnitHandler;
import software.spool.mounter.internal.control.steps.RoutingMissingPartitionAttributeHandler;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.decorator.SafeMountAggregator;
import software.spool.mounter.internal.decorator.SafePartitionedReader;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.quarantine.QuarantiningMissingPartitionAttributeHandler;

import java.util.Optional;

class MountUnitHandlerAssembler {
    private MountUnitHandlerAssembler() {
    }

    @SuppressWarnings("unchecked")
    static <O> MountUnitHandler<O> assemble(PartitionedReader reader, MountAggregator<O> aggregator, MountTarget target,
                                            MountFacet<?> mount, CheckpointFacet<?> checkpoint, ValidationFacet<?> validation,
                                            QuarantineFacet<?> quarantine, EventPublisher publisher, ErrorRouter router) {
        Optional<DataLakeRecordQuarantine> recordQuarantine = quarantine.quarantineFor(publisher);
        return new MountUnitHandler<>(
                SafePartitionedReader.of(validation.readerFor(reader, target, recordQuarantine)),
                SafeMountAggregator.of(aggregator),
                SafeDataMartWriter.of(mount.writer),
                checkpoint.windowPolicy, checkpoint.checkpoint,
                (MountPartitioning<? super O>) mount.partitioning,
                missingAttributeHandler(target, recordQuarantine, router));
    }

    private static MissingPartitionAttributeHandler missingAttributeHandler(MountTarget target,
                                                                            Optional<DataLakeRecordQuarantine> recordQuarantine,
                                                                            ErrorRouter router) {
        MissingPartitionAttributeHandler routing = new RoutingMissingPartitionAttributeHandler(router);
        return recordQuarantine.filter(present -> target.mode() == MountMode.TRANSFORMATION)
                .<MissingPartitionAttributeHandler>map(present -> new QuarantiningMissingPartitionAttributeHandler(present, routing))
                .orElse(routing);
    }
}
