package software.spool.mounter.api.builder;

import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.Handler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.SequentialPartitionDispatcher;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.*;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.control.steps.*;
import software.spool.mounter.internal.decorator.*;
import software.spool.mounter.internal.strategy.PollingMountStrategy;

import java.util.List;
import java.util.Objects;

class PollingMounterAssembler {
    static <T, R> Mounter assemble(PollingMounterBuilder<T, R> b) {
        ErrorRouter router = Objects.requireNonNullElse(b.observability.errorRouter,
                MounterErrorRouter.defaults(b.scheduling.publisher));
        MountTarget effectiveTarget = b.mount.scope != null
                ? b.mount.target.withSourceKey(b.mount.scope)
                : b.mount.target;

        Handler<MountTarget> unitHandler = MountUnitHandlerAssembler.assemble(b.reader, b.aggregator, effectiveTarget,
                b.mount, b.checkpoint, b.validation, b.quarantine, b.scheduling.publisher, router);

        PartitionDiscovery discovery = SafePartitionDiscovery.of(
                Objects.requireNonNullElse(b.scaling.discovery, scope -> List.of(PartitionInfo.of(scope))));
        ScalingPolicy scalingPolicy = SafeScalingPolicy.of(
                Objects.requireNonNullElse(b.scaling.scalingPolicy, partitions -> ScalingPlan.sequential()));
        PartitionDispatcher seqRaw = Objects.requireNonNullElse(b.scaling.localDispatcher,
                new SequentialPartitionDispatcher());
        PartitionDispatcher seq = SafePartitionDispatcher.of(seqRaw);
        PartitionDispatcher distributed = SafePartitionDispatcher.of(
                Objects.requireNonNullElse(b.scaling.distributedDispatcher, seqRaw));
        PartitionSplitter splitter = b.scaling.splitter != null
                ? SafePartitionSplitter.of(b.scaling.splitter) : null;

        Pipeline<PipelineContext, PipelineContext> pipeline = Pipeline.<PipelineContext>start()
                .add(new ObservedStep<>("discover-partitions", new DiscoverPartitionsStep(discovery)))
                .add(new ObservedStep<>("resolve-scaling-plan", new ResolveScalingPlanStep(scalingPolicy)))
                .add(new ObservedStep<>("build-mount-units", new BuildMountUnitsStep(splitter)))
                .add(new ObservedStep<>("dispatch-units", new DispatchUnitsStep(seq, distributed, unitHandler)));

        MountStrategy strategy = new PollingMountStrategy(effectiveTarget, new AtomicMountHandler(pipeline), router,
                b.scheduling.scheduler, b.scheduling.schedule);
        return new Mounter(strategy, router, b.heartBeat);
    }
}
