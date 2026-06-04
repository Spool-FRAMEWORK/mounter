package software.spool.mounter.api.builder;

import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.SequentialPartitionDispatcher;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionKeyExtractor;
import software.spool.mounter.api.port.scaling.*;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.control.steps.*;
import software.spool.mounter.internal.strategy.PollingMountStrategy;

import java.util.List;
import java.util.Objects;

class PollingMounterAssembler {
    @SuppressWarnings("unchecked")
    static <T, R> Mounter assemble(PollingMounterBuilder<T, R> b) {
        ErrorRouter router = Objects.requireNonNullElse(b.observability.errorRouter,
                MounterErrorRouter.defaults(b.scheduling.publisher));
        MountTarget effectiveTarget = b.mount.scope != null
                ? b.mount.target.withSourceKey(b.mount.scope)
                : b.mount.target;

        MountUnitHandler<R> unitHandler = new MountUnitHandler<>(
                b.reader, b.aggregator, b.mount.writer,
                b.checkpoint.windowPolicy, b.checkpoint.checkpoint,
                (PartitionKeyExtractor<R>) b.mount.keyExtractor
        );

        PartitionDiscovery discovery = Objects.requireNonNullElse(b.scaling.discovery,
                scope -> List.of(PartitionInfo.of(scope)));
        ScalingPolicy scalingPolicy = Objects.requireNonNullElse(b.scaling.scalingPolicy,
                partitions -> ScalingPlan.sequential());
        PartitionDispatcher seq = Objects.requireNonNullElse(b.scaling.localDispatcher,
                new SequentialPartitionDispatcher());
        PartitionDispatcher distributed = Objects.requireNonNullElse(b.scaling.distributedDispatcher, seq);

        Pipeline<PipelineContext, PipelineContext> pipeline = Pipeline.<PipelineContext>start()
                .add(new DiscoverPartitionsStep(discovery))
                .add(new ResolveScalingPlanStep(scalingPolicy))
                .add(new BuildMountUnitsStep(b.scaling.splitter))
                .add(new DispatchUnitsStep(seq, distributed, unitHandler));

        MountStrategy strategy = new PollingMountStrategy(effectiveTarget, new AtomicMountHandler<>(pipeline), router,
                b.scheduling.scheduler, b.scheduling.pollingPolicy);
        return new Mounter(strategy, router, b.heartBeat);
    }
}
