package software.spool.mounter.api.builder;

import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.SequentialPartitionDispatcher;
import software.spool.mounter.api.port.PartitionKeyExtractor;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.ScalingPlan;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.control.steps.*;
import software.spool.mounter.internal.decorator.*;
import software.spool.mounter.internal.strategy.ReactiveMountStrategy;

import java.util.List;
import java.util.Objects;

class ReactiveMounterAssembler {
    @SuppressWarnings("unchecked")
    static <I, O> Mounter assemble(ReactiveMounterBuilder<I, O> b) {
        ErrorRouter router = Objects.requireNonNullElse(b.observability.errorRouter,
                MounterErrorRouter.defaults(b.bus));

        MountUnitHandler<O> unitHandler = new MountUnitHandler<>(
                SafePartitionedReader.of(b.reader),
                SafeMountAggregator.of(b.aggregator),
                SafeDataMartWriter.of(b.mount.writer),
                b.checkpoint.windowPolicy, b.checkpoint.checkpoint,
                (PartitionKeyExtractor<O>) b.mount.keyExtractor
        );

        PartitionDispatcher seq = SafePartitionDispatcher.of(new SequentialPartitionDispatcher());
        Pipeline<PipelineContext, PipelineContext> pipeline = Pipeline.<PipelineContext>start()
                .add(new ObservedStep<>("discover-partitions",  new DiscoverPartitionsStep(SafePartitionDiscovery.of(scope -> List.of(PartitionInfo.of(scope))))))
                .add(new ObservedStep<>("resolve-scaling-plan", new ResolveScalingPlanStep(SafeScalingPolicy.of(partitions -> ScalingPlan.sequential()))))
                .add(new ObservedStep<>("build-mount-units",    new BuildMountUnitsStep(null)))
                .add(new ObservedStep<>("dispatch-units",       new DispatchUnitsStep(seq, seq, unitHandler)));

        MountStrategy strategy = new ReactiveMountStrategy(b.mount.target, b.bus,
                new AtomicMountHandler(pipeline), router);
        return new Mounter(strategy, router, b.moduleHeartBeat);
    }
}
