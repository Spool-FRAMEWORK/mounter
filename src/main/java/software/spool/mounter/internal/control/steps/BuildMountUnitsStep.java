package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.*;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class BuildMountUnitsStep implements Step<PipelineContext, PipelineContext> {
    private final PartitionSplitter splitter;

    public BuildMountUnitsStep(PartitionSplitter splitter) {
        this.splitter = splitter;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        MountTarget scope = ctx.require(MountContextKeys.TARGET);
        List<PartitionInfo> discovered = ctx.require(MountContextKeys.DISCOVERED);
        ScalingPlan plan = ctx.require(MountContextKeys.PLAN);
        return ctx.with(MountContextKeys.UNITS, buildUnits(scope, discovered, plan));
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
}
