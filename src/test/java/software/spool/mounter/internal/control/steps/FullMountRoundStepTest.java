package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullMountRoundStepTest {
    private static final PartitionKey ALL = new PartitionKey("scope=all");
    private static final PartitionKey SPAIN = new PartitionKey("country=ES");
    private static final PartitionKey FRANCE = new PartitionKey("country=FR");
    private static final MountTarget WEB = MountTarget.transformation("payments", new PartitionKey("source=web"));
    private static final MountTarget APP = MountTarget.transformation("payments", new PartitionKey("source=app"));

    private final ListingDataMart dataMart = new ListingDataMart();

    @Test
    void apply_roundWithAChangedResult_leavesOnlyTheNewOne() throws Exception {
        round(WEB, result(ALL, "total=35"));

        round(WEB, result(ALL, "total=40"));

        assertThat(dataMart.in(ALL)).containsExactly("total=40");
    }

    @Test
    void apply_partitionThatNoLongerGetsResults_isLeftWithoutThem() throws Exception {
        round(WEB, result(SPAIN, "payment-1"), result(FRANCE, "payment-2"));

        round(WEB, result(SPAIN, "payment-1"));

        assertThat(dataMart.in(FRANCE)).isEmpty();
        assertThat(dataMart.in(SPAIN)).containsExactly("payment-1");
    }

    @Test
    void apply_twoMountsThatWriteTheSamePartition_removeNothingOfTheOther() throws Exception {
        round(WEB, result(SPAIN, "web-payment"));
        round(APP, result(SPAIN, "app-payment"));

        round(WEB, result(SPAIN, "web-payment"));
        round(APP, result(SPAIN, "app-payment"));

        assertThat(dataMart.in(SPAIN)).containsExactlyInAnyOrder("web-payment", "app-payment");
    }

    @Test
    void apply_roundThatFails_removesNothing() throws Exception {
        round(WEB, result(ALL, "total=35"));

        assertThatThrownBy(() -> failingRound(WEB, result(ALL, "total=40"))).isInstanceOf(IllegalStateException.class);

        assertThat(dataMart.in(ALL)).containsExactlyInAnyOrder("total=35", "total=40");
    }

    @Test
    void apply_roundAfterOneThatFailed_removesWhatTheFailedOneWroteAndIsNoLongerWritten() throws Exception {
        round(WEB, result(ALL, "total=35"));
        assertThatThrownBy(() -> failingRound(WEB, result(ALL, "total=40"))).isInstanceOf(IllegalStateException.class);

        round(WEB, result(ALL, "total=45"));

        assertThat(dataMart.in(ALL)).containsExactly("total=45");
    }

    @Test
    void apply_partOfTheScopeThatIsNoLongerMounted_isLeftWithoutItsResults() throws Exception {
        MountTarget day17 = WEB.withSourceKey(new PartitionKey("source=web::day=17"));
        MountTarget day18 = WEB.withSourceKey(new PartitionKey("source=web::day=18"));
        roundOfUnits(WEB, Map.of(day17, List.of(result(SPAIN, "payment-17")), day18, List.of(result(SPAIN, "payment-18"))));

        roundOfUnits(WEB, Map.of(day18, List.of(result(SPAIN, "payment-18"))));

        assertThat(dataMart.in(SPAIN)).containsExactly("payment-18");
    }

    @SafeVarargs
    private void round(MountTarget target, PartitionedRecord<String>... results) throws Exception {
        roundOfUnits(target, Map.of(target, List.of(results)));
    }

    @SafeVarargs
    private void failingRound(MountTarget target, PartitionedRecord<String>... results) throws Exception {
        Step<PipelineContext, PipelineContext> dispatch = context -> {
            dataMart.write(target, Stream.of(results).map(result -> (PartitionedRecord<?>) result));
            throw new IllegalStateException("A unit failed after writing");
        };
        new FullMountRoundStep(dispatch, dataMart).apply(PipelineContext.empty().with(MountContextKeys.TARGET, target));
    }

    private void roundOfUnits(MountTarget target, Map<MountTarget, List<PartitionedRecord<String>>> units) throws Exception {
        Step<PipelineContext, PipelineContext> dispatch = context -> {
            units.forEach((unit, results) -> dataMart.write(unit, results.stream().map(result -> (PartitionedRecord<?>) result)));
            return context;
        };
        new FullMountRoundStep(dispatch, dataMart).apply(PipelineContext.empty().with(MountContextKeys.TARGET, target));
    }

    private static PartitionedRecord<String> result(PartitionKey partition, String content) {
        return new PartitionedRecord<>(partition, content);
    }

    /** A data mart that keeps, for each mount, the list of what it wrote, as the real writers do. */
    private static final class ListingDataMart implements DataMartWriter {
        private final Map<PartitionKey, Set<Object>> stored = new HashMap<>();
        private final Map<MountTarget, Map<PartitionKey, Set<Object>>> lists = new HashMap<>();
        private final Map<MountTarget, Map<PartitionKey, Set<Object>>> rounds = new HashMap<>();

        @Override
        public void write(MountTarget target, Stream<PartitionedRecord<?>> results) {
            results.forEach(result -> {
                stored.computeIfAbsent(result.partitionKey(), ignored -> new HashSet<>()).add(result.record());
                rounds.entrySet().stream()
                        .filter(round -> target.sourceKey().contains(round.getKey().sourceKey()))
                        .forEach(round -> round.getValue()
                                .computeIfAbsent(result.partitionKey(), ignored -> new HashSet<>()).add(result.record()));
            });
        }

        @Override
        public void beginRound(MountTarget target) {
            rounds.put(target, new HashMap<>());
        }

        @Override
        public void completeRound(MountTarget target) {
            Map<PartitionKey, Set<Object>> written = rounds.remove(target);
            lists.getOrDefault(target, Map.of()).forEach((partition, before) -> before.stream()
                    .filter(result -> !written.getOrDefault(partition, Set.of()).contains(result))
                    .forEach(result -> stored.get(partition).remove(result)));
            lists.put(target, written);
        }

        @Override
        public void abandonRound(MountTarget target) {
            Map<PartitionKey, Set<Object>> written = rounds.remove(target);
            Map<PartitionKey, Set<Object>> list = lists.computeIfAbsent(target, ignored -> new HashMap<>());
            written.forEach((partition, results) -> list.computeIfAbsent(partition, ignored -> new HashSet<>()).addAll(results));
        }

        Set<Object> in(PartitionKey partition) {
            return stored.getOrDefault(partition, Set.of());
        }
    }
}
