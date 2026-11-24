package software.spool.mounter.acceptance;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.Event;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.DatedPartitionWindowPolicy;
import software.spool.mounter.api.adapter.InMemoryAggregateMetadataRepository;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.PartitioningQuarantinedResultWriter;
import software.spool.mounter.api.adapter.SameAsSourcePartitioning;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.builder.PollingMounterBuilder;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.internal.control.steps.IncrementalMountUnitHandler;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class AggregateIncrementallySteps {

    private static final PartitionKey SCOPE = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();
    private static final MountTarget TARGET = MountTarget.aggregation("payments-by-country", SCOPE);
    private static final String GOLD = TARGET.qualifiedDataMart();
    private static final Instant EVERY_PARTITION_CLOSED = Instant.parse("2026-11-18T00:00:00Z");

    public record Total(String country, long sum) {}

    public record Average(String country, long sum, long count) {}

    public static class TotalByCountry implements IncrementalMountAggregator<Total, GenericRecord> {
        public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) { return countryOf(record.record()); }
        public Total initial() { return new Total("", 0); }
        public Total add(Total state, GenericRecord record) {
            return new Total(record.getString("country"), state.sum() + record.getLong("amount"));
        }
        public GenericRecord result(Total state) { return GenericRecord.of(Map.of("country", state.country(), "total", state.sum())); }
        public Class<Total> stateType() { return Total.class; }
    }

    public static class AverageByCountry implements IncrementalMountAggregator<Average, GenericRecord> {
        public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) { return countryOf(record.record()); }
        public Average initial() { return new Average("", 0, 0); }
        public Average add(Average state, GenericRecord record) {
            return new Average(record.getString("country"), state.sum() + record.getLong("amount"), state.count() + 1);
        }
        public GenericRecord result(Average state) {
            return GenericRecord.of(Map.of("country", state.country(), "average", (double) state.sum() / state.count()));
        }
        public Class<Average> stateType() { return Average.class; }
    }

    private static final class MovableClock extends Clock {
        private Instant now = EVERY_PARTITION_CLOSED;

        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }

    private final Map<PartitionKey, List<GenericRecord>> silver = new LinkedHashMap<>();
    private final Set<PartitionedRecord<?>> gold = new LinkedHashSet<>();
    private final Set<PartitionKey> readInLastRound = new LinkedHashSet<>();
    private final List<Exception> errors = new ArrayList<>();
    private final MovableClock clock = new MovableClock();
    private Optional<InMemoryAggregateMetadataRepository> metadata = Optional.empty();
    private Duration closingMargin = Duration.ZERO;
    private IncrementalMountAggregator<?, GenericRecord> aggregator;
    private int failingWrites;
    private final MountRound mountRound;
    private Runnable round;

    private final Logger mounterLog = (Logger) org.slf4j.LoggerFactory.getLogger(IncrementalMountUnitHandler.class);
    private final ListAppender<ILoggingEvent> log = new ListAppender<>();

    public AggregateIncrementallySteps(MountRound mountRound) {
        this.mountRound = mountRound;
        log.start();
        mounterLog.addAppender(log);
    }

    @Given("aggregate metadata in memory")
    public void aggregateMetadataInMemory() {
        metadata = Optional.of(new InMemoryAggregateMetadataRepository());
    }

    @Given("there is no aggregate metadata")
    public void thereIsNoAggregateMetadata() {
        metadata = Optional.empty();
    }

    @Given("a margin of {int} minutes to close a partition")
    public void aMarginOfMinutesToCloseAPartition(int minutes) {
        closingMargin = Duration.ofMinutes(minutes);
    }

    @Given("the time is {string}")
    public void theTimeIs(String time) {
        clock.now = Instant.parse(time);
    }

    @Given("silver has the payments of the closed partition {string}:")
    @Given("silver has the payments of the partition {string}:")
    public void silverHasThePaymentsOfThePartition(String partition, DataTable payments) {
        List<GenericRecord> records = silver.computeIfAbsent(silverPartition(partition), ignored -> new ArrayList<>());
        payments.asMaps().forEach(payment -> records.add(GenericRecord.of(Map.of(
                "country", payment.get("country"), "amount", Long.parseLong(payment.get("amount"))))));
    }

    @Given("the mounter totals the amount of each country")
    public void theMounterTotalsTheAmountOfEachCountry() {
        aggregator = new TotalByCountry();
        mountRound.runWith(this::theMounterMounts);
    }

    @Given("the mounter averages the amount of each country")
    public void theMounterAveragesTheAmountOfEachCountry() {
        aggregator = new AverageByCountry();
        mountRound.runWith(this::theMounterMounts);
    }

    @Given("gold fails to write once")
    public void goldFailsToWriteOnce() {
        failingWrites = 1;
    }

    @Given("the mounter has mounted once")
    @Given("the mounter has mounted again")
    @When("the mounter mounts again")
    public void theMounterMounts() {
        if (round == null) start();
        readInLastRound.clear();
        round.run();
    }

    @Given("the mounter has mounted once without keeping how many files each partition had")
    public void theMounterHasMountedOnceWithoutKeepingHowManyFilesEachPartitionHad() {
        theMounterMounts();
        InMemoryAggregateMetadataRepository repository = metadata.orElseThrow();
        repository.markConsumed(GOLD, repository.consumedSources(GOLD).stream()
                .map(consumed -> ConsumedPartition.withoutFiles(consumed.partition()))
                .collect(Collectors.toSet()));
    }

    @Given("a payment taken out of the quarantine is written back to the partition {string} of silver:")
    public void aPaymentTakenOutOfTheQuarantineIsWrittenBack(String partition, DataTable payments) {
        PartitioningQuarantinedResultWriter quarantinedResults = new PartitioningQuarantinedResultWriter(
                MountTarget.transformation("payments", SCOPE), new SameAsSourcePartitioning<>(),
                (target, results) -> results.forEach(result -> silver
                        .computeIfAbsent(result.partitionKey(), ignored -> new ArrayList<>())
                        .add((GenericRecord) result.record())),
                new EventPublisher() {
                    public <E extends Event> void publish(E event) { }
                });
        payments.asMaps().forEach(payment -> quarantinedResults.write(silverPartition(partition),
                ("{\"country\":\"" + payment.get("country") + "\",\"amount\":" + payment.get("amount") + "}")
                        .getBytes(StandardCharsets.UTF_8)));
    }

    @Then("the mounter reads no partition")
    public void theMounterReadsNoPartition() {
        assertThat(readInLastRound).isEmpty();
    }

    @Then("the mounter warns that the partition {string} changed after it was read")
    public void theMounterWarnsThatThePartitionChanged(String partition) {
        assertThat(warnings()).anySatisfy(warning -> assertThat(warning).contains(silverPartition(partition).value()));
    }

    @Then("the mounter does not warn that any partition changed")
    public void theMounterDoesNotWarnThatAnyPartitionChanged() {
        assertThat(warnings()).isEmpty();
    }

    @After
    public void stopListeningToTheLog() {
        mounterLog.detachAppender(log);
        log.stop();
    }

    private List<String> warnings() {
        return log.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Then("the mounter reads only the partition {string}")
    public void theMounterReadsOnlyThePartition(String partition) {
        assertThat(readInLastRound).containsExactly(silverPartition(partition));
    }

    @Then("the mounter reads every partition of its scope")
    public void theMounterReadsEveryPartitionOfItsScope() {
        assertThat(readInLastRound).containsExactlyInAnyOrderElementsOf(silver.keySet());
    }

    @Then("gold has the total {long} for the country {string}")
    public void goldHasTheTotalForTheCountry(long total, String country) {
        assertThat(resultsOf(country)).singleElement()
                .satisfies(result -> assertThat(((GenericRecord) result.record()).getLong("total")).isEqualTo(total));
    }

    @Then("gold has a single result for the country {string}")
    public void goldHasASingleResultForTheCountry(String country) {
        assertThat(resultsOf(country)).hasSize(1);
    }

    @Then("gold has no result")
    public void goldHasNoResult() {
        assertThat(gold).isEmpty();
    }

    @Then("gold has the same result for each country as aggregating every partition at once")
    public void goldHasTheSameResultAsAggregatingEveryPartitionAtOnce() {
        List<PartitionedRecord<?>> atOnce = aggregator.aggregate(silver.entrySet().stream()
                        .flatMap(partition -> partition.getValue().stream().map(record -> new PartitionedRecord<>(partition.getKey(), record))))
                .<PartitionedRecord<?>>map(result -> result)
                .toList();
        assertThat(gold).containsExactlyInAnyOrderElementsOf(atOnce);
    }

    @Then("the metadata of the aggregate {string} names the partitions {string} and {string}")
    public void theMetadataOfTheAggregateNamesThePartitions(String aggregate, String first, String second) {
        assertThat(metadataOf(aggregate).sources()).containsExactlyInAnyOrder(silverPartition(first), silverPartition(second));
    }

    @Then("the metadata of the aggregate {string} names only the partition {string}")
    public void theMetadataOfTheAggregateNamesOnlyThePartition(String aggregate, String partition) {
        assertThat(metadataOf(aggregate).sources()).containsExactly(silverPartition(partition));
    }

    @Then("the metadata of the aggregate {string} keeps the sum {long} and the count {long}")
    public void theMetadataOfTheAggregateKeepsTheSumAndTheCount(String aggregate, long sum, long count) {
        Map<String, Object> state = stateOf(aggregate);
        assertThat(((Number) state.get("sum")).longValue()).isEqualTo(sum);
        assertThat(((Number) state.get("count")).longValue()).isEqualTo(count);
    }

    @Then("the metadata of the aggregate {string} keeps the sum {long}")
    public void theMetadataOfTheAggregateKeepsTheSum(String aggregate, long sum) {
        assertThat(((Number) stateOf(aggregate).get("sum")).longValue()).isEqualTo(sum);
    }

    private void start() {
        PollingMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>polling(target -> silver.entrySet().stream()
                        .filter(partition -> partition.getKey().value().startsWith(target.sourceKey().value()))
                        .flatMap(partition -> partition.getValue().stream().map(record -> new PartitionedRecord<>(partition.getKey(), record)))
                        .peek(record -> readInLastRound.add(record.partitionKey())))
                .aggregatingWith(asObjectAggregator());
        builder.mount().onTarget(TARGET).writingWith((target, results) -> results.forEach(result -> {
            if (failingWrites > 0) {
                failingWrites--;
                throw new IllegalStateException("Gold is not available");
            }
            gold.add(result);
        })).removingWith((target, result) -> gold.remove(result));
        builder.observability().withErrorRouter(new ErrorRouter().orElse((exception, context) -> errors.add(exception)));
        builder.scheduling().pollingWith(Schedule.every(Duration.ofMinutes(1))).scheduledWith((task, schedule, token) -> round = task);
        metadata.ifPresentOrElse(
                repository -> builder.checkpoint()
                        .windowPolicy(new DatedPartitionWindowPolicy(clock, closingMargin))
                        .checkpoint(new NoOpMountCheckpoint())
                        .aggregatingIncrementally(repository, scope -> silver.entrySet().stream()
                                .filter(partition -> partition.getKey().value().startsWith(scope.value()))
                                .map(partition -> PartitionInfo.of(partition.getKey(), partition.getValue().size(), 0))
                                .toList()),
                () -> builder.checkpoint().windowPolicy(new AlwaysClosedWindowPolicy()).checkpoint(new NoOpMountCheckpoint()));
        Mounter mounter = builder.build();
        mounter.start(mock(SpoolNode.StartPermit.class));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private IncrementalMountAggregator<Object, Object> asObjectAggregator() {
        return (IncrementalMountAggregator) aggregator;
    }

    private List<PartitionedRecord<?>> resultsOf(String country) {
        return gold.stream()
                .filter(result -> country.equals(((GenericRecord) result.record()).getString("country")))
                .toList();
    }

    private AggregateMetadata metadataOf(String aggregate) {
        return metadata.orElseThrow().find(GOLD, partitionOf(aggregate)).orElseThrow();
    }

    private Map<String, Object> stateOf(String aggregate) {
        return PayloadDeserializerFactory.json().asMap().deserialize(metadataOf(aggregate).state().getBytes());
    }

    private static PartitionKey silverPartition(String partition) {
        return new PartitionKey(KeyEntries.join(List.of(SCOPE.value(), partitionOf(partition).value())));
    }

    private static PartitionKey partitionOf(String path) {
        return new PartitionKey(KeyEntries.join(Arrays.stream(path.split("/")).collect(Collectors.toList())));
    }

    private static PartitionKey countryOf(GenericRecord record) {
        return PartitionKey.ofEntriesWithoutDate().with("country", record.getString("country")).build();
    }
}
