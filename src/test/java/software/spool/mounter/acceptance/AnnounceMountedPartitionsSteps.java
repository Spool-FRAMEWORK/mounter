package software.spool.mounter.acceptance;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.memory.InMemoryEventBus;
import software.spool.core.adapter.memory.InMemoryQuarantineRepository;
import software.spool.core.exception.QuarantinedResultRejectedException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopesMounted;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.AttributesPartitioning;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.PartitioningQuarantinedResultWriter;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.builder.PollingMounterBuilder;
import software.spool.mounter.api.builder.ReactiveMounterBuilder;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class AnnounceMountedPartitionsSteps {
    private static final PartitionKey BRONZE = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();
    private static final String ATTRIBUTE_PREFIX = "payload.";
    private static final PartitionKey ALL_PAYMENTS = PartitionKey.ofEntriesWithoutDate().with("scope", "all").build();

    private final List<PartitionedRecord<GenericRecord>> bronze = new ArrayList<>();
    private final Map<PartitionKey, List<GenericRecord>> silver = new LinkedHashMap<>();
    private final Map<String, List<PartitionedRecord<?>>> written = new LinkedHashMap<>();
    private final List<EnvelopesMounted> announced = new ArrayList<>();
    private InMemoryEventBus bus;
    private boolean silverFails;
    private PartitioningQuarantinedResultWriter quarantinedResults;
    private RuntimeException takenOutFailure;
    private final MountRound mountRound;
    private Runnable round;

    public AnnounceMountedPartitionsSteps(MountRound mountRound) {
        this.mountRound = mountRound;
    }

    @Before
    public void listenToTheBus() {
        bus = new InMemoryEventBus();
        bus.subscribe(EnvelopesMounted.class, announced::add);
    }

    @Given("bronze has these payments:")
    public void bronzeHasThesePayments(DataTable payments) {
        payments.asMaps().forEach(payment -> {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("id", payment.get("id"));
            if (payment.get("country") != null) payload.put("country", payment.get("country"));
            bronze.add(bronzeRecordOf(payment.get("id"), payload));
        });
    }

    @Given("bronze has no payments")
    public void bronzeHasNoPayments() {
        bronze.clear();
    }

    @Given("a mounter that copies each payment to the silver data mart {string} partitioned by {string}")
    public void aMounterThatCopiesEachPayment(String dataMart, String attribute) {
        MountAggregator<Object> copy = records -> new PassThroughMountAggregator().aggregate(records)
                .map(read -> new PartitionedRecord<Object>(read.partitionKey(), read.record()));
        PollingMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>polling(target -> List.copyOf(bronze).stream())
                .aggregatingWith(copy);
        builder.mount().onTarget(MountTarget.transformation(dataMart, BRONZE))
                .writingWith((target, results) -> write(target, results))
                .partitioningWith(AttributesPartitioning.of(ATTRIBUTE_PREFIX + attribute));
        builder.checkpoint().windowPolicy(new AlwaysClosedWindowPolicy()).checkpoint(new NoOpMountCheckpoint());
        builder.quarantine().in(new InMemoryQuarantineRepository())
                .removingFromDataLakeWith((partition, idempotencyKey) -> { });
        builder.observability().withErrorRouter(new ErrorRouter().orElse((exception, context) -> { }));
        builder.scheduling().pollingWith(Schedule.every(Duration.ofMinutes(1)))
                .scheduledWith((task, schedule, token) -> round = task)
                .emittingWith(bus);
        builder.build().start(mock(SpoolNode.StartPermit.class));
        mountRound.runWith(() -> round.run());
        quarantinedResults = new PartitioningQuarantinedResultWriter(MountTarget.transformation(dataMart, BRONZE),
                AttributesPartitioning.of(ATTRIBUTE_PREFIX + attribute), this::write, bus);
    }

    @Given("silver cannot be written")
    public void silverCannotBeWritten() {
        silverFails = true;
    }

    @Given("silver has these payments in the partition {string}:")
    public void silverHasThesePaymentsInThePartition(String partition, DataTable payments) {
        payments.asMaps().forEach(payment -> silver.computeIfAbsent(partitionOf(partition), ignored -> new ArrayList<>())
                .add(GenericRecord.of(Map.of("id", payment.get("id"), "amount", Long.parseLong(payment.get("amount"))))));
    }

    @Given("a reactive mounter of gold that totals the payments of {string} into {string}")
    public void aReactiveMounterOfGoldThatTotalsThePayments(String partition, String dataMart) {
        MountAggregator<Object> total = records -> Stream.of(new PartitionedRecord<Object>(ALL_PAYMENTS,
                GenericRecord.of(Map.of("total", records.mapToLong(read -> read.record().getLong("amount")).sum()))));
        ReactiveMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>reactive(bus)
                .readingWith(target -> silver.entrySet().stream()
                        .filter(stored -> stored.getKey().contains(target.sourceKey()))
                        .flatMap(stored -> stored.getValue().stream().map(record -> new PartitionedRecord<>(stored.getKey(), record))))
                .aggregatingWith(total)
                .mount().onTarget(MountTarget.aggregation(dataMart, partitionOf(partition)))
                .writingWith(this::write).and()
                .checkpoint().windowPolicy(new AlwaysClosedWindowPolicy()).checkpoint(new NoOpMountCheckpoint()).and()
                .observability().withErrorRouter(new ErrorRouter().orElse((exception, context) -> { })).and();
        builder.build().start(mock(SpoolNode.StartPermit.class));
    }

    @When("the partition {string} of {string} is announced as mounted")
    public void thePartitionIsAnnouncedAsMounted(String partition, String dataMart) {
        bus.publish(EnvelopesMounted.builder().dataMart(dataMart).partitionKey(partitionOf(partition)).build());
    }

    @When("an envelope of {string} is persisted in bronze")
    public void anEnvelopeIsPersistedInBronze(String partition) {
        bus.publish(EnvelopePersisted.builder().idempotencyKey(IdempotencyKey.of("payment-9"))
                .partitionKey(partitionOf(partition)).build());
    }

    @When("the payment {string} of the country {string} is taken out of the quarantine")
    public void thePaymentOfTheCountryIsTakenOutOfTheQuarantine(String id, String country) {
        takeOutOfTheQuarantine("{\"id\":\"" + id + "\",\"payload\":{\"country\":\"" + country + "\"}}");
    }

    @When("the payment {string} without a country is taken out of the quarantine")
    public void thePaymentWithoutACountryIsTakenOutOfTheQuarantine(String id) {
        takeOutOfTheQuarantine("{\"id\":\"" + id + "\",\"payload\":{}}");
    }

    @Then("the payment {string} is rejected again")
    public void thePaymentIsRejectedAgain(String id) {
        assertThat(takenOutFailure).isInstanceOf(QuarantinedResultRejectedException.class);
        assertThat(writtenIds()).doesNotContain(id);
    }

    @Then("the payment {string} is not written")
    public void thePaymentIsNotWritten(String id) {
        assertThat(takenOutFailure).isNotNull();
        assertThat(writtenIds()).doesNotContain(id);
    }

    @Then("the partition {string} of {string} is announced as mounted once")
    public void thePartitionIsAnnouncedAsMountedOnce(String partition, String dataMart) {
        assertThat(announced).filteredOn(event -> event.dataMart().equals(dataMart)
                        && event.partitionKey().equals(silverPartitionOf(partition)))
                .hasSize(1);
    }

    @Then("no partition is announced as mounted")
    public void noPartitionIsAnnouncedAsMounted() {
        assertThat(announced).isEmpty();
    }

    @Then("nothing else is announced as mounted")
    public void nothingElseIsAnnouncedAsMounted() {
        assertThat(announced).hasSize(1);
    }

    @Then("gold has the total {long} in {string}")
    public void goldHasTheTotal(long total, String dataMart) {
        assertThat(written.getOrDefault("gold/" + dataMart, List.of())).singleElement()
                .satisfies(result -> assertThat(((GenericRecord) result.record()).getLong("total")).isEqualTo(total));
    }

    @Then("gold has nothing in {string}")
    public void goldHasNothingIn(String dataMart) {
        assertThat(written.getOrDefault("gold/" + dataMart, List.of())).isEmpty();
    }

    private void takeOutOfTheQuarantine(String result) {
        try {
            quarantinedResults.write(BRONZE, result.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            takenOutFailure = e;
        }
    }

    private List<Object> writtenIds() {
        return written.values().stream().flatMap(List::stream)
                .map(result -> ((GenericRecord) result.record()).toMap().get("id")).toList();
    }

    private void write(MountTarget target, Stream<PartitionedRecord<?>> results) {
        if (silverFails && target.qualifiedDataMart().startsWith("silver/")) {
            throw new IllegalStateException("Silver is not available");
        }
        results.forEach(result -> written.computeIfAbsent(target.qualifiedDataMart(), ignored -> new ArrayList<>()).add(result));
    }

    private static PartitionKey partitionOf(String partition) {
        return new PartitionKey(KeyEntries.join(List.of(partition.split("::"))));
    }

    private static PartitionKey silverPartitionOf(String partition) {
        String[] attributeAndValue = partition.split("=", 2);
        return PartitionKey.ofEntriesWithoutDate().with(ATTRIBUTE_PREFIX + attributeAndValue[0], attributeAndValue[1]).build();
    }

    private static PartitionedRecord<GenericRecord> bronzeRecordOf(String id, Map<String, Object> payload) {
        byte[] payloadBytes = RecordSerializerFactory.record().serialize(payload);
        Envelope envelope = new Envelope(IdempotencyKey.of(id), new EventMetadata()
                .set(EventMetadataKey.SOURCE, "payments").set(EventMetadataKey.CORRELATION_ID, id),
                MediaTypes.JSON, payloadBytes, EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope)));
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(payloadBytes));
        return new PartitionedRecord<>(BRONZE, GenericRecord.of(fields));
    }
}
