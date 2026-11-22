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
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantineStatus;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.utils.media.MediaTypes;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.AttributesPartitioning;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.builder.ReactiveMounterBuilder;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class UnwritableResultQuarantineSteps {

    private static final PartitionKey BRONZE_PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "orders").build();
    private static final String ENVELOPE_PAYLOAD = "payload.";

    record Payment(String order, String country, long amount) {}

    private final Map<IdempotencyKey, PartitionedRecord<GenericRecord>> bronze = new LinkedHashMap<>();
    private final Map<String, List<PartitionedRecord<?>>> dataMarts = new LinkedHashMap<>();
    private final InMemoryQuarantineRepository quarantineRepository = new InMemoryQuarantineRepository();
    private final List<EnvelopeQuarantined> announced = new ArrayList<>();
    private final List<Exception> errors = new ArrayList<>();
    private InMemoryEventBus bus;
    private MountTarget target;
    private MountAggregator<Object> aggregator;
    private String attributePrefix = "";
    private boolean withQuarantine = true;
    private Mounter mounter;
    private QuarantinedRecord inspected;

    @Before
    public void listenToTheBus() {
        bus = new InMemoryEventBus();
        bus.subscribe(EnvelopeQuarantined.class, announced::add);
    }

    @Given("these orders are in bronze:")
    public void theseOrdersAreInBronze(DataTable orders) {
        for (Map<String, String> order : orders.asMaps()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("order", order.get("order"));
            payload.put("day", order.get("day"));
            if (order.get("country") != null) payload.put("country", order.get("country"));
            payload.put("amount", Long.parseLong(order.get("amount")));
            bronze.put(IdempotencyKey.of(order.get("order")), bronzeRecordOf(order.get("order"), payload));
        }
    }

    @Given("a mount that adds up the amount of each day and country into the gold data mart {string}")
    public void aMountThatAddsUpTheAmountOfEachDayAndCountry(String dataMart) {
        target = MountTarget.aggregation(dataMart, BRONZE_PARTITION);
        aggregator = UnwritableResultQuarantineSteps::dailyTotalsByCountry;
    }

    @Given("a mount without quarantine that adds up the amount of each day and country into the gold data mart {string}")
    public void aMountWithoutQuarantineThatAddsUpTheAmountOfEachDayAndCountry(String dataMart) {
        aMountThatAddsUpTheAmountOfEachDayAndCountry(dataMart);
        withQuarantine = false;
    }

    @Given("a mount that turns each order into a payment in the silver data mart {string}")
    public void aMountThatTurnsEachOrderIntoAPayment(String dataMart) {
        target = MountTarget.transformation(dataMart, BRONZE_PARTITION);
        aggregator = records -> records.map(read -> {
            GenericRecord payload = read.record().getNested("payload");
            return new PartitionedRecord<>(read.partitionKey(),
                    new Payment(payload.getString("order"), payload.has("country") ? payload.getString("country") : null,
                            payload.getLong("amount")));
        });
    }

    @Given("a mount that copies each order as it is to the silver data mart {string}")
    public void aMountThatCopiesEachOrderAsItIs(String dataMart) {
        target = MountTarget.transformation(dataMart, BRONZE_PARTITION);
        aggregator = records -> new PassThroughMountAggregator().aggregate(records)
                .map(read -> new PartitionedRecord<Object>(read.partitionKey(), read.record()));
        attributePrefix = ENVELOPE_PAYLOAD;
    }

    @Given("the data mart is partitioned by {string}")
    public void theDataMartIsPartitionedBy(String attribute) {
        ReactiveMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>reactive(bus)
                .readingWith(mountTarget -> List.copyOf(bronze.values()).stream())
                .aggregatingWith(aggregator)
                .mount().onTarget(target)
                .writingWith((mountTarget, results) -> results.forEach(result ->
                        dataMarts.computeIfAbsent(mountTarget.qualifiedDataMart(), ignored -> new ArrayList<>()).add(result)))
                .partitioningWith(AttributesPartitioning.of(attributePrefix + attribute)).and()
                .checkpoint().windowPolicy(new AlwaysClosedWindowPolicy()).checkpoint(new NoOpMountCheckpoint()).and()
                .observability().withErrorRouter(new ErrorRouter().orElse((exception, context) -> errors.add(exception))).and();
        if (withQuarantine) {
            DataLakeEnvelopeRemover remover = (partition, idempotencyKey) -> bronze.remove(idempotencyKey);
            builder.quarantine().in(quarantineRepository).removingFromDataLakeWith(remover);
        }
        mounter = builder.build();
        mounter.start(mock(SpoolNode.StartPermit.class));
    }

    @When("the mounter mounts bronze")
    public void theMounterMountsBronze() {
        bus.publish(EnvelopePersisted.builder()
                .correlationId("mount")
                .idempotencyKey(IdempotencyKey.of("order-1"))
                .partitionKey(BRONZE_PARTITION)
                .build());
    }

    @Then("{string} has the result of day {string} in the partition {string}")
    public void hasTheResultOfDayInThePartition(String dataMart, String day, String partition) {
        assertThat(written(dataMart, record -> day.equals(fieldOf(record, "day"))))
                .singleElement()
                .satisfies(result -> assertThat(result.partitionKey().value()).isEqualTo(partitionValue(partition)));
    }

    @Then("{string} has the payment of {string} in the partition {string}")
    public void hasThePaymentInThePartition(String dataMart, String order, String partition) {
        assertThat(written(dataMart, record -> order.equals(fieldOf(record, "order"))))
                .singleElement()
                .satisfies(result -> assertThat(result.partitionKey().value()).isEqualTo(partitionValue(partition)));
    }

    @Then("the result of day {string} is not in {string}")
    public void theResultOfDayIsNotIn(String day, String dataMart) {
        assertThat(written(dataMart, record -> day.equals(fieldOf(record, "day")))).isEmpty();
    }

    @Then("the payment of {string} is not in {string}")
    public void thePaymentIsNotIn(String order, String dataMart) {
        assertThat(written(dataMart, record -> order.equals(fieldOf(record, "order")))).isEmpty();
    }

    @Then("the result of day {string} is pending in quarantine because a partition attribute is missing")
    public void theResultOfDayIsPendingInQuarantine(String day) {
        inspected = quarantinedResult("day", day);
        assertPendingForAMissingPartitionAttribute();
    }

    @Then("the payment of {string} is pending in quarantine because a partition attribute is missing")
    public void thePaymentIsPendingInQuarantine(String order) {
        inspected = quarantinedResult("order", order);
        assertPendingForAMissingPartitionAttribute();
    }

    @Then("{string} is pending in quarantine because a partition attribute is missing")
    public void theOrderIsPendingInQuarantine(String order) {
        inspected = quarantineRepository.find(IdempotencyKey.of(order)).orElseThrow();
        assertPendingForAMissingPartitionAttribute();
    }

    @Then("its quarantine says the attribute {string} is missing")
    public void itsQuarantineSaysTheAttributeIsMissing(String attribute) {
        assertThat(inspected.violations()).anyMatch(violation -> violation.contains(attribute));
    }

    @Then("its quarantine says it belongs to the data mart {string}")
    public void itsQuarantineSaysItBelongsToTheDataMart(String dataMart) {
        assertThat(inspected.location()).isInstanceOfSatisfying(QuarantineLocation.DataMart.class,
                location -> assertThat(location.dataMart()).isEqualTo(dataMart));
    }

    @Then("its quarantine says it stayed in bronze")
    public void itsQuarantineSaysItStayedInBronze() {
        assertThat(inspected.location()).isEqualTo(QuarantineLocation.bronze(BRONZE_PARTITION));
    }

    @Then("its quarantine has a single entry")
    public void itsQuarantineHasASingleEntry() {
        assertThat(inspected.history()).hasSize(1);
    }

    @Then("a quarantine of the result of day {string} is announced")
    public void aQuarantineOfTheResultOfDayIsAnnounced(String day) {
        QuarantinedRecord result = quarantinedResult("day", day);
        assertThat(announced).anySatisfy(event -> {
            assertThat(event.idempotencyKey()).isEqualTo(result.idempotencyKey());
            assertThat(event.reason()).isEqualTo(QuarantineReason.MISSING_PARTITION_ATTRIBUTE);
            assertThat(event.location()).isEqualTo(result.location());
        });
    }

    @Then("{string} is no longer in bronze")
    public void isNoLongerInBronze(String order) {
        assertThat(bronze).doesNotContainKey(IdempotencyKey.of(order));
    }

    @Then("an error reports that the attribute {string} is missing")
    public void anErrorReportsThatTheAttributeIsMissing(String attribute) {
        assertThat(errors).anySatisfy(error -> assertThat(error).isInstanceOfSatisfying(MissingPartitionAttributeException.class,
                missing -> assertThat(missing.attribute()).isEqualTo(attribute)));
        assertThat(quarantineRepository.findAll()).isEmpty();
    }

    private void assertPendingForAMissingPartitionAttribute() {
        assertThat(inspected.status()).isEqualTo(QuarantineStatus.PENDING);
        assertThat(inspected.reason()).isEqualTo(QuarantineReason.MISSING_PARTITION_ATTRIBUTE);
    }

    private QuarantinedRecord quarantinedResult(String field, String value) {
        List<QuarantinedRecord> matching = quarantineRepository.findAll().stream()
                .filter(quarantined -> quarantined.location() instanceof QuarantineLocation.DataMart)
                .filter(quarantined -> value.equals(String.valueOf(PayloadDeserializerFactory.json().asMap()
                        .deserialize(quarantined.envelope().payload()).get(field))))
                .toList();
        assertThat(matching).hasSize(1);
        return matching.getFirst();
    }

    private List<PartitionedRecord<?>> written(String dataMart, Predicate<Object> matching) {
        return dataMarts.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("/" + dataMart))
                .flatMap(entry -> entry.getValue().stream())
                .filter(result -> matching.test(result.record()))
                .toList();
    }

    private String partitionValue(String partition) {
        String[] attributeAndValue = partition.split("=", 2);
        return PartitionKey.ofEntriesWithoutDate().with(attributePrefix + attributeAndValue[0], attributeAndValue[1]).build().value();
    }

    private static Object fieldOf(Object record, String field) {
        if (record instanceof GenericRecord genericRecord) {
            return genericRecord.has(field) ? genericRecord.get(field) : genericRecord.getNested("payload").get(field);
        }
        if (record instanceof Payment payment) return "order".equals(field) ? payment.order() : null;
        return null;
    }

    private static Stream<PartitionedRecord<Object>> dailyTotalsByCountry(Stream<PartitionedRecord<GenericRecord>> records) {
        Map<List<Object>, Long> totals = records.map(read -> read.record().getNested("payload"))
                .collect(Collectors.groupingBy(UnwritableResultQuarantineSteps::totalKeyOf, LinkedHashMap::new,
                        Collectors.summingLong(payload -> payload.getLong("amount"))));
        return totals.entrySet().stream().map(total -> {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("day", total.getKey().get(0));
            if (total.getKey().get(1) != null) fields.put("country", total.getKey().get(1));
            fields.put("amount", total.getValue());
            PartitionKey day = PartitionKey.ofEntriesWithoutDate().with("day", String.valueOf(total.getKey().get(0))).build();
            return new PartitionedRecord<Object>(day, GenericRecord.of(fields));
        });
    }

    private static List<Object> totalKeyOf(GenericRecord payload) {
        List<Object> key = new ArrayList<>();
        key.add(payload.getString("day"));
        key.add(payload.has("country") ? payload.getString("country") : null);
        return key;
    }

    private static PartitionedRecord<GenericRecord> bronzeRecordOf(String order, Map<String, Object> payload) {
        byte[] payloadBytes = RecordSerializerFactory.record().serialize(payload);
        Envelope envelope = new Envelope(IdempotencyKey.of(order), new EventMetadata()
                .set(EventMetadataKey.SOURCE, "orders").set(EventMetadataKey.CORRELATION_ID, order),
                MediaTypes.JSON, payloadBytes, EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope)));
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(payloadBytes));
        return new PartitionedRecord<>(BRONZE_PARTITION, GenericRecord.of(fields));
    }
}
