# Spool — Mounter

[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Mounter** is the **Spool** framework module responsible for **materializing partitioned data from a source store into a target data mart**. It handles scheduling, windowing, checkpointing, and optional parallel scaling.

---

## Table of Contents

- [What it does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [Polling Mounter](#polling-mounter)
  - [Reactive Mounter](#reactive-mounter)
  - [Incremental Mounter](#incremental-mounter)
  - [Scoped Mounter](#scoped-mounter)
  - [Parallel Scaling](#parallel-scaling)
- [Key Concepts](#key-concepts)
- [Built-in Adapters](#built-in-adapters)
- [License](#license)

---

## What it does

```
Source Store (data lake / event store)
      │
      ▼
PartitionedReader ──► MountAggregator ──► DataMartWriter
      │                                        │
PartitionWindowPolicy                    MountTarget
(is this partition closed?)             (output location)
      │
MountCheckpoint
(track what's already been mounted)
```

The Mounter reads closed partitions, applies an aggregation, and writes the result to a data mart. It tracks processed partitions so each is mounted exactly once.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>mounter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 21+**.

---

## Quick Start

```java
Mounter mounter = MounterBuilderFactory.polling(reader)
        .aggregatingWith(aggregator)
        .mount()
            .writingWith(writer)
            .onTarget(MountTarget.transformation("orders-by-customer", rootKey))
            .partitioningWith(partitioning)
            .and()
        .checkpoint()
            .windowPolicy(new AlwaysClosedWindowPolicy())
            .checkpoint(checkpoint)
            .and()
        .scheduling()
            .pollingWith(pollingPolicy)
            .and()
        .build();

SpoolNode node = SpoolNode.create()
        .register(mounter);

node.start();
```

---

## Examples

### Polling Mounter

Runs on a fixed schedule. On every tick it discovers all closed partitions and mounts them.

```java
Mounter mounter = MounterBuilderFactory.polling(reader)
        .aggregatingWith(aggregator)
        .mount()
            .writingWith(writer)
            .onTarget(MountTarget.transformation("orders-by-customer", rootKey))
            .partitioningWith(partitioning)
            .and()
        .checkpoint()
            .windowPolicy(new AlwaysClosedWindowPolicy())
            .checkpoint(checkpoint)
            .and()
        .scheduling()
            .pollingWith(pollingPolicy)
            .emittingWith(eventPublisher)
            .and()
        .build();
```

A full mount, one that is not incremental, writes every result again in each round. When a round ends well, each
partition it wrote keeps only the results of that round, so a total that changes from 35 to 40 does not leave the 35
behind, and a record that left bronze leaves silver too. A round that fails removes nothing, and a partition the round
does not write keeps what it had. The writer does it with `keepOnlyWhatWasWritten`, which the file system and S3
writers implement by noting the files they write; a writer of its own that does not implement it keeps everything, as
before. A partition of a data mart must be written by a single mounter, or each one would remove what the other wrote.

---

### Reactive Mounter

Mounts when what it reads has just been written: a `TRANSFORMATION` mount each time an `EnvelopePersisted` arrives on the bus, and an `AGGREGATION` mount each time an `EnvelopesMounted` says that a partition of silver holding its scope has been mounted. It ignores the mounts of gold, so a mounter of gold never reacts to its own results.

Every mounter with a publisher, polling or reactive, publishes an `EnvelopesMounted` for each partition of its data mart that received results in a round, with the data mart named with its layer (`silver/payments`). A round that fails, or writes nothing, publishes none, and neither does a result sent to the quarantine. When a result is taken out of the quarantine, `PartitioningQuarantinedResultWriter` writes it and then publishes an `EnvelopesMounted` for the partition it wrote to, so a reactive mounter of gold also mounts what comes back from the quarantine. A result that still cannot be written publishes nothing.

```java
Mounter mounter = MounterBuilderFactory.reactive(eventBus)
        .aggregatingWith(aggregator)
        .readingWith(reader)
        .mount()
            .writingWith(writer)
            .onTarget(MountTarget.aggregation("orders-summary", rootKey))
            .partitioningWith(partitioning)
            .and()
        .checkpoint()
            .windowPolicy(windowPolicy)
            .checkpoint(checkpoint)
            .and()
        .build();
```

---

### Incremental Mounter

An `AGGREGATION` mount can add new records to what it aggregated before instead of reading silver whole in each round ([ADR-027](https://linear.app/javier-castilla/document/adr-027-el-mounter-agrega-de-forma-incremental-con-un-agregador-que-c8a5f0f44866)). Its aggregator is an `IncrementalMountAggregator`: a fold with a state of its own, kept as JSON, so the state type must be one Jackson can write and read back, such as a record.

```java
public class AverageAmount implements IncrementalMountAggregator<AverageAmount.State, Average> {
    public record State(String country, long sum, long count) {}

    public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) {
        return PartitionKey.ofEntriesWithoutDate().with("country", record.record().getString("country")).build();
    }
    public State initial() { return new State("", 0, 0); }
    public State add(State state, GenericRecord record) {
        return new State(record.getString("country"), state.sum() + record.getLong("amount"), state.count() + 1);
    }
    public Average result(State state) { return new Average(state.country(), (double) state.sum() / state.count()); }
    public Class<State> stateType() { return State.class; }
}

Mounter mounter = MounterBuilderFactory.<Object, Average>polling(reader)
        .aggregatingWith(new AverageAmount())
        .mount()
            .onTarget(MountTarget.aggregation("average-by-country", new PartitionKey("source=payments")))
            .writingWith(writer)
            .removingWith(remover)
            .and()
        .checkpoint()
            .aggregatingIncrementally(aggregateMetadataRepository, partitionDiscovery)
            .and()
        .build();
```

Each round lists the partitions of the scope with the `PartitionDiscovery` and reads only the closed ones the data mart has not consumed. A partition is closed when its last date entry has ended and a margin has passed: `CheckpointFacet.DEFAULT_CLOSING_MARGIN` (15 minutes) unless `windowPolicy(...)` sets another, such as `new DatedPartitionWindowPolicy(clock, Duration.ofHours(1))`. A descriptor sets it with `read.closingMargin`. A scope with a partition without a date is aggregated again in full in every round.

Each consumed partition is kept with how many files it had when it was read, as the `PartitionDiscovery` counts them in `PartitionInfo.estimatedRecords`. When a partition already read has a different number of files, a record reached it late, as a result taken out of the quarantine always does, and the round aggregates the data mart again from the start with every closed partition and logs a warning naming the partitions that changed ([ADR-037](https://linear.app/javier-castilla/document/adr-037-un-dato-que-llega-tarde-a-una-particion-de-silver-ya-consumida-522132338c11)). A discovery that does not count files cannot see late records, and a partition consumed before its number was kept gets it in the next round.

The `AggregateMetadataRepository` keeps, for each aggregate, the partitions of silver it comes from, how many records it has added, when it changed and the state of its aggregator. The result of an aggregate that changes replaces its previous result, which the `DataMartRemover` removes, so the data mart has a single result per aggregate. The metadata is saved before anything is published, and a result that fails to be published is published in the next round.

---

### Partitioning and Quarantine

The aggregator returns each result as a `PartitionedRecord`, with its partition: `PassThroughMountAggregator` keeps the partition each record was read from, and an aggregator gives each result the partition of its group. `partitioningWith` decides where each result is written:

- `SameAsSourcePartitioning` (the default) writes it in the partition it comes with.
- `AttributesPartitioning.of("payload.country", "payload.day")` builds the partition from attributes of the result, a `GenericRecord` or a Java record. Each entry is named after the attribute path: `payload.country=ES::payload.day=2026-11-21`.

A result that lacks one of those attributes is not written. In a `TRANSFORMATION` mount with a quarantine, the record goes to the quarantine with the missing attribute and is removed from the data lake. Otherwise, a `MissingPartitionAttributeException` naming the attribute reaches the `ErrorRouter`.

The same quarantine keeps the records that fail validation on read:

```java
.mount()
    .onTarget(MountTarget.transformation("payments", rootKey))
    .writingWith(writer)
    .partitioningWith(AttributesPartitioning.of("payload.country"))
    .and()
.quarantine()
    .in(quarantineRepository)
    .removingFromDataLakeWith(dataLakeEnvelopeRemover)
    .and()
.validation()
    .enabled()
    .and()
```

---

### Scoped Mounter

Restrict the reader to a sub-partition with `.onScope()`:

```java
.mount()
    .onTarget(MountTarget.transformation("orders-by-customer", rootKey))
    .onScope(PartitionKey.of("tenant", "acme"))
    .writingWith(writer)
    .partitioningWith(partitioning)
    .and()
```

---

### Parallel Scaling

Process large data sets by dispatching sub-partitions in parallel.

```java
Mounter mounter = MounterBuilderFactory.polling(reader)
        .aggregatingWith(aggregator)
        .mount()
            .writingWith(writer)
            .onTarget(target)
            .partitioningWith(partitioning)
            .and()
        .checkpoint()
            .windowPolicy(windowPolicy)
            .checkpoint(checkpoint)
            .and()
        .scheduling()
            .pollingWith(pollingPolicy)
            .and()
        .scaling()
            .discoveringWith(discovery)
            .splittingWith(splitter)
            .scalingWith(ThresholdScalingPolicy.of(5, 100, 1_000_000))
            .dispatchingWith(new ThreadPoolPartitionDispatcher(executor))
            .and()
        .build();
```

`ThresholdScalingPolicy.of(localAbove, distributedAbove, splitAboveRecords)`:

| Condition | Mode |
|---|---|
| Partitions ≤ `localAbove` | Sequential |
| Partitions > `localAbove` or any partition > `splitAboveRecords` records | Local parallel |
| Partitions > `distributedAbove` | Distributed |

---

## Key Concepts

| Concept | Description |
|---|---|
| **`PartitionedReader`** | Reads raw records from a source partition. |
| **`DataMartWriter`** | Writes aggregated results to the target store. |
| **`MountAggregator<R>`** | Transforms a stream of records into typed results `R`, each with its partition. |
| **`MountPartitioning<R>`** | Decides the partition each result is written to, looking only at the result. |
| **`MountTarget`** | Identifies the output data mart and source partition. Factory methods: `transformation()`, `aggregation()`. |
| **`PartitionWindowPolicy`** | Decides whether a partition is closed and ready to process. |
| **`MountCheckpoint`** | Tracks which partitions have already been mounted. |
| **`MounterBuilderFactory`** | DSL entry point with `polling()` and `reactive()` presets. |

---

## Built-in Adapters

| Class | Description |
|---|---|
| `AlwaysClosedWindowPolicy` | Treats every partition as closed — suitable for non-windowed sources or tests. |
| `NoOpMountCheckpoint` | Never records a mounted partition — re-mounts everything on each run. |
| `DatedPartitionWindowPolicy` | Closes a partition once its last date entry has ended and a margin has passed. |
| `InMemoryAggregateMetadataRepository` | Keeps the metadata of the aggregates in memory, for tests. |
| `PassThroughMountAggregator` | Emits each record unchanged, with the partition it was read from. |
| `SameAsSourcePartitioning` | Writes each result in the partition it comes with. |
| `AttributesPartitioning` | Builds the partition from attributes of the result. |
| `ThresholdScalingPolicy` | Selects sequential / local parallel / distributed based on thresholds. |
| `SequentialPartitionDispatcher` | Processes mount units one at a time in the calling thread. |
| `ThreadPoolPartitionDispatcher` | Processes mount units in parallel using a provided `Executor`. |

---

## License

Distributed under the [GNU Affero General Public License v3.0](LICENSE).
