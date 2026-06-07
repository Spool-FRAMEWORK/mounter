# Spool — Mounter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
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
            .partitioningWith(schema)
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
            .partitioningWith(schema)
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

---

### Reactive Mounter

Triggers a mount every time an `EnvelopePersisted` event arrives on the bus.

```java
Mounter mounter = MounterBuilderFactory.reactive(eventBus)
        .aggregatingWith(aggregator)
        .readingWith(reader)
        .mount()
            .writingWith(writer)
            .onTarget(MountTarget.aggregation("orders-summary", rootKey))
            .partitioningWith(schema)
            .and()
        .checkpoint()
            .windowPolicy(windowPolicy)
            .checkpoint(checkpoint)
            .and()
        .build();
```

---

### Incremental Mounter

For delta/merge use cases — maintains running state and only processes new partitions.

```java
Pipeline<PipelineContext, PipelineContext> pipeline = Pipeline.<PipelineContext>start()
        .add(new FetchPendingPartitionsStep(reader, windowPolicy, cursor))
        .add(new ReadCurrentStateStep<>(reader))
        .add(new MergePartitionsStep<>(reader, aggregator))
        .add(new WriteIncrementalResultStep<>(writer))
        .add(new AdvanceCursorStep(cursor));

IncrementalMountHandler<Input, Output> handler = new IncrementalMountHandler<>(pipeline);
```

On each call: fetches new closed partitions → reads current state → merges → writes → advances the cursor.

---

### Scoped Mounter

Restrict the reader to a sub-partition with `.onScope()`:

```java
.mount()
    .onTarget(MountTarget.transformation("orders-by-customer", rootKey))
    .onScope(PartitionKey.of("tenant", "acme"))
    .writingWith(writer)
    .partitioningWith(schema)
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
            .partitioningWith(schema)
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
| **`MountAggregator<R>`** | Transforms a stream of records into a typed result `R`. |
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
| `NoOpMountCursor` | Reports no processed sources — processes all partitions on each run. |
| `PassThroughMountAggregator` | Emits each record unchanged as the aggregation result. |
| `ThresholdScalingPolicy` | Selects sequential / local parallel / distributed based on thresholds. |
| `SequentialPartitionDispatcher` | Processes mount units one at a time in the calling thread. |
| `ThreadPoolPartitionDispatcher` | Processes mount units in parallel using a provided `Executor`. |

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
