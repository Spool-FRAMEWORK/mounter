# Mounter

Part of the [Spool Framework](https://github.com/Spool-FRAMEWORK). Mounter materializes partitioned data from a source store (data lake, event store) into a target store (data mart), handling scheduling, windowing, checkpointing, and optional parallel scaling.

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>mounter</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Core Concepts

| Concept | Interface | Role |
|---|---|---|
| `PartitionedReader` | Port | Reads raw records from a source partition |
| `DataMartWriter` | Port | Writes the aggregated result to the target store |
| `MountAggregator<R>` | Port | Transforms a stream of records into a typed result `R` |
| `MountTarget` | Value | Identifies the target data mart + source partition being mounted |
| `PartitionWindowPolicy` | Port | Decides whether a partition is closed and ready to process |
| `MountCheckpoint` | Port | Tracks which partitions have already been mounted |
| `MountPartitionSchema<R>` | Port | Maps an aggregated record `R` to its output partition key |

### Mount modes

`MountTarget` is created with a **mode** that qualifies how the output is stored:

```java
// Record-per-partition transformation
MountTarget target = MountTarget.transformation("orders-by-customer", sourceKey);

// Aggregated single-file per partition
MountTarget target = MountTarget.aggregation("orders-summary", sourceKey);

// Explicit output file extension
MountTarget target = MountTarget.transformation("orders-by-customer", sourceKey, "parquet");
```

---

## Polling Mounter

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

mounter.start();
// ...
mounter.stop();
```

### Scoping

Restrict the reader to a sub-partition with `.onScope()`:

```java
.mount()
    .onTarget(MountTarget.transformation("orders-by-customer", rootKey))
    .onScope(PartitionKey.of("tenant", "acme"))
    .writingWith(writer)
    .partitioningWith(schema)
    .and()
```

### Custom key extractor

Use a `PartitionKeyExtractor` instead of a schema when the partition key depends on the mount target:

```java
.mount()
    .partitioningWith((source, output, target) -> target.sourceKey())
    .and()
```

### Observability

Route errors to a custom handler:

```java
.observability()
    .withErrorRouter(error -> log.error("Mount failed", error))
    .and()
```

---

## Reactive Mounter

Triggers a mount every time an `EnvelopePersisted` event arrives on the bus. Use it when you want near-real-time materialization without polling.

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

mounter.start();
```

---

## Incremental Mounter

For delta/merge use cases where you maintain a running state and only process new partitions on each run. Build an `IncrementalMountHandler` by composing pipeline steps:

```java
import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.mounter.internal.control.IncrementalMountHandler;
import software.spool.mounter.internal.control.steps.*;

Pipeline<PipelineContext, PipelineContext> pipeline = Pipeline.<PipelineContext>start()
    .add(new FetchPendingPartitionsStep(reader, windowPolicy, cursor))
    .add(new ReadCurrentStateStep<>(reader))
    .add(new MergePartitionsStep<>(reader, aggregator))
    .add(new WriteIncrementalResultStep<>(writer))
    .add(new AdvanceCursorStep(cursor));

IncrementalMountHandler<Input, Output> handler = new IncrementalMountHandler<>(pipeline);
```

The `MountCursor` tracks which source partitions have been processed. On each call to `handle(target)`:

1. Fetches pending source partitions not yet in the cursor, filtered by `PartitionWindowPolicy`.
2. Reads the current materialized state.
3. Merges each new partition into the current state via `MergeableMountAggregator`.
4. Writes the merged result.
5. Advances the cursor for all processed partitions.

If there are no new closed partitions, no write occurs.

---

## Scaling

Add the `.scaling()` facet to the polling mounter to handle large data sets. Mounter will discover sub-partitions, optionally split large ones into slices, and dispatch them in parallel.

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

`ThresholdScalingPolicy.of(localAbove, distributedAbove, splitAboveRecords)` selects the execution mode automatically:

| Condition | Mode |
|---|---|
| Partitions ≤ `localAbove` | Sequential |
| Partitions > `localAbove` or any partition > `splitAboveRecords` records | Local parallel |
| Partitions > `distributedAbove` | Distributed |

For separate local and distributed dispatchers:

```java
.scaling()
    .dispatchingWith(localDispatcher, distributedDispatcher)
    .and()
```

---

## Built-in Adapters

Ready-to-use implementations for common scenarios:

| Class | Description |
|---|---|
| `AlwaysClosedWindowPolicy` | Treats every partition as closed — suitable for non-windowed sources or tests |
| `NoOpMountCheckpoint` | Never records a mounted partition — re-mounts everything on each run |
| `NoOpMountCursor` | Reports no processed sources — processes all partitions on each run |
| `PassThroughMountAggregator` | Emits each record unchanged as the aggregation result |
| `ThresholdScalingPolicy` | Selects sequential / local parallel / distributed mode based on partition and record count thresholds |
| `SequentialPartitionDispatcher` | Processes mount units one at a time in the calling thread |
| `ThreadPoolPartitionDispatcher` | Processes mount units in parallel using a provided `Executor` |
| `MounterErrorRouter` | Default error router that publishes errors to the event bus |

---

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
