package software.spool.mounter.api.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedConcurrencyTest {

    private final List<BoundedConcurrency> opened = new ArrayList<>();

    private BoundedConcurrency threads(int threads) {
        BoundedConcurrency concurrency = BoundedConcurrency.withThreads(threads);
        opened.add(concurrency);
        return concurrency;
    }

    private BoundedConcurrency threads(int threads, int window) {
        BoundedConcurrency concurrency = BoundedConcurrency.withThreads(threads, window);
        opened.add(concurrency);
        return concurrency;
    }

    @AfterEach
    void closeThePools() {
        opened.forEach(BoundedConcurrency::close);
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void theWindowNeverExceedsTheNumberOfThreads() {
        assertThat(BoundedConcurrency.withThreads(2, 10).window()).isEqualTo(2);
        assertThat(BoundedConcurrency.withThreads(4, 2).window()).isEqualTo(2);
        assertThat(BoundedConcurrency.withThreads(3).window()).isEqualTo(3);
    }

    @Test
    void aPoolWithoutThreadsOrWindowIsRejected() {
        assertThatThrownBy(() -> BoundedConcurrency.withThreads(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BoundedConcurrency.withThreads(2, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mapGivesTheSameResultAsDoingItOneByOne() {
        List<Integer> inputs = IntStream.range(0, 200).boxed().toList();

        List<Integer> concurrent = threads(8).map(inputs, n -> n * n).toList();

        assertThat(concurrent).isEqualTo(inputs.stream().map(n -> n * n).toList());
    }

    @Test
    void mapKeepsTheInputOrderEvenWhenLaterInputsFinishFirst() {
        List<Integer> inputs = IntStream.range(0, 6).boxed().toList();

        List<Integer> results = threads(6).map(inputs, n -> {
            pause((6 - n) * 30L);
            return n;
        }).toList();

        assertThat(results).containsExactlyElementsOf(inputs);
    }

    @Test
    void mapReallyRunsInputsAtTheSameTime() throws Exception {
        CountDownLatch allRunning = new CountDownLatch(4);
        AtomicInteger released = new AtomicInteger();

        List<Integer> results = threads(4).map(List.of(1, 2, 3, 4), n -> {
            allRunning.countDown();
            try {
                if (allRunning.await(5, TimeUnit.SECONDS)) released.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return n;
        }).toList();

        assertThat(results).containsExactly(1, 2, 3, 4);
        assertThat(released.get()).isEqualTo(4);
    }

    @Test
    void mapNeverRunsMoreInputsAtOnceThanTheWindow() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostRunning = new AtomicInteger();

        threads(4, 2).map(IntStream.range(0, 40).boxed().toList(), n -> {
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            pause(5);
            running.decrementAndGet();
            return n;
        }).toList();

        assertThat(mostRunning.get()).isLessThanOrEqualTo(2);
    }

    @Test
    void mapDoesNothingUntilTheStreamIsConsumed() {
        AtomicInteger started = new AtomicInteger();

        threads(4).map(IntStream.range(0, 10).boxed().toList(), n -> started.incrementAndGet());

        pause(50);
        assertThat(started.get()).isZero();
    }

    @Test
    void mapDoesNotReadAheadOfTheWindow() {
        AtomicInteger started = new AtomicInteger();
        Stream<Integer> stream = threads(3).map(IntStream.range(0, 100).boxed().toList(), n -> started.incrementAndGet());

        stream.iterator().next();

        pause(50);
        assertThat(started.get()).isLessThanOrEqualTo(3);
    }

    @Test
    void mapRunsInItsOwnThreadsAndNotInTheCommonPool() {
        ConcurrentLinkedQueue<String> names = new ConcurrentLinkedQueue<>();

        threads(3).map(IntStream.range(0, 20).boxed().toList(), n -> names.add(Thread.currentThread().getName())).toList();

        assertThat(names).isNotEmpty().allMatch(name -> name.startsWith("spool-io-"));
    }

    @Test
    void mapOfNothingIsAnEmptyStream() {
        assertThat(threads(2).map(List.<Integer>of(), n -> n).toList()).isEmpty();
    }

    @Test
    void mapRethrowsTheFailureOfAnInputWhenItsResultIsReached() {
        Stream<Integer> stream = threads(3).map(List.of(1, 2, 3, 4, 5), n -> {
            if (n == 3) throw new IllegalStateException("boom on 3");
            return n;
        });
        List<Integer> delivered = new ArrayList<>();

        assertThatThrownBy(() -> stream.forEach(delivered::add))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom on 3");
        assertThat(delivered).containsExactly(1, 2);
    }

    @Test
    void forEachRunsEveryInput() {
        ConcurrentLinkedQueue<Integer> seen = new ConcurrentLinkedQueue<>();

        threads(4).forEach(IntStream.range(0, 100).boxed(), seen::add);

        assertThat(seen).hasSize(100).containsExactlyInAnyOrderElementsOf(IntStream.range(0, 100).boxed().toList());
    }

    @Test
    void forEachNeverRunsMoreInputsAtOnceThanTheWindow() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostRunning = new AtomicInteger();

        threads(4, 2).forEach(IntStream.range(0, 40).boxed(), n -> {
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            pause(5);
            running.decrementAndGet();
        });

        assertThat(mostRunning.get()).isLessThanOrEqualTo(2);
    }

    @Test
    void forEachReallyRunsInputsAtTheSameTime() throws Exception {
        CountDownLatch allRunning = new CountDownLatch(3);
        AtomicInteger released = new AtomicInteger();

        threads(3).forEach(Stream.of(1, 2, 3), n -> {
            allRunning.countDown();
            try {
                if (allRunning.await(5, TimeUnit.SECONDS)) released.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertThat(released.get()).isEqualTo(3);
    }

    @Test
    void forEachRethrowsTheFirstFailure() {
        assertThatThrownBy(() -> threads(2).forEach(Stream.of(1, 2, 3, 4), n -> {
            if (n == 2) throw new IllegalStateException("boom on 2");
        })).isInstanceOf(IllegalStateException.class).hasMessage("boom on 2");
    }

    @Test
    void forEachClosesTheStreamItWasGiven() {
        AtomicInteger closed = new AtomicInteger();
        Stream<Integer> inputs = Stream.of(1, 2, 3).onClose(closed::incrementAndGet);

        threads(2).forEach(inputs, n -> { });

        assertThat(closed.get()).isEqualTo(1);
    }
}
