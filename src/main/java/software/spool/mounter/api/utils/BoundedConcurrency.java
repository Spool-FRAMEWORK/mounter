package software.spool.mounter.api.utils;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Runs blocking input and output, such as the reads and writes of a data lake, several at a time on a pool of its
 * own.
 *
 * <p>Stores like S3 have no multi-get or multi-put, so the only way to be faster is to have many requests in
 * flight. A parallel stream is no help: it uses the common pool of the JVM, sized for computation and shared with
 * everything else, and it has no limit that fits the store.</p>
 *
 * <p>The window is how many inputs can be running, or finished and not yet consumed, at the same time. It never
 * exceeds the number of threads, so nothing waits in a queue and memory stays bounded however long the input is.</p>
 *
 * <p>The boundary is explicit: the concurrent part is the input and output only. {@link #map} hands back a
 * sequential stream in input order, so whoever consumes it, an aggregator for instance, sees the same elements in
 * the same order as if they had been read one by one and needs no synchronization. Results are therefore identical
 * to a sequential run.</p>
 *
 * <p>An instance owns its threads. They are daemon threads named {@code spool-io-N}, and {@link #close()} stops them.</p>
 */
public final class BoundedConcurrency implements AutoCloseable {

    private static final AtomicInteger POOLS = new AtomicInteger();

    private final ExecutorService pool;
    private final int threads;
    private final int window;

    private BoundedConcurrency(int threads, int window) {
        if (threads < 1) throw new IllegalArgumentException("threads must be at least 1, got " + threads);
        if (window < 1) throw new IllegalArgumentException("window must be at least 1, got " + window);
        this.threads = threads;
        this.window = Math.min(window, threads);
        this.pool = Executors.newFixedThreadPool(threads, namedDaemonThreads(POOLS.incrementAndGet()));
    }

    /**
     * Creates a pool of the given number of threads, with a window as large as the pool.
     *
     * @param threads how many inputs can run at the same time
     * @return the concurrency
     * @throws IllegalArgumentException if {@code threads} is less than 1
     */
    public static BoundedConcurrency withThreads(int threads) {
        return new BoundedConcurrency(threads, threads);
    }

    /**
     * Creates a pool with a window smaller than its threads, to stay below what the store accepts.
     *
     * @param threads the size of the pool
     * @param window  how many inputs can be in flight; it is lowered to {@code threads} if it is larger
     * @return the concurrency
     * @throws IllegalArgumentException if {@code threads} or {@code window} is less than 1
     */
    public static BoundedConcurrency withThreads(int threads, int window) {
        return new BoundedConcurrency(threads, window);
    }

    /** @return the number of threads of the pool */
    public int threads() {
        return threads;
    }

    /** @return how many inputs can be in flight at the same time, never more than {@link #threads()} */
    public int window() {
        return window;
    }

    /**
     * Applies the function to every input, several at a time, and returns the results in input order.
     *
     * <p>Nothing runs until the stream is consumed, and the stream reads ahead by at most the window. If an
     * input fails, its exception is thrown when the stream reaches it, after the results that come before it.
     * Closing the stream cancels what is still running.</p>
     *
     * @param inputs the inputs, in the order the results are wanted
     * @param io     the blocking work for one input
     * @return a sequential stream of the results, in input order
     */
    public <T, R> Stream<R> map(List<T> inputs, Function<? super T, ? extends R> io) {
        Iterator<T> next = inputs.iterator();
        ArrayDeque<Future<R>> inFlight = new ArrayDeque<>();
        Spliterator<R> results = new Spliterators.AbstractSpliterator<>(inputs.size(), Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super R> action) {
                while (inFlight.size() < window && next.hasNext()) {
                    T input = next.next();
                    inFlight.add(pool.submit(() -> io.apply(input)));
                }
                Future<R> head = inFlight.poll();
                if (head == null) return false;
                action.accept(await(head));
                return true;
            }
        };
        return StreamSupport.stream(results, false).onClose(() -> inFlight.forEach(future -> future.cancel(true)));
    }

    /**
     * Runs the work for every input, several at a time, and returns when all of them are done.
     *
     * <p>The first failure is thrown and what is still running is cancelled. The stream is closed when the work
     * ends, whichever way.</p>
     *
     * @param inputs the inputs
     * @param io     the blocking work for one input
     */
    public <T> void forEach(Stream<T> inputs, Consumer<? super T> io) {
        ArrayDeque<Future<?>> inFlight = new ArrayDeque<>();
        try (inputs) {
            Iterator<T> next = inputs.iterator();
            while (next.hasNext()) {
                if (inFlight.size() == window) await(inFlight.poll());
                T input = next.next();
                inFlight.add(pool.submit(() -> io.accept(input)));
            }
            while (!inFlight.isEmpty()) await(inFlight.poll());
        } finally {
            inFlight.forEach(future -> future.cancel(true));
        }
    }

    /** Stops the threads of the pool. */
    @Override
    public void close() {
        pool.shutdownNow();
    }

    private static <R> R await(Future<R> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a concurrent read or write", e);
        }
    }

    private static ThreadFactory namedDaemonThreads(int pool) {
        AtomicInteger counter = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "spool-io-" + pool + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
