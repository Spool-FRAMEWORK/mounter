package software.spool.mounter.api.adapter;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionDispatcher;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

public class ThreadPoolPartitionDispatcher implements PartitionDispatcher {
    private final Executor executor;

    public ThreadPoolPartitionDispatcher(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void dispatch(List<MountTarget> units, Handler<MountTarget> worker) {
        List<CompletableFuture<Void>> futures = units.stream()
                .map(unit -> CompletableFuture.runAsync(() -> invoke(worker, unit), executor))
                .toList();
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SpoolException se) throw se;
            if (cause instanceof RuntimeException re) throw re;
            throw e;
        }
    }

    private void invoke(Handler<MountTarget> worker, MountTarget target) {
        try {
            worker.handle(target);
        } catch (SpoolException e) {
            throw new RuntimeException(e);
        }
    }
}
