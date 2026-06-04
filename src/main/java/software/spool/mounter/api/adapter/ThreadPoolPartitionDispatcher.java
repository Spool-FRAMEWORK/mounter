package software.spool.mounter.api.adapter;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionDispatcher;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void invoke(Handler<MountTarget> worker, MountTarget target) {
        try {
            worker.handle(target);
        } catch (SpoolException e) {
            throw new RuntimeException(e);
        }
    }
}
