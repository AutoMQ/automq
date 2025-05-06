package kafka.automq.table.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

class PartitionWriteTaskContext {
    boolean requireReset;
    CompletableFuture<Void> lastFlushCf;

    final Writer writer;
    final EventLoops.EventLoopRef eventLoop;
    final ExecutorService flushExecutors;
    final WorkerConfig config;
    final long priority;
    long writeSize = 0;

    public PartitionWriteTaskContext(Writer writer, EventLoops.EventLoopRef eventLoop, ExecutorService flushExecutors, WorkerConfig config, long priority) {
        this.requireReset = false;
        this.lastFlushCf = CompletableFuture.completedFuture(null);

        this.writer = writer;
        this.eventLoop = eventLoop;
        this.flushExecutors = flushExecutors;
        this.config = config;
        this.priority = priority;
    }

    public void recordWriteSize(long size) {
        writeSize += size;
    }
}
