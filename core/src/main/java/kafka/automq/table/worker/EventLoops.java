package kafka.automq.table.worker;

import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLoops {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventLoops.class);
    private final EventLoopWrapper[] eventLoops;

    private final long[] lastRecordNanoTimes;
    private final long[] lastTotalBusyTimes;

    public EventLoops(EventLoop[] eventLoops) {
        this.eventLoops = new EventLoopWrapper[eventLoops.length];
        for (int i = 0; i < eventLoops.length; i++) {
            this.eventLoops[i] = new EventLoopWrapper(eventLoops[i]);
        }
        this.lastRecordNanoTimes = new long[eventLoops.length];
        long now = System.nanoTime();
        for (int i = 0; i < eventLoops.length; i++) {
            lastRecordNanoTimes[i] = now;
        }
        this.lastTotalBusyTimes = new long[eventLoops.length];
        Threads.COMMON_SCHEDULER.scheduleAtFixedRate(this::logStats, 1, 1, TimeUnit.MINUTES);
    }

    public int size() {
        return eventLoops.length;
    }

    public EventLoopRef leastLoadEventLoop() {
        int leastLoad = Integer.MAX_VALUE;
        int leastLoadIndex = -1;
        for (int i = 0; i < eventLoops.length; i++) {
            int load = eventLoops[i].inflight.get();
            if (load < leastLoad) {
                leastLoad = load;
                leastLoadIndex = i;
            }
        }
        return new EventLoopRef(eventLoops[leastLoadIndex]);
    }

    void logStats() {
        StringBuilder sb = new StringBuilder();
        long now = System.nanoTime();
        sb.append("EventLoops stats: ");
        for (int i = 0; i < eventLoops.length; i++) {
            EventLoopWrapper eventLoop = eventLoops[i];
            long totalBusyTime = eventLoop.totalBusyTime;
            long lastTotalBusyTime = lastTotalBusyTimes[i];
            long busyTimeDelta = Math.max(totalBusyTime - lastTotalBusyTime, 0);
            lastTotalBusyTimes[i] = totalBusyTime;

            long runningTaskStartTime = eventLoop.runningTaskStartTime;
            long recordNanoTime = runningTaskStartTime == -1 ? now : runningTaskStartTime;
            long lastRecordNanoTime = lastRecordNanoTimes[i];
            lastRecordNanoTimes[i] = recordNanoTime;

            long elapseDelta = Math.max(recordNanoTime - lastRecordNanoTime, 1);
            sb.append(eventLoop.eventLoop.getName()).append(String.format(": %.1f", (double) busyTimeDelta / elapseDelta * 100)).append("%, ");
        }
        LOGGER.info(sb.toString());
    }

    public static class EventLoopWrapper {
        final EventLoop eventLoop;

        final PriorityBlockingQueue<PriorityTask> tasks = new PriorityBlockingQueue<>();
        final AtomicInteger inflight = new AtomicInteger();
        volatile long runningTaskStartTime = -1;
        volatile long totalBusyTime = 0;

        public EventLoopWrapper(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }
    }

    public static class EventLoopRef extends AbstractReferenceCounted implements Executor {
        private final EventLoopWrapper eventLoop;
        // visible for testing
        final AtomicInteger inflight;

        public EventLoopRef(EventLoopWrapper eventLoop) {
            this.eventLoop = eventLoop;
            this.inflight = eventLoop.inflight;
            inflight.incrementAndGet();
        }

        @Override
        protected void deallocate() {
            inflight.decrementAndGet();
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
        }

        @Override
        public void execute(@NotNull Runnable command) {
            execute(command, 0);
        }

        public CompletableFuture<Void> execute(@NotNull Runnable command, long priority) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            eventLoop.tasks.add(new PriorityTask(() -> {
                eventLoop.runningTaskStartTime = System.nanoTime();
                try {
                    command.run();
                } finally {
                    //noinspection NonAtomicOperationOnVolatileField
                    eventLoop.totalBusyTime += System.nanoTime() - eventLoop.runningTaskStartTime;
                    eventLoop.runningTaskStartTime = -1L;
                }
            }, (int) priority, cf));
            eventLoop.eventLoop.execute(() -> {
                CompletableFuture<Void> headTaskCf = null;
                try {
                    PriorityTask headTask = eventLoop.tasks.take();
                    headTaskCf = headTask.cf;
                    headTask.task.run();
                    headTask.cf.complete(null);
                } catch (Throwable e) {
                    if (headTaskCf != null) {
                        headTaskCf.completeExceptionally(e);
                    }
                    throw new RuntimeException(e);
                }
            });
            return cf;
        }
    }

    record PriorityTask(Runnable task, int priority, CompletableFuture<Void> cf) implements Comparable<PriorityTask> {
        @Override
        public int compareTo(PriorityTask o) {
            return Integer.compare(priority, o.priority);
        }
    }

}
