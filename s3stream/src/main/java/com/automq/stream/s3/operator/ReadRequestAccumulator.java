/*
 * Copyright 2026, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Owns per-storage batching and routing on a shared daemon EventLoop. Blocking GET
 * submission and concurrency quota belong to the nonblocking storage reader. Caller futures complete
 * on a shared single-thread callback executor. User callbacks must offload blocking work.
 * Timer callbacks only enqueue events. Neither the shared EventLoop nor the supplied scheduler
 * is owned here; closing one accumulator leaves them available to other storage clients.
 * Each merged read synchronizes its own progress, attachment and terminal state. Progress listeners
 * retain chunk slices synchronously; caller delivery remains asynchronous.
 */
final class ReadRequestAccumulator {
    static final EventLoop EVENT_LOOP = new EventLoop("s3-read-event-loop", true);
    static final Executor CALLBACK_EXECUTOR = Executors.newSingleThreadExecutor(
        ThreadUtils.createThreadFactory("s3-read-callback", true));
    private final ScheduledExecutorService scheduler;
    private final LongSupplier nanoTime;
    private final ReadMergePlanner planner;
    private final boolean autoFlushEnabled;
    private final Reader reader;
    private final Logger logger;
    private final Executor completionDispatcher;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final Map<String, PendingBatch> pending = new LinkedHashMap<>();
    private final Map<String, List<MergedReadTask>> inflight = new HashMap<>();
    private boolean closed;

    ReadRequestAccumulator(float maxSparsity, boolean autoFlushEnabled, Reader reader, Logger logger) {
        this(Threads.COMMON_SCHEDULER, System::nanoTime, maxSparsity, autoFlushEnabled, reader, CALLBACK_EXECUTOR, logger);
    }

    ReadRequestAccumulator(ScheduledExecutorService scheduler, LongSupplier nanoTime,
        float maxSparsity, boolean autoFlushEnabled, Reader reader, Executor completionDispatcher, Logger logger) {
        this.scheduler = scheduler;
        this.nanoTime = nanoTime;
        this.autoFlushEnabled = autoFlushEnabled;
        this.reader = reader;
        this.completionDispatcher = completionDispatcher;
        this.logger = logger;
        this.planner = new ReadMergePlanner(maxSparsity,
            (tasks, sparsity) -> new MergedReadTask(tasks, sparsity, this.reader, this.logger,
                completionDispatcher, task -> EVENT_LOOP.execute(() -> {
                    List<MergedReadTask> reads = inflight.get(task.objectPath);
                    if (reads != null) {
                        reads.remove(task);
                        if (reads.isEmpty()) {
                            inflight.remove(task.objectPath);
                        }
                    }
                })));
    }

    /**
     * Enqueue a logical read; its result is delivered through {@link ReadTask#cf}.
     * Reads arriving after close bypass batching and are handed to the storage reader.
     */
    public void add(ReadTask task) {
        Runnable bypass = () -> reader.read(task.options, task.objectPath, task.start, task.end, (chunks, size) -> { })
            .whenComplete((data, ex) -> completionDispatcher.execute(
                () -> MergedReadTask.handleReadCompleted(List.of(task), task.start, data, ex)));
        if (closeStarted.get()) {
            bypass.run();
            return;
        }
        EVENT_LOOP.execute(() -> {
            if (closed) {
                bypass.run();
                return;
            }
            for (MergedReadTask candidate : inflight.getOrDefault(task.objectPath, List.of())) {
                if (candidate.tryAdd(task)) {
                    return;
                }
            }
            long enqueueNanos = nanoTime.getAsLong();
            pending.computeIfAbsent(task.objectPath, path -> new PendingBatch(path, enqueueNanos))
                .add(task, enqueueNanos);
        });
    }

    /**
     * Seal pending batches and submit their GETs to the nonblocking storage reader.
     * The returned future does not wait for permits or physical GET completion.
     */
    public CompletableFuture<Void> flush() {
        // Complete control futures off the shared loop, including when planning fails.
        return EVENT_LOOP.submit(() -> new ArrayList<>(pending.values()).forEach(PendingBatch::close))
            .whenCompleteAsync((ignored, error) -> { });
    }

    /**
     * Stop batching and deliver terminal results for this instance's waiting callers asynchronously.
     * Repeated calls return the same future. Late physical responses are released when they arrive;
     * closing does not wait for those GETs or stop any of the shared executors.
     */
    public CompletableFuture<Void> close() {
        if (!closeStarted.compareAndSet(false, true)) {
            return closeFuture;
        }
        EVENT_LOOP.submit(() -> {
            closed = true;
            List<MergedReadTask> active = inflight.values().stream().flatMap(List::stream).toList();
            // Storage admission is already closed, so pending reads can be handed directly to it.
            new ArrayList<>(pending.values()).forEach(PendingBatch::close);
            active.forEach(MergedReadTask::close);
        }).thenCompose(ignored -> EVENT_LOOP.submit(() -> { }))
            .whenCompleteAsync((ignored, error) -> {
                if (error == null) {
                    closeFuture.complete(null);
                } else {
                    closeFuture.completeExceptionally(error);
                }
            }, completionDispatcher);
        return closeFuture;
    }

    /** Starts a physical read without blocking, including while storage quota is exhausted. */
    @FunctionalInterface
    interface Reader {
        /** Returns a non-null future and reports read failures through it rather than throwing synchronously. */
        CompletableFuture<ByteBuf> read(ReadOptions options, String path, long start, long end, ReadProgressListener listener);
    }

    /** Owns one object's enqueue deadlines and seals its requests on the EventLoop. */
    private final class PendingBatch {
        private static final long IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
        private static final long MAX_LINGER_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
        private final String path;
        private final long firstEnqueueNanos;
        private long lastEnqueueNanos;
        private final List<ReadTask> tasks = new ArrayList<>();
        private ScheduledFuture<?> timer;

        private PendingBatch(String path, long firstEnqueueNanos) {
            this.path = path;
            this.firstEnqueueNanos = firstEnqueueNanos;
            this.lastEnqueueNanos = firstEnqueueNanos;
        }

        private void add(ReadTask task, long enqueueNanos) {
            lastEnqueueNanos = enqueueNanos;
            tasks.add(task);
            if (autoFlushEnabled && tasks.size() == 1) {
                schedule(IDLE_NANOS);
            }
        }

        private void schedule(long delay) {
            timer = scheduler.schedule(() -> EVENT_LOOP.execute(this::onTimer), delay, TimeUnit.NANOSECONDS);
        }

        private void onTimer() {
            if (closed || pending.get(path) != this) {
                return;
            }
            long now = nanoTime.getAsLong();
            long delay = Math.min(IDLE_NANOS - (now - lastEnqueueNanos),
                MAX_LINGER_NANOS - (now - firstEnqueueNanos));
            if (delay > 0) {
                schedule(delay);
                return;
            }
            close();
        }

        private void close() {
            if (!pending.remove(path, this)) {
                return;
            }
            if (timer != null) {
                timer.cancel(false);
            }
            List<MergedReadTask> plans = planner.plan(tasks);
            if (!plans.isEmpty()) {
                inflight.computeIfAbsent(path, ignored -> new ArrayList<>()).addAll(plans);
            }
            plans.forEach(MergedReadTask::start);
        }
    }

    static final class ReadTask {
        final ReadOptions options;
        final String objectPath;
        final long start;
        final long end;
        final CompletableFuture<ByteBuf> cf;

        ReadTask(ReadOptions options, String objectPath, long start, long end, CompletableFuture<ByteBuf> cf) {
            this.options = options;
            this.objectPath = objectPath;
            this.start = start;
            this.end = end;
            this.cf = cf;
        }
    }

    /**
     * Owns one GET's waiting callers and terminal state under this instance's monitor.
     * Completion dispatch must enqueue asynchronously without running user callbacks under the monitor.
     * Terminal delivery is enqueued before notifying the owner to remove this task from routing.
     */
    static final class MergedReadTask {
        final String objectPath;
        final long start;
        final long end;
        final double dataSparsityRate;
        final List<ReadTask> readTasks;
        private final ReadOptions dispatchOptions;
        private final Reader reader;
        private final Logger logger;
        private final Executor completionDispatcher;
        private final Consumer<MergedReadTask> onComplete;
        private boolean finished;
        private boolean needSort = true;
        MergedReadTask(List<ReadTask> tasks, double sparsity, Reader reader, Logger logger,
            Executor completionDispatcher, Consumer<MergedReadTask> onComplete) {
            this.reader = reader;
            this.logger = logger;
            this.completionDispatcher = completionDispatcher;
            this.onComplete = onComplete;
            objectPath = tasks.get(0).objectPath;
            start = tasks.stream().mapToLong(t -> t.start).min().orElseThrow();
            end = tasks.stream().anyMatch(t -> t.end == ObjectStorage.RANGE_READ_TO_END)
                ? ObjectStorage.RANGE_READ_TO_END : tasks.stream().mapToLong(t -> t.end).max().orElseThrow();
            readTasks = new ArrayList<>(tasks);
            dataSparsityRate = sparsity;
            dispatchOptions = aggregateOptions(tasks);
        }

        // Called once on the EventLoop, immediately after registration in inflight.
        void start() {
            if (logger.isDebugEnabled()) {
                logger.debug("merge read: {}, {}-{}, size: {}, sparsityRate: {}",
                    objectPath, start, end, end - start, dataSparsityRate);
            }
            reader.read(options(), objectPath, start, end, this::onProgress).whenComplete(this::complete);
        }

        synchronized boolean tryAdd(ReadTask task) {
            if (finished || end == ObjectStorage.RANGE_READ_TO_END || task.end == ObjectStorage.RANGE_READ_TO_END
                || task.start < start || task.end > end) {
                return false;
            }
            readTasks.add(task);
            needSort = true;
            return true;
        }

        synchronized void onProgress(List<ByteBuf> chunks, int length) {
            if (finished || readTasks.isEmpty()) {
                return;
            }
            if (needSort) {
                readTasks.sort(Comparator.comparingLong(t -> t.end == ObjectStorage.RANGE_READ_TO_END ? Long.MAX_VALUE : t.end));
                needSort = false;
            }
            int count = 0;
            long rangeStart = Long.MAX_VALUE;
            long readyEnd = start + length;
            while (count < readTasks.size()) {
                ReadTask caller = readTasks.get(count);
                if (caller.end == ObjectStorage.RANGE_READ_TO_END || caller.end > readyEnd) {
                    break;
                }
                rangeStart = Math.min(rangeStart, caller.start);
                count++;
            }
            if (count == 0) {
                return;
            }
            long rangeEnd = readTasks.get(count - 1).end;
            ByteBuf response = retainRange(chunks, length, Math.toIntExact(rangeStart - start),
                Math.toIntExact(rangeEnd - start));
            List<ReadTask> callers = new ArrayList<>(readTasks.subList(0, count));
            readTasks.subList(0, count).clear();
            long responseStart = rangeStart;
            completionDispatcher.execute(() -> handleReadCompleted(callers, responseStart, response, null));
        }

        synchronized void complete(ByteBuf data, Throwable exception) {
            if (finished) {
                if (data != null) {
                    data.release();
                }
                return;
            }
            finished = true;
            List<ReadTask> callers = new ArrayList<>(readTasks);
            readTasks.clear();
            completionDispatcher.execute(() -> handleReadCompleted(callers, start, data, exception));
            onComplete.accept(this);
        }

        void close() {
            complete(null, new IllegalStateException("Object storage is closed"));
        }

        ReadOptions options() {
            return dispatchOptions.copy();
        }

        // Build a fixed component table for the requested span; the borrowed list never escapes this call.
        private static CompositeByteBuf retainRange(List<ByteBuf> chunks, int length, int start, int end) {
            int first = chunks.size();
            int chunkStart = length;
            while (chunkStart > start) {
                chunkStart -= chunks.get(--first).readableBytes();
            }
            CompositeByteBuf result = Unpooled.compositeBuffer(Integer.MAX_VALUE);
            try {
                for (int i = first; chunkStart < end; i++) {
                    ByteBuf chunk = chunks.get(i);
                    int from = Math.max(start, chunkStart);
                    int to = Math.min(end, chunkStart + chunk.readableBytes());
                    result.addComponent(true, chunk.retainedSlice(chunk.readerIndex() + from - chunkStart, to - from));
                    chunkStart += chunk.readableBytes();
                }
                return result;
            } catch (Throwable ex) {
                result.release();
                throw ex;
            }
        }

        private static void completeReady(List<ReadTask> callers, List<ByteBuf> responses) {
            for (int i = 0; i < callers.size(); i++) {
                ByteBuf response = responses.get(i);
                if (!callers.get(i).cf.complete(response)) {
                    response.release();
                }
            }
        }

        private static ReadOptions aggregateOptions(List<ReadTask> tasks) {
            ReadOptions options = tasks.get(0).options.copy();
            for (ReadTask task : tasks) {
                if (task.options.source() != options.source()) {
                    options.source(S3RequestSource.MIXED);
                }
                if (task.options.readPurpose() != options.readPurpose()) {
                    options.readPurpose(S3ReadPurpose.MIXED);
                }
            }
            return options;
        }

        static void handleReadCompleted(List<ReadTask> tasks, long start, ByteBuf data, Throwable exception) {
            if (exception != null) {
                if (data != null) {
                    data.release();
                }
                tasks.forEach(t -> t.cf.completeExceptionally(exception));
                return;
            }
            List<ByteBuf> slices = new ArrayList<>();
            try {
                for (ReadTask task : tasks) {
                    int offset = Math.toIntExact(task.start - start);
                    int length = task.end == ObjectStorage.RANGE_READ_TO_END ? data.readableBytes() - offset
                        : Math.toIntExact(task.end - task.start);
                    slices.add(data.retainedSlice(data.readerIndex() + offset, length));
                }
            } catch (Throwable ex) {
                slices.forEach(ByteBuf::release);
                tasks.forEach(t -> t.cf.completeExceptionally(ex));
                return;
            } finally {
                data.release();
            }
            // Retain every slice before completing any caller, which may synchronously release its slice.
            completeReady(tasks, slices);
        }
    }
}
