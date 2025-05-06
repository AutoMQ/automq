/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Utils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCounted;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.model.S3Exception;

@SuppressWarnings("this-escape")
public abstract class AbstractObjectStorage implements ObjectStorage {
    private static final int MAX_INFLIGHT_FAST_RETRY_COUNT = 5;

    private static final AtomicInteger INDEX = new AtomicInteger(-1);
    private static final int DEFAULT_CONCURRENCY_PER_CORE = 25;
    private static final int MIN_CONCURRENCY = 50;
    private static final int MAX_CONCURRENCY = 1000;
    private static final long DEFAULT_UPLOAD_PART_COPY_TIMEOUT = TimeUnit.MINUTES.toMillis(2);
    private final String threadPrefix;
    final Logger logger;
    private final float maxMergeReadSparsityRate;
    private final int currentIndex;
    private final Semaphore inflightReadLimiter;
    private final Semaphore inflightWriteLimiter;
    private final List<AbstractObjectStorage.ReadTask> waitingReadTasks = new LinkedList<>();
    protected final NetworkBandwidthLimiter networkInboundBandwidthLimiter;
    protected final NetworkBandwidthLimiter networkOutboundBandwidthLimiter;
    protected final ExecutorService writeLimiterCallbackExecutor;
    private final ExecutorService readCallbackExecutor;
    private final ExecutorService writeCallbackExecutor;
    final ScheduledExecutorService scheduler;
    private final HashedWheelTimer fastRetryTimer;

    private final DeleteObjectsAccumulator deleteObjectsAccumulator;
    final boolean checkS3ApiMode;
    protected final BucketURI bucketURI;

    private final S3LatencyCalculator s3LatencyCalculator;
    private final Semaphore fastRetryPermit = new Semaphore(MAX_INFLIGHT_FAST_RETRY_COUNT);

    /**
     * A monitor for successful write requests, in bytes.
     */
    private final TrafficMonitor successWriteMonitor = new TrafficMonitor();
    /**
     * A monitor for failed write requests (i.e., requests that are throttled), in bytes.
     */
    private final TrafficMonitor failedWriteMonitor = new TrafficMonitor();
    /**
     * A limiter to control the rate of write requests.
     * It is used to limit the write traffic when the write requests are throttled.
     */
    private final TrafficRateLimiter writeRateLimiter;
    /**
     * A limiter to control the volume of write requests.
     * It is used to limit the inflight write traffic when the write requests are throttled.
     */
    private final TrafficVolumeLimiter writeVolumeLimiter;
    /**
     * The regulator to control the rate of write requests.
     */
    private final TrafficRegulator writeRegulator;
    /**
     * Pending tasks for write operations (PutObject and UploadPart).
     * The task with higher priority and requested earlier will be executed first.
     */
    private final Queue<AsyncTask> writeTasks = new PriorityBlockingQueue<>();
    /**
     * The lock to protect the {@link this#currentWriteTask}.
     */
    private final Lock writeTaskLock = new ReentrantLock();
    /**
     * The current write task (e.g., waiting for {@link this#writeRateLimiter}).
     */
    private CompletableFuture<Void> currentWriteTask = CompletableFuture.completedFuture(null);

    protected AbstractObjectStorage(
        BucketURI bucketURI,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter,
        NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        int maxObjectStorageConcurrency,
        int currentIndex,
        boolean readWriteIsolate,
        boolean checkS3ApiMode,
        boolean manualMergeRead,
        String threadPrefix) {
        this.threadPrefix = threadPrefix;
        this.logger = new LogContext(String.format("[ObjectStorage-%s-%s] ", threadPrefix, currentIndex)).logger(AbstractObjectStorage.class);
        this.bucketURI = bucketURI;
        this.currentIndex = currentIndex;
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.inflightWriteLimiter = new Semaphore(maxObjectStorageConcurrency);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(maxObjectStorageConcurrency) : inflightWriteLimiter;
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter != null ? networkInboundBandwidthLimiter : NetworkBandwidthLimiter.NOOP;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter != null ? networkOutboundBandwidthLimiter : NetworkBandwidthLimiter.NOOP;
        this.checkS3ApiMode = checkS3ApiMode;

        String prefix = threadPrefix + "-" + currentIndex + "-";
        writeLimiterCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
            prefix + "s3-write-limiter-cb-executor", true, logger);
        readCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
            prefix + "s3-read-cb-executor", true, logger);
        writeCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
            prefix + "s3-write-cb-executor", true, logger);
        scheduler = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory(prefix + "s3-scheduler", true), logger);
        fastRetryTimer = new HashedWheelTimer(
            ThreadUtils.createThreadFactory(prefix + "s3-fast-retry-timer", true), 10, TimeUnit.MILLISECONDS, 1000);

        if (!manualMergeRead) {
            scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        }
        S3StreamMetricsManager.registerInflightS3ReadQuotaSupplier(inflightReadLimiter::availablePermits, currentIndex);
        S3StreamMetricsManager.registerInflightS3WriteQuotaSupplier(inflightWriteLimiter::availablePermits, currentIndex);

        this.deleteObjectsAccumulator = newDeleteObjectsAccumulator();

        s3LatencyCalculator = new S3LatencyCalculator(
            new long[] {
                1024, 16 * 1024, 64 * 1024, 256 * 1024, 512 * 1024,
                1024 * 1024, 2 * 1024 * 1024, 3 * 1024 * 1024, 4 * 1024 * 1024, 5 * 1024 * 1024, 8 * 1024 * 1024,
                12 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024},
            Duration.ofSeconds(3).toMillis());

        writeRateLimiter = new TrafficRateLimiter(scheduler);
        writeVolumeLimiter = new TrafficVolumeLimiter();
        writeRegulator = new TrafficRegulator("write", successWriteMonitor, failedWriteMonitor, writeRateLimiter, writeVolumeLimiter, logger);
        scheduler.scheduleWithFixedDelay(writeRegulator::regulate, 60, 60, TimeUnit.SECONDS);
    }

    public AbstractObjectStorage(BucketURI bucketURI,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter,
        NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate,
        boolean checkS3ApiMode,
        String threadPrefix) {
        this(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, getMaxObjectStorageConcurrency(),
            INDEX.incrementAndGet(), readWriteIsolate, checkS3ApiMode, false, threadPrefix);
    }

    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        options = options.copy().bucketId(bucketURI.bucketId());
        return new ProxyWriter(options, this, objectPath);
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        if (!bucketCheck(options.bucket(), cf)) {
            return cf;
        }
        if (end != RANGE_READ_TO_END && start > end) {
            IllegalArgumentException ex = new IllegalArgumentException();
            logger.error("[UNEXPECTED] rangeRead [{}, {})", start, end, ex);
            cf.completeExceptionally(ex);
            return cf;
        } else if (start == end) {
            cf.complete(Unpooled.EMPTY_BUFFER);
            return cf;
        }

        BiFunction<ThrottleStrategy, Long, CompletableFuture<Void>> networkInboundBandwidthLimiterFunction =
            (throttleStrategy, size) -> {
                long startTime = System.nanoTime();
                return networkInboundBandwidthLimiter.consume(throttleStrategy, size)
                    .whenComplete((v, ex) ->
                        NetworkStats.getInstance()
                            .networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.INBOUND, throttleStrategy)
                            .record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS)));

            };

        long acquiredSize = end - start;

        if (end == RANGE_READ_TO_END) {
            // we don't know the size so acquire size 1 first.
            acquiredSize = 1;

            // when read complete use bypass to forceConsume limiter token.
            cf.whenComplete((data, ex) -> {
                if (ex == null && data.readableBytes() - 1 > 0) {
                    networkInboundBandwidthLimiterFunction.apply(ThrottleStrategy.BYPASS, (long) (data.readableBytes() - 1));
                }
            });
        }

        networkInboundBandwidthLimiterFunction.apply(options.throttleStrategy(), acquiredSize).whenComplete((v, ex) -> {
            if (ex != null) {
                cf.completeExceptionally(ex);
            } else {
                synchronized (waitingReadTasks) {
                    waitingReadTasks.add(new AbstractObjectStorage.ReadTask(options, objectPath, start, end, cf));
                }
            }
        });

        return FutureUtil.timeoutWithNewReturn(cf, 2, TimeUnit.MINUTES, () -> {
            logger.warn("rangeRead {} {}-{} timeout", objectPath, start, end);
            // The return CompletableFuture will be completed with TimeoutException,
            // so we need to release the ByteBuf if the read complete later.
            cf.thenAccept(ReferenceCounted::release);
        });
    }

    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf data) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<WriteResult> retCf = acquireWritePermit(cf).thenApply(nil -> new WriteResult(bucketURI.bucketId()));
        if (retCf.isDone()) {
            data.release();
            return retCf;
        }
        TimerUtil timerUtil = new TimerUtil();
        networkOutboundBandwidthLimiter
            .consume(options.throttleStrategy(), data.readableBytes())
            .whenCompleteAsync((v, ex) -> {
                NetworkStats.getInstance().networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.OUTBOUND, options.throttleStrategy())
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                    data.release();
                    return;
                }
                if (checkTimeout(options, cf)) {
                    data.release();
                    return;
                }
                queuedWrite0(options, objectPath, data, cf);
            }, writeLimiterCallbackExecutor);
        return retCf;
    }

    private void recordWriteStats(String path, long objectSize, TimerUtil timerUtil) {
        s3LatencyCalculator.record(objectSize, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
        S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, objectSize);
        S3OperationStats.getInstance().putObjectStats(objectSize, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        successWriteMonitor.record(objectSize);
        if (logger.isDebugEnabled()) {
            logger.debug("put object {} with size {}, cost {}ms", path, objectSize, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Put an object to the specified path.
     *
     * @param options   options (or context) about the write operation
     * @param path      the path to put the object
     * @param data      the data to put, it will be released once the finalCf is done
     * @param attemptCf the CompletableFuture to complete when a single attempt is done
     * @param finalCf   the CompletableFuture to complete when the write operation is done
     */
    private void write0(WriteOptions options, String path, ByteBuf data, CompletableFuture<Void> attemptCf,
        CompletableFuture<Void> finalCf) {
        TimerUtil timerUtil = new TimerUtil();
        long objectSize = data.readableBytes();

        if (checkTimeout(options, finalCf)) {
            attemptCf.completeExceptionally(new TimeoutException());
            data.release();
            return;
        }

        CompletableFuture<Void> writeCf = doWrite(options, path, data);
        FutureUtil.propagate(writeCf, attemptCf);
        AtomicBoolean completedFlag = new AtomicBoolean(false);
        WriteOptions retryOptions = options.copy().retry(true);

        // Fast retry should only be triggered by the original request.
        long delayMillis = s3LatencyCalculator.valueAtPercentile(objectSize, 99);

        if (options.enableFastRetry() && delayMillis > 0 && !options.retry()) {
            data.retain();
            fastRetryTimer.newTimeout(timeout -> {
                if (writeCf != null && !writeCf.isDone() && fastRetryPermit.tryAcquire()) {
                    TimerUtil retryTimerUtil = new TimerUtil();

                    doWrite(retryOptions, path, data).thenAccept(nil -> {
                        recordWriteStats(path, objectSize, retryTimerUtil);

                        data.release();
                        fastRetryPermit.release();

                        if (completedFlag.compareAndSet(false, true)) {
                            finalCf.complete(null);
                            logger.info("Fast retry: put object {} with size {}, cost {}ms, delay {}ms", path, objectSize, retryTimerUtil.elapsedAs(TimeUnit.MILLISECONDS), delayMillis);
                        } else {
                            logger.info("Fast retry but duplicated: put object {} with size {}, cost {}ms, delay {}ms", path, objectSize, retryTimerUtil.elapsedAs(TimeUnit.MILLISECONDS), delayMillis);
                        }
                    }).exceptionally(ignore -> {
                        data.release();
                        fastRetryPermit.release();

                        // The fast retry request will not retry again.
                        S3OperationStats.getInstance().putObjectStats(objectSize, false).record(retryTimerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                        return null;
                    });
                } else {
                    data.release();
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        }

        writeCf.thenAccept(nil -> {
            recordWriteStats(path, objectSize, timerUtil);
            data.release();
            if (completedFlag.compareAndSet(false, true)) {
                finalCf.complete(null);
            }
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().putObjectStats(objectSize, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.PUT_OBJECT);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();

            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                // no need to retry
                logger.error("PutObject for object {} fail", path, cause);
                data.release();
                if (completedFlag.compareAndSet(false, true)) {
                    finalCf.completeExceptionally(cause);
                }
                return null;
            }

            int retryCount = retryOptions.retryCountGetAndAdd();
            if (isThrottled(cause, retryCount)) {
                failedWriteMonitor.record(objectSize);
                logger.warn("PutObject for object {} fail, retry count {}, queued and retry later", path, retryCount, cause);
                queuedWrite0(retryOptions, path, data, finalCf);
            } else {
                int delay = retryDelay(S3Operation.PUT_OBJECT, retryCount);
                logger.warn("PutObject for object {} fail, retry count {}, retry in {}ms", path, retryCount, delay, cause);
                delayedWrite0(retryOptions, path, data, finalCf, delay);
            }
            return null;
        });
    }

    private void delayedWrite0(WriteOptions options, String path, ByteBuf data, CompletableFuture<Void> cf,
        int delayMs) {
        CompletableFuture<Void> ignored = new CompletableFuture<>();
        scheduler.schedule(() -> write0(options, path, data, ignored, cf), delayMs, TimeUnit.MILLISECONDS);
    }

    private void queuedWrite0(WriteOptions options, String path, ByteBuf data, CompletableFuture<Void> cf) {
        CompletableFuture<Void> attemptCf = new CompletableFuture<>();
        AsyncTask task = new AsyncTask(
            options.requestTime(),
            options.throttleStrategy(),
            () -> write0(options, path, data, attemptCf, cf),
            attemptCf,
            () -> (long) data.readableBytes()
        );
        writeTasks.add(task);
        maybeRunNextWriteTask();
    }

    public CompletableFuture<String> createMultipartUpload(WriteOptions options, String path) {
        CompletableFuture<String> cf = new CompletableFuture<>();
        CompletableFuture<String> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        createMultipartUpload0(options, path, cf);
        return retCf;
    }

    private void createMultipartUpload0(WriteOptions options, String path, CompletableFuture<String> cf) {
        TimerUtil timerUtil = new TimerUtil();
        doCreateMultipartUpload(options, path).thenAccept(uploadId -> {
            S3OperationStats.getInstance().createMultiPartUploadStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(uploadId);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().createMultiPartUploadStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.CREATE_MULTI_PART_UPLOAD);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                logger.error("CreateMultipartUpload for object {} fail", path, cause);
                cf.completeExceptionally(cause);
            } else {
                int delay = retryDelay(S3Operation.CREATE_MULTI_PART_UPLOAD, options.retryCountGetAndAdd());
                logger.warn("CreateMultipartUpload for object {} fail, retry in {}ms", path, delay, cause);
                scheduler.schedule(() -> createMultipartUpload0(options, path, cf), delay, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    public CompletableFuture<ObjectStorageCompletedPart> uploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf data) {
        CompletableFuture<ObjectStorageCompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<ObjectStorageCompletedPart> refCf = acquireWritePermit(cf);
        refCf = refCf.whenComplete((v, ex) -> data.release());
        if (refCf.isDone()) {
            return refCf;
        }
        networkOutboundBandwidthLimiter
            .consume(options.throttleStrategy(), data.readableBytes())
            .whenCompleteAsync((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                    return;
                }
                if (checkTimeout(options, cf)) {
                    return;
                }
                queuedUploadPart0(options, path, uploadId, partNumber, data, cf);
            }, writeLimiterCallbackExecutor);
        return refCf;
    }

    /**
     * Upload a part of an object to the specified path.
     *
     * @param options    options (or context) about the write operation
     * @param path       the path of the object where the part will be uploaded
     * @param uploadId   the upload ID of the multipart upload
     * @param partNumber the part number of the part to be uploaded
     * @param data       the data to be uploaded, it will be released once the finalCf is done
     * @param attemptCf  the CompletableFuture to complete when a single attempt is done
     * @param finalCf    the CompletableFuture to complete when the upload operation is done
     */
    private void uploadPart0(WriteOptions options, String path, String uploadId, int partNumber, ByteBuf data,
        CompletableFuture<ObjectStorageCompletedPart> attemptCf,
        CompletableFuture<ObjectStorageCompletedPart> finalCf) {
        if (checkTimeout(options, finalCf)) {
            attemptCf.completeExceptionally(new TimeoutException());
            return;
        }
        TimerUtil timerUtil = new TimerUtil();
        int size = data.readableBytes();
        CompletableFuture<ObjectStorageCompletedPart> uploadPartCf = doUploadPart(options, path, uploadId, partNumber, data);
        FutureUtil.propagate(uploadPartCf, attemptCf);
        uploadPartCf.thenAccept(part -> {
            S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, size);
            S3OperationStats.getInstance().uploadPartStats(size, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            successWriteMonitor.record(size);
            finalCf.complete(part);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.UPLOAD_PART);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();

            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                // no need to retry
                logger.error("UploadPart for object {}-{} fail", path, partNumber, cause);
                finalCf.completeExceptionally(cause);
                return null;
            }

            int retryCount = options.retryCountGetAndAdd();
            if (isThrottled(cause, retryCount)) {
                failedWriteMonitor.record(size);
                logger.warn("UploadPart for object {}-{} fail, retry count {}, queued and retry later", path, partNumber, retryCount, cause);
                queuedUploadPart0(options, path, uploadId, partNumber, data, finalCf);
            } else {
                int delay = retryDelay(S3Operation.UPLOAD_PART, retryCount);
                logger.warn("UploadPart for object {}-{} fail, retry count {}, retry in {}ms", path, partNumber, retryCount, delay, cause);
                delayedUploadPart0(options, path, uploadId, partNumber, data, finalCf, delay);
            }
            return null;
        });
    }

    private void delayedUploadPart0(WriteOptions options, String path, String uploadId, int partNumber, ByteBuf data,
        CompletableFuture<ObjectStorageCompletedPart> cf, int delayMs) {
        CompletableFuture<ObjectStorageCompletedPart> ignored = new CompletableFuture<>();
        scheduler.schedule(() -> uploadPart0(options, path, uploadId, partNumber, data, ignored, cf), delayMs, TimeUnit.MILLISECONDS);
    }

    private void queuedUploadPart0(WriteOptions options, String path, String uploadId, int partNumber, ByteBuf data,
        CompletableFuture<ObjectStorageCompletedPart> cf) {
        CompletableFuture<ObjectStorageCompletedPart> attemptCf = new CompletableFuture<>();
        AsyncTask task = new AsyncTask(
            options.requestTime(),
            options.throttleStrategy(),
            () -> uploadPart0(options, path, uploadId, partNumber, data, attemptCf, cf),
            attemptCf,
            () -> (long) data.readableBytes()
        );
        writeTasks.add(task);
        maybeRunNextWriteTask();
    }

    public CompletableFuture<ObjectStorageCompletedPart> uploadPartCopy(WriteOptions options, String sourcePath,
        String path, long start, long end, String uploadId, int partNumber) {
        CompletableFuture<ObjectStorageCompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<ObjectStorageCompletedPart> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        options.apiCallAttemptTimeout(DEFAULT_UPLOAD_PART_COPY_TIMEOUT);
        uploadPartCopy0(options, sourcePath, path, start, end, uploadId, partNumber, cf);
        return retCf;
    }

    private void uploadPartCopy0(WriteOptions options, String sourcePath, String path, long start, long end,
        String uploadId, int partNumber, CompletableFuture<ObjectStorageCompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        doUploadPartCopy(options, sourcePath, path, start, end, uploadId, partNumber).thenAccept(part -> {
            S3OperationStats.getInstance().uploadPartCopyStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(part);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartCopyStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.UPLOAD_PART_COPY);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                logger.warn("UploadPartCopy for object {}-{} [{}, {}] fail", path, partNumber, start, end, cause);
                cf.completeExceptionally(cause);
            } else {
                long nextApiCallAttemptTimeout = Math.min(options.apiCallAttemptTimeout() * 2, TimeUnit.MINUTES.toMillis(10));
                options.apiCallAttemptTimeout(nextApiCallAttemptTimeout);
                int delay = retryDelay(S3Operation.UPLOAD_PART_COPY, options.retryCountGetAndAdd());
                logger.warn("UploadPartCopy for object {}-{} [{}, {}] fail, retry in {}ms with apiCallAttemptTimeout={}", path, partNumber, start, end, delay, nextApiCallAttemptTimeout, cause);
                scheduler.schedule(() -> uploadPartCopy0(options, sourcePath, path, start, end, uploadId, partNumber, cf), delay, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    public CompletableFuture<Void> completeMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<Void> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        completeMultipartUpload0(options, path, uploadId, parts, cf);
        return retCf;
    }

    private void completeMultipartUpload0(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts, CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        doCompleteMultipartUpload(options, path, uploadId, parts).thenAccept(nil -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.COMPLETE_MULTI_PART_UPLOAD);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                logger.error("CompleteMultipartUpload for object {} fail", path, cause);
                cf.completeExceptionally(cause);
            } else if (!checkPartNumbers(parts)) {
                logger.error("CompleteMultipartUpload for object {} fail, part numbers are not continuous", path);
                cf.completeExceptionally(new IllegalArgumentException("Part numbers are not continuous"));
            } else if (retryStrategy == RetryStrategy.VISIBILITY_CHECK) {
                rangeRead(new ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(options.bucketId()), path, 0, 1)
                    .whenComplete((nil, t) -> {
                        if (t != null) {
                            int delay = retryDelay(S3Operation.COMPLETE_MULTI_PART_UPLOAD, options.retryCountGetAndAdd());
                            logger.warn("CompleteMultipartUpload for object {} fail, retry in {}ms", path, delay, t);
                            scheduler.schedule(() -> completeMultipartUpload0(options, path, uploadId, parts, cf), delay, TimeUnit.MILLISECONDS);
                        } else {
                            cf.complete(null);
                        }
                    });
            } else {
                int delay = retryDelay(S3Operation.COMPLETE_MULTI_PART_UPLOAD, options.retryCountGetAndAdd());
                logger.warn("CompleteMultipartUpload for object {} fail, retry in {}ms", path, delay, cause);
                scheduler.schedule(() -> completeMultipartUpload0(options, path, uploadId, parts, cf), delay, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        if (objectPaths.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> cf = new CompletableFuture<>();
        for (ObjectPath objectPath : objectPaths) {
            if (!bucketCheck(objectPath.bucketId(), cf)) {
                logger.error("[BUG] {} bucket check fail, expect {}", objectPath, bucketId());
                return cf;
            }
        }

        deleteObjectsAccumulator.batchDeleteObjects(objectPaths, cf);

        return cf;
    }

    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        TimerUtil timerUtil = new TimerUtil();
        CompletableFuture<List<ObjectInfo>> cf = doList(prefix);
        cf.thenAccept(keyList -> {
            S3OperationStats.getInstance().listObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            logger.info("List objects finished, count: {}, cost: {}ms", keyList.size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().listObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            logger.info("List objects failed, cost: {}, ex: {}", timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
            return null;
        });
        return cf;
    }

    @Override
    public short bucketId() {
        return bucketURI == null ? 0 : bucketURI.bucketId();
    }

    @Override
    public void close() {
        writeLimiterCallbackExecutor.shutdown();
        readCallbackExecutor.shutdown();
        writeCallbackExecutor.shutdown();
        scheduler.shutdown();
        fastRetryTimer.stop();
        doClose();
    }

    abstract CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end);

    abstract CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data);

    abstract CompletableFuture<String> doCreateMultipartUpload(WriteOptions options, String path);

    abstract CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path,
        String uploadId, int partNumber, ByteBuf part);

    abstract CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath,
        String path, long start, long end, String uploadId, int partNumber);

    abstract CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts);

    abstract CompletableFuture<Void> doDeleteObjects(List<String> objectKeys);

    abstract Pair<RetryStrategy, Throwable> toRetryStrategyAndCause(Throwable ex, S3Operation operation);

    abstract void doClose();

    abstract CompletableFuture<List<ObjectInfo>> doList(String prefix);

    protected int retryDelay(S3Operation operation, int retryCount) {
        switch (operation) {
            case UPLOAD_PART_COPY:
                return 1000;
            default:
                return ThreadLocalRandom.current().nextInt(1000) + Math.min(1000 * (1 << Math.min(retryCount, 16)), (int) (TimeUnit.MINUTES.toMillis(1)));
        }
    }

    private static boolean checkPartNumbers(List<ObjectStorageCompletedPart> parts) {
        Optional<Integer> maxOpt = parts.stream().map(ObjectStorageCompletedPart::getPartNumber).max(Integer::compareTo);
        return maxOpt.isPresent() && maxOpt.get() == parts.size();
    }

    void tryMergeRead() {
        try {
            tryMergeRead0();
        } catch (Throwable e) {
            logger.error("[UNEXPECTED] tryMergeRead fail", e);
        }
    }

    /**
     * Get adjacent read tasks and merge them into one read task which read range is not exceed 16MB.
     */
    private void tryMergeRead0() {
        List<AbstractObjectStorage.MergedReadTask> mergedReadTasks = new ArrayList<>();
        synchronized (waitingReadTasks) {
            if (waitingReadTasks.isEmpty()) {
                return;
            }
            int readPermit = availableReadPermit();
            while (readPermit > 0 && !waitingReadTasks.isEmpty()) {
                Iterator<AbstractObjectStorage.ReadTask> it = waitingReadTasks.iterator();
                Map<String, AbstractObjectStorage.MergedReadTask> mergingReadTasks = new HashMap<>();
                while (it.hasNext()) {
                    AbstractObjectStorage.ReadTask readTask = it.next();
                    String path = readTask.objectPath;
                    AbstractObjectStorage.MergedReadTask mergedReadTask = mergingReadTasks.get(path);
                    if (mergedReadTask == null) {
                        if (readPermit > 0) {
                            readPermit -= 1;
                            mergedReadTask = new AbstractObjectStorage.MergedReadTask(readTask, maxMergeReadSparsityRate);
                            mergingReadTasks.put(path, mergedReadTask);
                            mergedReadTasks.add(mergedReadTask);
                            it.remove();
                        }
                    } else {
                        if (mergedReadTask.tryMerge(readTask)) {
                            it.remove();
                        }
                    }
                }
            }
        }
        mergedReadTasks.forEach(
            mergedReadTask -> {
                String path = mergedReadTask.objectPath;
                if (logger.isDebugEnabled()) {
                    logger.debug("merge read: {}, {}-{}, size: {}, sparsityRate: {}",
                        path, mergedReadTask.start, mergedReadTask.end,
                        mergedReadTask.end - mergedReadTask.start, mergedReadTask.dataSparsityRate);
                }
                mergedRangeRead(mergedReadTask.readTasks.get(0).options, path, mergedReadTask.start, mergedReadTask.end)
                    .whenComplete((rst, ex) -> FutureUtil.suppress(() -> mergedReadTask.handleReadCompleted(rst, ex), logger));
            }
        );
    }

    private int availableReadPermit() {
        return inflightReadLimiter.availablePermits();
    }

    CompletableFuture<ByteBuf> mergedRangeRead(ReadOptions options, String path, long start, long end) {
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retCf = acquireReadPermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        mergedRangeRead0(options, path, start, end, cf);
        return retCf;
    }

    private void mergedRangeRead0(ReadOptions options, String path, long start, long end,
        CompletableFuture<ByteBuf> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long size = end - start;
        doRangeRead(options, path, start, end).thenAccept(buf -> {
            // the end may be RANGE_READ_TO_END (-1) for read all object
            long dataSize = buf.readableBytes();
            if (logger.isDebugEnabled()) {
                logger.debug("GetObject for object {} [{}, {}), size: {}, cost: {} ms",
                    path, start, end, dataSize, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
            }
            S3OperationStats.getInstance().downloadSizeTotalStats.add(MetricsLevel.INFO, dataSize);
            S3OperationStats.getInstance().getObjectStats(dataSize, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(buf);
        }).exceptionally(ex -> {
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.GET_OBJECT);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                if (!(cause instanceof ObjectNotExistException)) {
                    logger.error("GetObject for object {} [{}, {}) fail", path, start, end, cause);
                }
                cf.completeExceptionally(cause);
            } else {
                int delay = retryDelay(S3Operation.GET_OBJECT, options.retryCountGetAndAdd());
                logger.warn("GetObject for object {} [{}, {}) fail, retry in {}ms", path, start, end, delay, cause);
                scheduler.schedule(() -> mergedRangeRead0(options, path, start, end, cf), delay, TimeUnit.MILLISECONDS);
            }
            S3OperationStats.getInstance().getObjectStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            return null;
        });
    }

    private void maybeRunNextWriteTask() {
        writeTaskLock.lock();
        try {
            if (!currentWriteTask.isDone()) {
                return;
            }

            AsyncTask task = writeTasks.poll();
            if (task == null) {
                return;
            }

            long size = Math.min(task.bytes(), writeRegulator.maxRequestSize());
            currentWriteTask = CompletableFuture.allOf(
                writeRateLimiter.consume(size),
                writeVolumeLimiter.acquire(size)
            ).thenRun(task::run);
            task.registerCallback(() -> writeVolumeLimiter.release(size));
            currentWriteTask.whenComplete((nil, ignored) -> maybeRunNextWriteTask());
        } finally {
            writeTaskLock.unlock();
        }
    }

    static int getMaxObjectStorageConcurrency() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        return Math.max(MIN_CONCURRENCY, Math.min(cpuCores * DEFAULT_CONCURRENCY_PER_CORE, MAX_CONCURRENCY));
    }

    private static boolean isThrottled(Throwable ex, int retryCount) {
        if (ex instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) ex;
            return s3Ex.statusCode() == HttpStatusCode.THROTTLING || s3Ex.statusCode() == HttpStatusCode.SERVICE_UNAVAILABLE;
        }
        // regard timeout as throttled except for the first try
        return ex instanceof TimeoutException && retryCount > 0;
    }

    /**
     * Check whether the operation is timeout, and fail the future with {@link TimeoutException} if timeout.
     */
    private static boolean checkTimeout(WriteOptions options, CompletableFuture<?> cf) {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - options.requestTime());
        if (elapsedMs > options.timeout()) {
            cf.completeExceptionally(new TimeoutException(String.format("request timeout, elapsedMs %d > timeoutMs %d",
                elapsedMs, options.timeout())));
            return true;
        } else {
            return false;
        }
    }

    /**
     * Acquire read permit, permit will auto release when cf complete.
     *
     * @return retCf the retCf should be used as method return value to ensure release before following operations.
     */
    <T> CompletableFuture<T> acquireReadPermit(CompletableFuture<T> cf) {
        // TODO: async acquire?
        try {
            TimerUtil timerUtil = new TimerUtil();
            inflightReadLimiter.acquire();
            StorageOperationStats.getInstance().readS3LimiterStats(currentIndex).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            CompletableFuture<T> newCf = new CompletableFuture<>();
            cf.whenComplete((rst, ex) -> {
                inflightReadLimiter.release();
                readCallbackExecutor.execute(() -> {
                    if (ex != null) {
                        newCf.completeExceptionally(ex);
                    } else {
                        newCf.complete(rst);
                    }
                });
            });
            return newCf;
        } catch (InterruptedException e) {
            cf.completeExceptionally(e);
            return cf;
        }
    }

    /**
     * Acquire write permit, permit will auto release when cf complete.
     *
     * @return retCf the retCf should be used as method return value to ensure release before following operations.
     */
    <T> CompletableFuture<T> acquireWritePermit(CompletableFuture<T> cf) {
        // this future will be return by the caller
        CompletableFuture<T> newCf = new CompletableFuture<>();

        try {
            TimerUtil timerUtil = new TimerUtil();
            inflightWriteLimiter.acquire();
            StorageOperationStats.getInstance().writeS3LimiterStats(currentIndex).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

            cf.whenComplete((rst, ex) -> {
                inflightWriteLimiter.release();
                writeCallbackExecutor.execute(() -> {
                    if (ex != null) {
                        newCf.completeExceptionally(ex);
                    } else {
                        newCf.complete(rst);
                    }
                });
            });
            return newCf;
        } catch (InterruptedException e) {
            newCf.completeExceptionally(e);
            return newCf;
        }
    }

    protected <T> boolean bucketCheck(int bucketId, CompletableFuture<T> cf) {
        if (bucketId == ObjectAttributes.MATCH_ALL_BUCKET) {
            return true;
        }
        if (bucketId != bucketURI.bucketId()) {
            cf.completeExceptionally(new IllegalArgumentException(String.format("bucket not match, expect %d, actual %d",
                bucketURI.bucketId(), bucketId)));
            return false;
        }
        return true;
    }

    protected DeleteObjectsAccumulator newDeleteObjectsAccumulator() {
        return new DeleteObjectsAccumulator(this::doDeleteObjects);
    }

    static class MergedReadTask {
        static final int MAX_MERGE_READ_SIZE = 4 * 1024 * 1024;
        final String objectPath;
        final List<AbstractObjectStorage.ReadTask> readTasks = new ArrayList<>();
        long start;
        long end;
        long uniqueDataSize;
        float dataSparsityRate = 0f;
        float maxMergeReadSparsityRate;

        MergedReadTask(AbstractObjectStorage.ReadTask readTask, float maxMergeReadSparsityRate) {
            this.objectPath = readTask.objectPath;
            this.start = readTask.start;
            this.end = readTask.end;
            this.readTasks.add(readTask);
            this.uniqueDataSize = readTask.end - readTask.start;
            this.maxMergeReadSparsityRate = maxMergeReadSparsityRate;
        }

        boolean tryMerge(AbstractObjectStorage.ReadTask readTask) {
            if (!canMerge(readTask)) {
                return false;
            }

            long newStart = Math.min(start, readTask.start);
            long newEnd = Math.max(end, readTask.end);
            boolean merge = newEnd - newStart <= MAX_MERGE_READ_SIZE;
            if (merge) {
                // insert read task in order
                int i = 0;
                long overlap = 0L;
                for (; i < readTasks.size(); i++) {
                    AbstractObjectStorage.ReadTask task = readTasks.get(i);
                    if (task.start >= readTask.start) {
                        readTasks.add(i, readTask);
                        // calculate data overlap
                        AbstractObjectStorage.ReadTask prev = i > 0 ? readTasks.get(i - 1) : null;
                        AbstractObjectStorage.ReadTask next = readTasks.get(i + 1);

                        if (prev != null && readTask.start < prev.end) {
                            overlap += prev.end - readTask.start;
                        }
                        if (readTask.end > next.start) {
                            overlap += readTask.end - next.start;
                        }
                        break;
                    }
                }
                if (i == readTasks.size()) {
                    readTasks.add(readTask);
                    AbstractObjectStorage.ReadTask prev = i >= 1 ? readTasks.get(i - 1) : null;
                    if (prev != null && readTask.start < prev.end) {
                        overlap += prev.end - readTask.start;
                    }
                }
                long uniqueSize = readTask.end - readTask.start - overlap;
                long tmpUniqueSize = uniqueDataSize + uniqueSize;
                float tmpSparsityRate = 1 - (float) tmpUniqueSize / (newEnd - newStart);
                if (tmpSparsityRate > maxMergeReadSparsityRate) {
                    // remove read task
                    readTasks.remove(i);
                    return false;
                }
                uniqueDataSize = tmpUniqueSize;
                dataSparsityRate = tmpSparsityRate;
                start = newStart;
                end = newEnd;
            }
            return merge;
        }

        private boolean canMerge(AbstractObjectStorage.ReadTask readTask) {
            return objectPath != null &&
                objectPath.equals(readTask.objectPath) &&
                dataSparsityRate <= this.maxMergeReadSparsityRate &&
                readTask.end != RANGE_READ_TO_END;
        }

        void handleReadCompleted(ByteBuf rst, Throwable ex) {
            if (ex != null) {
                readTasks.forEach(readTask -> readTask.cf.completeExceptionally(ex));
            } else {
                ArrayList<ByteBuf> sliceByteBufList = new ArrayList<>();
                for (AbstractObjectStorage.ReadTask readTask : readTasks) {
                    int sliceStart = (int) (readTask.start - start);
                    if (readTask.end == RANGE_READ_TO_END) {
                        sliceByteBufList.add(rst.retainedSlice(sliceStart, rst.readableBytes()));
                    } else {
                        sliceByteBufList.add(rst.retainedSlice(sliceStart, (int) (readTask.end - readTask.start)));
                    }
                }
                rst.release();
                for (int i = 0; i < readTasks.size(); i++) {
                    readTasks.get(i).cf.complete(sliceByteBufList.get(i));
                }

            }
        }
    }

    static final class ReadTask {
        private final ReadOptions options;
        private final String objectPath;
        private final long start;
        private final long end;
        private final CompletableFuture<ByteBuf> cf;

        ReadTask(ReadOptions options, String objectPath, long start, long end, CompletableFuture<ByteBuf> cf) {
            this.options = options;
            this.objectPath = objectPath;
            this.start = start;
            this.end = end;
            this.cf = cf;
        }

        public ReadOptions options() {
            return options;
        }

        public String objectPath() {
            return objectPath;
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

        public CompletableFuture<ByteBuf> cf() {
            return cf;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (AbstractObjectStorage.ReadTask) obj;
            return Objects.equals(this.objectPath, that.objectPath) &&
                this.start == that.start &&
                this.end == that.end &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectPath, start, end, cf);
        }

        @Override
        public String toString() {
            return "ReadTask[" +
                "s3ObjectMetadata=" + objectPath + ", " +
                "start=" + start + ", " +
                "end=" + end + ", " +
                "cf=" + cf + ']';
        }
    }

    static class DeleteObjectsException extends Exception {
        private final List<String> failedKeys;
        private final List<String> errorsMessages;

        public DeleteObjectsException(String message, List<String> failedKeys, List<String> errorsMessage) {
            super(message);
            this.failedKeys = failedKeys;
            this.errorsMessages = errorsMessage;
        }

        public List<String> getFailedKeys() {
            return failedKeys;
        }

        public List<String> getErrorsMessages() {
            return errorsMessages;
        }
    }

    public static class ObjectStorageCompletedPart {
        private final int partNumber;
        private final String partId;
        private final String checkSum;

        public ObjectStorageCompletedPart(int partNumber, String partId, String checkSum) {
            this.partNumber = partNumber;
            this.partId = partId;
            this.checkSum = checkSum;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getPartId() {
            return partId;
        }

        public String getCheckSum() {
            return checkSum;
        }
    }

    /**
     * An object storage operation task.
     */
    private static class AsyncTask implements Comparable<AsyncTask> {
        private final long requestTime;
        private final ThrottleStrategy strategy;
        /**
         * A runnable to start the task.
         */
        private final Runnable starter;
        /**
         * A future which will be completed when the task is done (whether success or failure).
         */
        private final CompletableFuture<?> finishFuture;
        private final Supplier<Long> sizeInBytes;

        public AsyncTask(long requestTime, ThrottleStrategy strategy, Runnable starter,
            CompletableFuture<?> finishFuture, Supplier<Long> sizeInBytes) {
            this.requestTime = requestTime;
            this.strategy = strategy;
            this.starter = starter;
            this.finishFuture = finishFuture;
            this.sizeInBytes = sizeInBytes;
        }

        /**
         * Start the async task.
         */
        public void run() {
            starter.run();
        }

        /**
         * Register a callback to be called when the async task is finished.
         */
        public void registerCallback(Runnable runnable) {
            finishFuture.whenComplete((nil, ignored) -> runnable.run());
        }

        /**
         * Get the request size in bytes.
         */
        public long bytes() {
            return sizeInBytes.get();
        }

        @Override
        public int compareTo(AsyncTask other) {
            int cmp = this.strategy.compareTo(other.strategy);
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(this.requestTime, other.requestTime);
        }
    }
}
