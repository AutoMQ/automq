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
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("this-escape")
public abstract class AbstractObjectStorage implements ObjectStorage {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractObjectStorage.class);
    private static final int DEFAULT_RETRY_DELAY = 100;
    private static final AtomicInteger INDEX = new AtomicInteger(-1);
    private static final int DEFAULT_CONCURRENCY_PER_CORE = 25;
    private static final int MIN_CONCURRENCY = 50;
    private static final int MAX_CONCURRENCY = 1000;
    private static final long DEFAULT_UPLOAD_PART_COPY_TIMEOUT = TimeUnit.MINUTES.toMillis(2);
    private final float maxMergeReadSparsityRate;
    private final int currentIndex;
    private final Semaphore inflightReadLimiter;
    private final Semaphore inflightWriteLimiter;
    private final List<AbstractObjectStorage.ReadTask> waitingReadTasks = new LinkedList<>();
    protected final NetworkBandwidthLimiter networkInboundBandwidthLimiter;
    protected final NetworkBandwidthLimiter networkOutboundBandwidthLimiter;
    protected final ExecutorService writeLimiterCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-write-limiter-cb-executor", true, LOGGER);
    private final ExecutorService readCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-read-cb-executor", true, LOGGER);
    private final ExecutorService writeCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-write-cb-executor", true, LOGGER);
    private final HashedWheelTimer timeoutDetect = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("s3-timeout-detect", true), 1, TimeUnit.SECONDS, 100);
    final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("objectStorage", true), LOGGER);
    private final HashedWheelTimer fastRetryTimer = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("s3-fast-retry-timer", true), 10, TimeUnit.MILLISECONDS, 1000);

    private final DeleteObjectsAccumulator deleteObjectsAccumulator;
    final boolean checkS3ApiMode;
    protected final BucketURI bucketURI;
    private final S3LatencyCalculator s3LatencyCalculator;

    protected AbstractObjectStorage(
        BucketURI bucketURI,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter,
        NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        int maxObjectStorageConcurrency,
        int currentIndex,
        boolean readWriteIsolate,
        boolean checkS3ApiMode,
        boolean manualMergeRead) {
        this.bucketURI = bucketURI;
        this.currentIndex = currentIndex;
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.inflightWriteLimiter = new Semaphore(maxObjectStorageConcurrency);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(maxObjectStorageConcurrency) : inflightWriteLimiter;
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter != null ? networkInboundBandwidthLimiter : NetworkBandwidthLimiter.NOOP;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter != null ? networkOutboundBandwidthLimiter : NetworkBandwidthLimiter.NOOP;
        this.checkS3ApiMode = checkS3ApiMode;
        if (!manualMergeRead) {
            scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        }
        checkConfig();
        S3StreamMetricsManager.registerInflightS3ReadQuotaSupplier(inflightReadLimiter::availablePermits, currentIndex);
        S3StreamMetricsManager.registerInflightS3WriteQuotaSupplier(inflightWriteLimiter::availablePermits, currentIndex);

        this.deleteObjectsAccumulator = new DeleteObjectsAccumulator(this::doDeleteObjects);
        this.deleteObjectsAccumulator.start();

        s3LatencyCalculator = new S3LatencyCalculator(
            new long[] {
                1024, 16 * 1024, 64 * 1024, 256 * 1024, 512 * 1024,
                1024 * 1024, 2 * 1024 * 1024, 3 * 1024 * 1024, 4 * 1024 * 1024, 5 * 1024 * 1024, 8 * 1024 * 1024,
                12 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024},
            Duration.ofSeconds(3).toMillis());
    }

    public AbstractObjectStorage(BucketURI bucketURI,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter,
        NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate,
        boolean checkS3ApiMode) {
        this(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, getMaxObjectStorageConcurrency(),
            INDEX.incrementAndGet(), readWriteIsolate, checkS3ApiMode, false);
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
            LOGGER.error("[UNEXPECTED] rangeRead [{}, {})", start, end, ex);
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
                            .record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS)));

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

        Timeout timeout = timeoutDetect.newTimeout(t -> LOGGER.warn("rangeRead {} {}-{} timeout", objectPath, start, end), 1, TimeUnit.MINUTES);
        return cf.whenComplete((rst, ex) -> timeout.cancel());
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
                    data.release();
                    cf.completeExceptionally(ex);
                } else {
                    write0(options, objectPath, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        return retCf;
    }

    private void recordWriteStats(String path, long objectSize, TimerUtil timerUtil) {
        s3LatencyCalculator.record(objectSize, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
        S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, objectSize);
        S3OperationStats.getInstance().putObjectStats(objectSize, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("put object {} with size {}, cost {}ms", path, objectSize, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }
    }

    private void write0(WriteOptions options, String path, ByteBuf data, CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long objectSize = data.readableBytes();

        CompletableFuture<Void> writeCf = doWrite(options, path, data);
        AtomicBoolean completedFlag = new AtomicBoolean(false);
        WriteOptions retryOptions = options.copy().retry(true);

        // Fast retry should only be triggered by the original request.
        long delayMillis = s3LatencyCalculator.valueAtPercentile(objectSize, 99);

        if (options.enableFastRetry() && delayMillis > 0 && !options.retry()) {
            data.retain();
            fastRetryTimer.newTimeout(timeout -> {
                if (writeCf != null && !writeCf.isDone()) {
                    TimerUtil retryTimerUtil = new TimerUtil();
                    doWrite(retryOptions, path, data).thenAccept(nil -> {
                        recordWriteStats(path, objectSize, retryTimerUtil);
                        data.release();
                        if (completedFlag.compareAndSet(false, true)) {
                            cf.complete(null);
                            LOGGER.info("Fast retry: put object {} with size {}, cost {}ms, delay {}ms", path, objectSize, retryTimerUtil.elapsedAs(TimeUnit.MILLISECONDS), delayMillis);
                        } else {
                            LOGGER.info("Fast retry but duplicated: put object {} with size {}, cost {}ms, delay {}ms", path, objectSize, retryTimerUtil.elapsedAs(TimeUnit.MILLISECONDS), delayMillis);
                        }
                    }).exceptionally(ignore -> {
                        data.release();
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
                cf.complete(null);
            }
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().putObjectStats(objectSize, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.PUT_OBJECT);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                LOGGER.error("PutObject for object {} fail", path, cause);
                data.release();
                if (completedFlag.compareAndSet(false, true)) {
                    cf.completeExceptionally(cause);
                }
            } else {
                LOGGER.warn("PutObject for object {} fail, retry later", path, cause);
                scheduler.schedule(() -> write0(retryOptions, path, data, cf), retryDelay(S3Operation.PUT_OBJECT), TimeUnit.MILLISECONDS);
            }
            return null;
        });
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
                LOGGER.error("CreateMultipartUpload for object {} fail", path, cause);
                cf.completeExceptionally(cause);
            } else {
                LOGGER.warn("CreateMultipartUpload for object {} fail, retry later", path, cause);
                scheduler.schedule(() -> createMultipartUpload0(options, path, cf), retryDelay(S3Operation.CREATE_MULTI_PART_UPLOAD), TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    public CompletableFuture<ObjectStorageCompletedPart> uploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf data) {
        CompletableFuture<ObjectStorageCompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<ObjectStorageCompletedPart> refCf = acquireWritePermit(cf);
        if (refCf.isDone()) {
            data.release();
            return refCf;
        }
        networkOutboundBandwidthLimiter
            .consume(options.throttleStrategy(), data.readableBytes())
            .whenCompleteAsync((v, ex) -> {
                if (ex != null) {
                    data.release();
                    cf.completeExceptionally(ex);
                } else {
                    uploadPart0(options, path, uploadId, partNumber, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        return refCf;
    }

    private void uploadPart0(WriteOptions options, String path, String uploadId, int partNumber, ByteBuf data,
        CompletableFuture<ObjectStorageCompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        int size = data.readableBytes();
        doUploadPart(options, path, uploadId, partNumber, data).thenAccept(part -> {
            S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, size);
            S3OperationStats.getInstance().uploadPartStats(size, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            data.release();
            cf.complete(part);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            Pair<RetryStrategy, Throwable> strategyAndCause = toRetryStrategyAndCause(ex, S3Operation.UPLOAD_PART);
            RetryStrategy retryStrategy = strategyAndCause.getLeft();
            Throwable cause = strategyAndCause.getRight();
            if (retryStrategy == RetryStrategy.ABORT || checkS3ApiMode) {
                LOGGER.error("UploadPart for object {}-{} fail", path, partNumber, cause);
                data.release();
                cf.completeExceptionally(cause);
            } else {
                LOGGER.warn("UploadPart for object {}-{} fail, retry later", path, partNumber, cause);
                scheduler.schedule(() -> uploadPart0(options, path, uploadId, partNumber, data, cf), retryDelay(S3Operation.UPLOAD_PART), TimeUnit.MILLISECONDS);
            }
            return null;
        });
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
                LOGGER.warn("UploadPartCopy for object {}-{} [{}, {}] fail", path, partNumber, start, end, cause);
                cf.completeExceptionally(cause);
            } else {
                long nextApiCallAttemptTimeout = Math.min(options.apiCallAttemptTimeout() * 2, TimeUnit.MINUTES.toMillis(10));
                LOGGER.warn("UploadPartCopy for object {}-{} [{}, {}] fail, retry later with apiCallAttemptTimeout={}", path, partNumber, start, end, nextApiCallAttemptTimeout, cause);
                options.apiCallAttemptTimeout(nextApiCallAttemptTimeout);
                scheduler.schedule(() -> uploadPartCopy0(options, sourcePath, path, start, end, uploadId, partNumber, cf), retryDelay(S3Operation.UPLOAD_PART_COPY), TimeUnit.MILLISECONDS);
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
                LOGGER.error("CompleteMultipartUpload for object {} fail", path, cause);
                cf.completeExceptionally(cause);
            } else if (!checkPartNumbers(parts)) {
                LOGGER.error("CompleteMultipartUpload for object {} fail, part numbers are not continuous", path);
                cf.completeExceptionally(new IllegalArgumentException("Part numbers are not continuous"));
            } else if (retryStrategy == RetryStrategy.VISIBILITY_CHECK) {
                rangeRead(new ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(options.bucketId()), path, 0, 1)
                    .whenComplete((nil, t) -> {
                        if (t != null) {
                            LOGGER.warn("CompleteMultipartUpload for object {} fail, retry later", path, cause);
                            scheduler.schedule(() -> completeMultipartUpload0(options, path, uploadId, parts, cf), retryDelay(S3Operation.COMPLETE_MULTI_PART_UPLOAD), TimeUnit.MILLISECONDS);
                        } else {
                            cf.complete(null);
                        }
                    });
            } else {
                LOGGER.warn("CompleteMultipartUpload for object {} fail, retry later", path, cause);
                scheduler.schedule(() -> completeMultipartUpload0(options, path, uploadId, parts, cf), retryDelay(S3Operation.COMPLETE_MULTI_PART_UPLOAD), TimeUnit.MILLISECONDS);
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
                LOGGER.error("[BUG] {} bucket check fail, expect {}", objectPath, bucketId());
                return cf;
            }
        }

        deleteObjectsAccumulator.batchOrSubmitDeleteRequests(objectPaths, cf);

        return cf;
    }

    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        TimerUtil timerUtil = new TimerUtil();
        CompletableFuture<List<ObjectInfo>> cf = doList(prefix);
        cf.thenAccept(keyList -> {
            S3OperationStats.getInstance().listObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("List objects finished, count: {}, cost: {}ms", keyList.size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().listObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("List objects failed, cost: {}, ex: {}", timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
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
        timeoutDetect.stop();
        fastRetryTimer.stop();
        deleteObjectsAccumulator.stop();
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

    protected int retryDelay(S3Operation operation) {
        switch (operation) {
            case UPLOAD_PART_COPY:
                return 1000;
            default:
                return DEFAULT_RETRY_DELAY;
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
            LOGGER.error("[UNEXPECTED] tryMergeRead fail", e);
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
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] merge read: {}, {}-{}, size: {}, sparsityRate: {}",
                        path, mergedReadTask.start, mergedReadTask.end,
                        mergedReadTask.end - mergedReadTask.start, mergedReadTask.dataSparsityRate);
                }
                mergedRangeRead(mergedReadTask.readTasks.get(0).options, path, mergedReadTask.start, mergedReadTask.end)
                    .whenComplete((rst, ex) -> FutureUtil.suppress(() -> mergedReadTask.handleReadCompleted(rst, ex), LOGGER));
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] getObject from path: {}, {}-{}, size: {}, cost: {} ms",
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
                LOGGER.error("GetObject for object {} [{}, {}) fail", path, start, end, cause);
                cf.completeExceptionally(cause);
            } else {
                LOGGER.warn("GetObject for object {} [{}, {}) fail, retry later", path, start, end, cause);
                scheduler.schedule(() -> mergedRangeRead0(options, path, start, end, cf), retryDelay(S3Operation.GET_OBJECT), TimeUnit.MILLISECONDS);
            }
            S3OperationStats.getInstance().getObjectStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            return null;
        });
    }

    static int getMaxObjectStorageConcurrency() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        return Math.max(MIN_CONCURRENCY, Math.min(cpuCores * DEFAULT_CONCURRENCY_PER_CORE, MAX_CONCURRENCY));
    }

    private void checkConfig() {
        if (this.networkInboundBandwidthLimiter != null) {
            if (this.networkInboundBandwidthLimiter.getMaxTokens() < Writer.MIN_PART_SIZE) {
                throw new IllegalArgumentException(String.format("Network inbound burst bandwidth limit %d must be no less than min part size %d",
                    this.networkInboundBandwidthLimiter.getMaxTokens(), Writer.MIN_PART_SIZE));
            }
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

        public DeleteObjectsException(String message, List<String> successKeys, List<String> errorsMessage) {
            super(message);
            this.failedKeys = successKeys;
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

        public ObjectStorageCompletedPart(int partNumber, String partId) {
            this.partNumber = partNumber;
            this.partId = partId;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getPartId() {
            return partId;
        }
    }
}
