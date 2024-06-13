/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;

public abstract class AbstractObjectStorage<E> implements ObjectStorage {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractObjectStorage.class);
    private static final AtomicInteger INDEX = new AtomicInteger(-1);
    private static final int DEFAULT_CONCURRENCY_PER_CORE = 25;
    private static final int MIN_CONCURRENCY = 50;
    private static final int MAX_CONCURRENCY = 1000;
    private static final long DEFAULT_UPLOAD_PART_COPY_TIMEOUT = TimeUnit.MINUTES.toMillis(2);
    private final float maxMergeReadSparsityRate;
    private final int currentIndex;
    final String bucket;
    final Tagging tagging;

    private final Semaphore inflightReadLimiter;
    private final Semaphore inflightWriteLimiter;
    private final List<AbstractObjectStorage.ReadTask> waitingReadTasks = new LinkedList<>();
    private final AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter;
    private final DeleteResponseHandler<E> deleteResponseHandler;


    private final ExecutorService readLimiterCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-read-limiter-cb-executor", true, LOGGER);
    private final ExecutorService writeLimiterCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-write-limiter-cb-executor", true, LOGGER);
    private final ExecutorService readCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(8,
        "s3-read-cb-executor", true, LOGGER);
    private final ExecutorService writeCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-write-cb-executor", true, LOGGER);
    private final HashedWheelTimer timeoutDetect = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("s3-timeout-detect", true), 1, TimeUnit.SECONDS, 100);

    final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("objectStorage", true), LOGGER);
    boolean checkS3ApiMode = false;

    public AbstractObjectStorage(String bucket, Map<String, String> tagging,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        DeleteResponseHandler<E> deleteResponseHandler,
        boolean readWriteIsolate) {
        this.bucket = bucket;
        this.tagging = tagging(tagging);
        this.currentIndex = INDEX.incrementAndGet();
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        int maxObjectStorageConcurrency = getMaxObjectStorageConcurrency();
        this.inflightWriteLimiter = new Semaphore(maxObjectStorageConcurrency);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(maxObjectStorageConcurrency) : inflightWriteLimiter;
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter;
        this.deleteResponseHandler = deleteResponseHandler;
        scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        checkConfig();
        S3StreamMetricsManager.registerInflightS3ReadQuotaSupplier(inflightReadLimiter::availablePermits, currentIndex);
        S3StreamMetricsManager.registerInflightS3WriteQuotaSupplier(inflightWriteLimiter::availablePermits, currentIndex);
    }

    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        return new ProxyWriterV2<E>(options, this, objectPath);
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, S3ObjectMetadata objectMetadata, long start, long end) {
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        if (start > end) {
            IllegalArgumentException ex = new IllegalArgumentException();
            LOGGER.error("[UNEXPECTED] rangeRead [{}, {})", start, end, ex);
            cf.completeExceptionally(ex);
            return cf;
        } else if (start == end) {
            cf.complete(Unpooled.EMPTY_BUFFER);
            return cf;
        }

        if (networkInboundBandwidthLimiter != null) {
            TimerUtil timerUtil = new TimerUtil();
            networkInboundBandwidthLimiter.consume(options.throttleStrategy(), end - start).whenCompleteAsync((v, ex) -> {
                NetworkStats.getInstance().networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.INBOUND, options.throttleStrategy())
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    rangeRead0(objectMetadata, start, end, cf);
                }
            }, readLimiterCallbackExecutor);
        } else {
            rangeRead0(objectMetadata, start, end, cf);

        }
        Timeout timeout = timeoutDetect.newTimeout(t -> LOGGER.warn("rangeRead {} {}-{} timeout", objectMetadata, start, end), 3, TimeUnit.MINUTES);
        return cf.whenComplete((rst, ex) -> timeout.cancel());
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(S3ObjectMetadata objectMetadata, long start, long end) {
        return rangeRead(ReadOptions.DEFAULT, objectMetadata, start, end);
    }

    @Override
    public CompletableFuture<List<String>> delete(List<S3ObjectMetadata> objectMetadataList) {
        TimerUtil timerUtil = new TimerUtil();
        List<String> objectKeys = objectMetadataList.stream()
            .map(metadata -> ObjectUtils.genKey(0, metadata.objectId()))
            .collect(Collectors.toList());
        return doDeleteObjects(objectKeys)
            .thenApply(resp -> deleteResponseHandler.handleDeleteResponse(objectKeys, timerUtil, resp))
            .exceptionally(ex -> {
                S3OperationStats.getInstance().deleteObjectsStats(false)
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                LOGGER.info("[ControllerS3Operator]: Delete objects failed, count: {}, cost: {}, ex: {}",
                    objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
                return Collections.emptyList();
            });
    }


    public CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<Void> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        if (networkOutboundBandwidthLimiter != null) {
            TimerUtil timerUtil = new TimerUtil();
            networkOutboundBandwidthLimiter.consume(throttleStrategy, data.readableBytes()).whenCompleteAsync((v, ex) -> {
                NetworkStats.getInstance().networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.OUTBOUND, throttleStrategy)
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    doWrite(path, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        } else {
            doWrite(path, data, cf);
        }
        return retCf;
    }

    public CompletableFuture<String> createMultipartUpload(String path) {
        CompletableFuture<String> cf = new CompletableFuture<>();
        CompletableFuture<String> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        doCreateMultipartUpload(path, cf);
        return retCf;

    }

    public CompletableFuture<ObjectStorageCompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data,
        ThrottleStrategy throttleStrategy) {
        CompletableFuture<ObjectStorageCompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<ObjectStorageCompletedPart> refCf = acquireWritePermit(cf);
        if (refCf.isDone()) {
            return refCf;
        }
        if (networkOutboundBandwidthLimiter != null) {
            networkOutboundBandwidthLimiter.consume(throttleStrategy, data.readableBytes()).whenCompleteAsync((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    doUploadPart(path, uploadId, partNumber, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        } else {
            doUploadPart(path, uploadId, partNumber, data, cf);
        }
        return refCf;
    }

    public CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<ObjectStorageCompletedPart> parts) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<Void> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        List<CompletedPart> completedParts = parts.stream()
            .map(part -> CompletedPart.builder().partNumber(part.getPartNumber()).eTag(part.getPartId()).build())
            .collect(Collectors.toList());
        doCompleteMultipartUpload(path, uploadId, completedParts, cf);
        return retCf;
    }

    public CompletableFuture<ObjectStorageCompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end,
        String uploadId, int partNumber) {
        CompletableFuture<ObjectStorageCompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<ObjectStorageCompletedPart> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        // TODO: get default timeout by latency baseline
        doUploadPartCopy(sourcePath, path, start, end, uploadId, partNumber, cf, DEFAULT_UPLOAD_PART_COPY_TIMEOUT);
        return retCf;
    }

    abstract void doWrite(String path, ByteBuf data, CompletableFuture<Void> cf);

    abstract void doRangeRead(String path, long start, long end, CompletableFuture<ByteBuf> cf, Consumer<Throwable> failHandler);

    abstract void doCreateMultipartUpload(String path, CompletableFuture<String> cf);

    abstract void doUploadPart(String path, String uploadId, int partNumber, ByteBuf part,
        CompletableFuture<ObjectStorageCompletedPart> cf);

    abstract void doCompleteMultipartUpload(String path, String uploadId, List<CompletedPart> parts,
        CompletableFuture<Void> cf);

    abstract CompletableFuture<E> doDeleteObjects(List<String> objectKeys);

    abstract void doUploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber,
        CompletableFuture<ObjectStorageCompletedPart> cf, long apiCallAttemptTimeout);

    abstract boolean isUnrecoverable(Throwable ex);

    private void rangeRead0(S3ObjectMetadata s3ObjectMetadata, long start, long end, CompletableFuture<ByteBuf> cf) {
        synchronized (waitingReadTasks) {
            waitingReadTasks.add(new AbstractObjectStorage.ReadTask(s3ObjectMetadata, start, end, cf));
        }
    }

    private void tryMergeRead() {
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
                    String path = ObjectUtils.genKey(0, readTask.s3ObjectMetadata.objectId());
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
                String path = mergedReadTask.s3ObjectMetadata.key();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] merge read: {}, {}-{}, size: {}, sparsityRate: {}",
                        path, mergedReadTask.start, mergedReadTask.end,
                        mergedReadTask.end - mergedReadTask.start, mergedReadTask.dataSparsityRate);
                }
                mergedRangeRead(path, mergedReadTask.start, mergedReadTask.end)
                    .whenComplete((rst, ex) -> FutureUtil.suppress(() -> mergedReadTask.handleReadCompleted(rst, ex), LOGGER));
            }
        );
    }

    private int availableReadPermit() {
        return inflightReadLimiter.availablePermits();
    }

    private CompletableFuture<ByteBuf> mergedRangeRead(String path, long start, long end) {
        end = end - 1;
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retCf = acquireReadPermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        mergedRangeRead0(path, start, end, cf);
        return retCf;
    }

    private void mergedRangeRead0(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long size = end - start + 1;
        Consumer<Throwable> failHandler = ex -> {
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.error("GetObject for object {} [{}, {}) fail", path, start, end, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("GetObject for object {} [{}, {}) fail, retry later", path, start, end, ex);
                scheduler.schedule(() -> mergedRangeRead0(path, start, end, cf), 100, TimeUnit.MILLISECONDS);
            }
            S3OperationStats.getInstance().getObjectStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        };
        doRangeRead(path, start, end, cf, failHandler);
    }

    int getMaxObjectStorageConcurrency() {
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
        if (this.networkOutboundBandwidthLimiter != null) {
            if (this.networkOutboundBandwidthLimiter.getMaxTokens() < Writer.MIN_PART_SIZE) {
                throw new IllegalArgumentException(String.format("Network outbound burst bandwidth limit %d must be no less than min part size %d",
                    this.networkOutboundBandwidthLimiter.getMaxTokens(), Writer.MIN_PART_SIZE));
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
        try {
            TimerUtil timerUtil = new TimerUtil();
            inflightWriteLimiter.acquire();
            StorageOperationStats.getInstance().writeS3LimiterStats(currentIndex).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            CompletableFuture<T> newCf = new CompletableFuture<>();
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
            cf.completeExceptionally(e);
            return cf;
        }
    }

    static class MergedReadTask {
        static final int MAX_MERGE_READ_SIZE = 4 * 1024 * 1024;
        final S3ObjectMetadata s3ObjectMetadata;
        final List<AbstractObjectStorage.ReadTask> readTasks = new ArrayList<>();
        long start;
        long end;
        long uniqueDataSize;
        float dataSparsityRate = 0f;
        float maxMergeReadSparsityRate;

        MergedReadTask(AbstractObjectStorage.ReadTask readTask, float maxMergeReadSparsityRate) {
            this.s3ObjectMetadata = readTask.s3ObjectMetadata;
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
            return s3ObjectMetadata != null &&
                s3ObjectMetadata.equals(readTask.s3ObjectMetadata) &&
                dataSparsityRate <= this.maxMergeReadSparsityRate;
        }

        void handleReadCompleted(ByteBuf rst, Throwable ex) {
            if (ex != null) {
                readTasks.forEach(readTask -> readTask.cf.completeExceptionally(ex));
            } else {
                for (AbstractObjectStorage.ReadTask readTask : readTasks) {
                    readTask.cf.complete(rst.retainedSlice((int) (readTask.start - start), (int) (readTask.end - readTask.start)));
                }
                rst.release();
            }
        }
    }

    static final class ReadTask {
        private final S3ObjectMetadata s3ObjectMetadata;
        private final long start;
        private final long end;
        private final CompletableFuture<ByteBuf> cf;

        ReadTask(S3ObjectMetadata s3ObjectMetadata, long start, long end, CompletableFuture<ByteBuf> cf) {
            this.s3ObjectMetadata = s3ObjectMetadata;
            this.start = start;
            this.end = end;
            this.cf = cf;
        }

        public S3ObjectMetadata s3ObjectMetadata() {
            return s3ObjectMetadata;
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
            return Objects.equals(this.s3ObjectMetadata, that.s3ObjectMetadata) &&
                this.start == that.start &&
                this.end == that.end &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(s3ObjectMetadata, start, end, cf);
        }

        @Override
        public String toString() {
            return "ReadTask[" +
                "s3ObjectMetadata=" + s3ObjectMetadata + ", " +
                "start=" + start + ", " +
                "end=" + end + ", " +
                "cf=" + cf + ']';
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
