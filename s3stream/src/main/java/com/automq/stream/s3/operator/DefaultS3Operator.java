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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.S3Utils;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;
import static com.automq.stream.utils.FutureUtil.cause;

public class DefaultS3Operator implements S3Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Operator.class);
    private static final AtomicInteger INDEX = new AtomicInteger(-1);
    private static final int DEFAULT_CONCURRENCY_PER_CORE = 25;
    private static final int MIN_CONCURRENCY = 50;
    private static final int MAX_CONCURRENCY = 1000;
    public final float maxMergeReadSparsityRate;
    private final int currentIndex;
    private final String bucket;
    /**
     * Tagging for all objects
     * It can be null if tagging is not enabled.
     */
    private final Tagging tagging;
    private final S3AsyncClient writeS3Client;
    private final S3AsyncClient readS3Client;
    private final Semaphore inflightWriteLimiter;
    private final Semaphore inflightReadLimiter;
    private final List<ReadTask> waitingReadTasks = new LinkedList<>();
    private final AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter;
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("s3operator", true), LOGGER);
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

    private boolean deleteObjectsReturnSuccessKeys;

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders, boolean tagging) {
        this(endpoint, region, bucket, forcePathStyle, credentialsProviders, tagging, null, null, false);
    }

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders,
        boolean tagging,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter, boolean readWriteIsolate) {
        this.currentIndex = INDEX.incrementAndGet();
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter;
        this.tagging = tagging ? tagging() : null;
        int maxS3Concurrency = getMaxS3Concurrency();
        this.writeS3Client = newS3Client(endpoint, region, forcePathStyle, credentialsProviders, maxS3Concurrency);
        this.readS3Client = readWriteIsolate ? newS3Client(endpoint, region, forcePathStyle, credentialsProviders, maxS3Concurrency) : writeS3Client;
        this.inflightWriteLimiter = new Semaphore(maxS3Concurrency);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(maxS3Concurrency) : inflightWriteLimiter;
        this.bucket = bucket;
        scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        checkConfig();
        S3Utils.S3Context s3Context = S3Utils.S3Context.builder()
            .setEndpoint(endpoint)
            .setRegion(region)
            .setBucketName(bucket)
            .setForcePathStyle(forcePathStyle)
            .setCredentialsProviders(credentialsProviders)
            .build();
        LOGGER.info("You are using s3Context: {}, max concurrency: {}", s3Context, maxS3Concurrency);
        checkAvailable(s3Context);
        S3StreamMetricsManager.registerInflightS3ReadQuotaSupplier(inflightReadLimiter::availablePermits, currentIndex);
        S3StreamMetricsManager.registerInflightS3WriteQuotaSupplier(inflightWriteLimiter::availablePermits, currentIndex);
    }

    // used for test only.
    public DefaultS3Operator(S3AsyncClient s3Client, String bucket) {
        this(s3Client, bucket, false);
    }

    // used for test only.
    DefaultS3Operator(S3AsyncClient s3Client, String bucket, boolean manualMergeRead) {
        this.currentIndex = 0;
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.writeS3Client = s3Client;
        this.readS3Client = s3Client;
        this.bucket = bucket;
        this.tagging = null;
        this.networkInboundBandwidthLimiter = null;
        this.networkOutboundBandwidthLimiter = null;
        this.inflightWriteLimiter = new Semaphore(50);
        this.inflightReadLimiter = new Semaphore(50);
        if (!manualMergeRead) {
            scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private static boolean checkPartNumbers(CompletedMultipartUpload multipartUpload) {
        Optional<Integer> maxOpt = multipartUpload.parts().stream().map(CompletedPart::partNumber).max(Integer::compareTo);
        return maxOpt.isPresent() && maxOpt.get() == multipartUpload.parts().size();
    }

    private static boolean isUnrecoverable(Throwable ex) {
        ex = cause(ex);
        if (ex instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) ex;
            return s3Ex.statusCode() == HttpStatusCode.FORBIDDEN || s3Ex.statusCode() == HttpStatusCode.NOT_FOUND;
        }
        return false;
    }

    public int getMaxS3Concurrency() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        return Math.max(MIN_CONCURRENCY, Math.min(cpuCores * DEFAULT_CONCURRENCY_PER_CORE, MAX_CONCURRENCY));
    }

    @Override
    public void close() {
        // TODO: complete in-flight CompletableFuture with ClosedException.
        writeS3Client.close();
        if (readS3Client != writeS3Client) {
            readS3Client.close();
        }
        scheduler.shutdown();
        readLimiterCallbackExecutor.shutdown();
        writeLimiterCallbackExecutor.shutdown();
        readCallbackExecutor.shutdown();
        writeCallbackExecutor.shutdown();
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ThrottleStrategy throttleStrategy) {
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
            networkInboundBandwidthLimiter.consume(throttleStrategy, end - start).whenCompleteAsync((v, ex) -> {
                NetworkStats.getInstance().networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.INBOUND)
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    rangeRead0(path, start, end, cf);
                }
            }, readLimiterCallbackExecutor);
        } else {
            rangeRead0(path, start, end, cf);
        }

        Timeout timeout = timeoutDetect.newTimeout(t -> LOGGER.warn("rangeRead {} {}-{} timeout", path, start, end), 3, TimeUnit.MINUTES);
        return cf.whenComplete((rst, ex) -> timeout.cancel());
    }

    private void rangeRead0(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
        synchronized (waitingReadTasks) {
            waitingReadTasks.add(new ReadTask(path, start, end, cf));
        }
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
        List<MergedReadTask> mergedReadTasks = new ArrayList<>();
        synchronized (waitingReadTasks) {
            if (waitingReadTasks.isEmpty()) {
                return;
            }
            int readPermit = availableReadPermit();
            while (readPermit > 0 && !waitingReadTasks.isEmpty()) {
                Iterator<ReadTask> it = waitingReadTasks.iterator();
                Map<String, MergedReadTask> mergingReadTasks = new HashMap<>();
                while (it.hasNext()) {
                    ReadTask readTask = it.next();
                    MergedReadTask mergedReadTask = mergingReadTasks.get(readTask.path);
                    if (mergedReadTask == null) {
                        if (readPermit > 0) {
                            readPermit -= 1;
                            mergedReadTask = new MergedReadTask(readTask, maxMergeReadSparsityRate);
                            mergingReadTasks.put(readTask.path, mergedReadTask);
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
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] merge read: {}, {}-{}, size: {}, sparsityRate: {}",
                        mergedReadTask.path, mergedReadTask.start, mergedReadTask.end,
                        mergedReadTask.end - mergedReadTask.start, mergedReadTask.dataSparsityRate);
                }
                mergedRangeRead(mergedReadTask.path, mergedReadTask.start, mergedReadTask.end)
                    .whenComplete((rst, ex) -> FutureUtil.suppress(() -> mergedReadTask.handleReadCompleted(rst, ex), LOGGER));
            }
        );
    }

    private int availableReadPermit() {
        return inflightReadLimiter.availablePermits();
    }

    CompletableFuture<ByteBuf> mergedRangeRead(String path, long start, long end) {
        end = end - 1;
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retCf = acquireReadPermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        mergedRangeRead0(path, start, end, cf);
        return retCf;
    }

    void mergedRangeRead0(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long size = end - start + 1;
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
        Consumer<Throwable> failHandler = ex -> {
            if (isUnrecoverable(ex)) {
                LOGGER.error("GetObject for object {} [{}, {}) fail", path, start, end, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("GetObject for object {} [{}, {}) fail, retry later", path, start, end, ex);
                scheduler.schedule(() -> mergedRangeRead0(path, start, end, cf), 100, TimeUnit.MILLISECONDS);
            }
            S3OperationStats.getInstance().getObjectStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        };

        readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
            .thenAccept(responsePublisher -> {
                CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
                responsePublisher.subscribe(bytes -> {
                    // the aws client will copy DefaultHttpContent to heap ByteBuffer
                    buf.addComponent(true, Unpooled.wrappedBuffer(bytes));
                }).thenAccept(v -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] getObject from path: {}, {}-{}, size: {}, cost: {} ms",
                            path, start, end, size, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                    }
                    S3OperationStats.getInstance().downloadSizeTotalStats.add(MetricsLevel.INFO, size);
                    S3OperationStats.getInstance().getObjectStats(size, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    cf.complete(buf);
                }).exceptionally(ex -> {
                    buf.release();
                    failHandler.accept(ex);
                    return null;
                });
            })
            .exceptionally(ex -> {
                failHandler.accept(ex);
                return null;
            });
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<Void> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        if (networkOutboundBandwidthLimiter != null) {
            TimerUtil timerUtil = new TimerUtil();
            networkOutboundBandwidthLimiter.consume(throttleStrategy, data.readableBytes()).whenCompleteAsync((v, ex) -> {
                NetworkStats.getInstance().networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type.OUTBOUND)
                    .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    write0(path, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        } else {
            write0(path, data, cf);
        }
        return retCf;
    }

    private void write0(String path, ByteBuf data, CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        int objectSize = data.readableBytes();
        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        PutObjectRequest request = builder.build();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data.nioBuffers());
        writeS3Client.putObject(request, body).thenAccept(putObjectResponse -> {
            S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, objectSize);
            S3OperationStats.getInstance().putObjectStats(objectSize, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.debug("put object {} with size {}, cost {}ms", path, objectSize, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            data.release();
            cf.complete(null);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().putObjectStats(objectSize, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.error("PutObject for object {} fail", path, ex);
                cf.completeExceptionally(ex);
                data.release();
            } else {
                LOGGER.warn("PutObject for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> write0(path, data, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public Writer writer(Writer.Context context, String path, ThrottleStrategy throttleStrategy) {
        return new ProxyWriter(context, this, path, throttleStrategy);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        TimerUtil timerUtil = new TimerUtil();
        DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(path).build();
        return writeS3Client.deleteObject(request).thenAccept(deleteObjectResponse -> {
            S3OperationStats.getInstance().deleteObjectStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("[ControllerS3Operator]: Delete object finished, path: {}, cost: {}", path, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().deleteObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("[ControllerS3Operator]: Delete object failed, path: {}, cost: {}, ex: {}", path, timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
            return null;
        });
    }

    private CompletableFuture<DeleteObjectsResponse> deleteObjects(List<String> objectKeys) {
        ObjectIdentifier[] toDeleteKeys = objectKeys.stream().map(key ->
                ObjectIdentifier.builder()
                        .key(key)
                        .build()
        ).toArray(ObjectIdentifier[]::new);

        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(toDeleteKeys).build())
                .build();

        return this.writeS3Client.deleteObjects(request);
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        TimerUtil timerUtil = new TimerUtil();
        return deleteObjects(objectKeys)
                .thenApply(resp -> handleDeleteObjectsResponse(objectKeys, timerUtil, resp, deleteObjectsReturnSuccessKeys))
                .exceptionally(ex -> {
                    S3OperationStats.getInstance().deleteObjectsStats(false)
                            .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    LOGGER.info("[ControllerS3Operator]: Delete objects failed, count: {}, cost: {}, ex: {}",
                            objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
                    return Collections.emptyList();
                });
    }

    static List<String> handleDeleteObjectsResponse(List<String> objectKeys,
                                                    TimerUtil timerUtil,
                                                    DeleteObjectsResponse response,
                                                    boolean deleteObjectsReturnSuccessKeys) {
        Set<String> successDeleteKeys = new HashSet<>();
        int errDeleteCount = 0;
        boolean hasUnExpectedResponse = false;

        if (deleteObjectsReturnSuccessKeys) {
            response.deleted().stream().map(DeletedObject::key).forEach(successDeleteKeys::add);

            for (S3Error error : response.errors()) {
                if ("NoSuchKey".equals(error.code()) && !StringUtils.isEmpty(error.key())) {
                    successDeleteKeys.add(error.key());
                } else {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                            error.key(), error.code(), error.message());
                    errDeleteCount++;
                }
            }

        } else {
            // deleteObjects not return successKeys think as all success.
            successDeleteKeys.addAll(objectKeys);


            for (S3Error error : response.errors()) {
                if ("NoSuchKey".equals(error.code())) {
                    // ignore for delete objects.
                    continue;
                }

                if (errDeleteCount < 30) {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                            error.key(), error.code(), error.message());
                }

                if (!StringUtils.isEmpty(error.key())) {
                    successDeleteKeys.remove(error.key());
                } else {
                    hasUnExpectedResponse = true;
                }

                errDeleteCount++;
            }

            if (hasUnExpectedResponse) {
                successDeleteKeys = Collections.emptySet();
            }
        }

        LOGGER.info("[ControllerS3Operator]: Delete objects finished, count: {}, errCount: {}, cost: {}",
                successDeleteKeys.size(), errDeleteCount, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

        if (!hasUnExpectedResponse) {
            S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        } else {
            S3OperationStats.getInstance().deleteObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }

        return new ArrayList<>(successDeleteKeys);
    }

    @Override
    public CompletableFuture<String> createMultipartUpload(String path) {
        CompletableFuture<String> cf = new CompletableFuture<>();
        CompletableFuture<String> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        createMultipartUpload0(path, cf);
        return retCf;
    }

    void createMultipartUpload0(String path, CompletableFuture<String> cf) {
        TimerUtil timerUtil = new TimerUtil();
        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        CreateMultipartUploadRequest request = builder.build();
        writeS3Client.createMultipartUpload(request).thenAccept(createMultipartUploadResponse -> {
            S3OperationStats.getInstance().createMultiPartUploadStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(createMultipartUploadResponse.uploadId());
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().createMultiPartUploadStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.error("CreateMultipartUpload for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("CreateMultipartUpload for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> createMultipartUpload0(path, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data,
        ThrottleStrategy throttleStrategy) {
        CompletableFuture<CompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<CompletedPart> refCf = acquireWritePermit(cf);
        if (refCf.isDone()) {
            return refCf;
        }
        if (networkOutboundBandwidthLimiter != null) {
            networkOutboundBandwidthLimiter.consume(throttleStrategy, data.readableBytes()).whenCompleteAsync((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    uploadPart0(path, uploadId, partNumber, data, cf);
                }
            }, writeLimiterCallbackExecutor);
        } else {
            uploadPart0(path, uploadId, partNumber, data, cf);
        }
        return refCf;
    }

    private void uploadPart0(String path, String uploadId, int partNumber, ByteBuf part,
        CompletableFuture<CompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        int size = part.readableBytes();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
        UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
            .partNumber(partNumber).build();
        CompletableFuture<UploadPartResponse> uploadPartCf = writeS3Client.uploadPart(request, body);
        uploadPartCf.thenAccept(uploadPartResponse -> {
            S3OperationStats.getInstance().uploadSizeTotalStats.add(MetricsLevel.INFO, size);
            S3OperationStats.getInstance().uploadPartStats(size, true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            part.release();
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.error("UploadPart for object {}-{} fail", path, partNumber, ex);
                part.release();
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("UploadPart for object {}-{} fail, retry later", path, partNumber, ex);
                scheduler.schedule(() -> uploadPart0(path, uploadId, partNumber, part, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end,
        String uploadId, int partNumber) {
        CompletableFuture<CompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<CompletedPart> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        uploadPartCopy0(sourcePath, path, start, end, uploadId, partNumber, cf);
        return retCf;
    }

    private void uploadPartCopy0(String sourcePath, String path, long start, long end, String uploadId, int partNumber,
        CompletableFuture<CompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long inclusiveEnd = end - 1;
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
            .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, inclusiveEnd)).uploadId(uploadId).partNumber(partNumber).build();
        writeS3Client.uploadPartCopy(request).thenAccept(uploadPartCopyResponse -> {
            S3OperationStats.getInstance().uploadPartCopyStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber)
                .eTag(uploadPartCopyResponse.copyPartResult().eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartCopyStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.warn("UploadPartCopy for object {}-{} fail", path, partNumber, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("UploadPartCopy for object {}-{} fail, retry later", path, partNumber, ex);
                scheduler.schedule(() -> uploadPartCopy0(sourcePath, path, start, end, uploadId, partNumber, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<CompletedPart> parts) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture<Void> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        completeMultipartUpload0(path, uploadId, parts, cf);
        return retCf;
    }

    public void completeMultipartUpload0(String path, String uploadId, List<CompletedPart> parts,
        CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(parts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();

        writeS3Client.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.error("CompleteMultipartUpload for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else if (!checkPartNumbers(request.multipartUpload())) {
                LOGGER.error("CompleteMultipartUpload for object {} fail, part numbers are not continuous", path);
                cf.completeExceptionally(new IllegalArgumentException("Part numbers are not continuous"));
            } else {
                LOGGER.warn("CompleteMultipartUpload for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> completeMultipartUpload0(path, uploadId, parts, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
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

    private CompletableFuture<Boolean> asyncCheckDeleteObjectsReturnSuccessDeleteKeys(S3Utils.S3Context s3Context) {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path1 = String.format("check_available/deleteObjectsMode/%d", System.nanoTime());
        String path2 = String.format("check_available/deleteObjectsMode/%d", System.nanoTime() + 1);

        List<String> path = List.of(path1, path2);

        return CompletableFuture.allOf(
                        this.write(path1, Unpooled.wrappedBuffer(content)),
                        this.write(path2, Unpooled.wrappedBuffer(content))
                )
                .thenCompose(__ -> deleteObjects(path)
                        .thenApply(resp -> checkIfDeleteObjectsWillReturnSuccessDeleteKeys(path, resp)));
    }

    private boolean checkIfDeleteObjectsWillReturnSuccessDeleteKeys(List<String> path, DeleteObjectsResponse resp) {
        // BOS S3 API works as quiet mode
        // in this mode success delete objects won't be returned.
        // which could cause object not deleted in metadata.
        //
        // BOS doc: https://cloud.baidu.com/doc/BOS/s/tkc5twspg
        // S3 doc: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_RequestBody

        boolean hasDeleted = resp.hasDeleted() && !resp.deleted().isEmpty();
        boolean hasErrors = resp.hasErrors() && !resp.errors().isEmpty();
        boolean allDeleteKeyMatch = resp.deleted().stream().map(DeletedObject::key).sorted().collect(Collectors.toList()).equals(path);

        if (hasDeleted && !hasErrors && allDeleteKeyMatch) {
            LOGGER.info("call deleteObjects deleteObjectKeys returned.");

            return true;

        } else if (!hasDeleted && !hasErrors) {
            LOGGER.info("call deleteObjects but deleteObjectKeys not returned. set deleteObjectsReturnSuccessKeys = false");

            return false;
        }

        IllegalStateException exception = new IllegalStateException();

        LOGGER.error("error when check if delete objects will return success." +
                        " delete keys {} resp {}, requestId {}，httpCode {} httpText {}",
                path, resp, resp.responseMetadata().requestId(),
                resp.sdkHttpResponse().statusCode(), resp.sdkHttpResponse().statusText(), exception);

        throw exception;
    }

    private void checkAvailable(S3Utils.S3Context s3Context) {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path = String.format("check_available/%d", System.nanoTime());
        String multipartPath = String.format("check_available_multipart/%d", System.nanoTime());
        try {
            // Check network and bucket
//            readS3Client.getBucketAcl(b -> b.bucket(bucket)).get(3, TimeUnit.SECONDS);

            // Simple write/read/delete
            this.write(path, Unpooled.wrappedBuffer(content)).get(30, TimeUnit.SECONDS);
            ByteBuf read = this.rangeRead(path, 0, content.length).get(30, TimeUnit.SECONDS);
            read.release();
            this.delete(path).get(30, TimeUnit.SECONDS);

            // Multipart write/read/delete
            Writer writer = this.writer(multipartPath);
            writer.write(Unpooled.wrappedBuffer(content));
            writer.close().get(30, TimeUnit.SECONDS);
            read = this.rangeRead(multipartPath, 0, content.length).get(30, TimeUnit.SECONDS);
            read.release();
            this.delete(multipartPath).get(30, TimeUnit.SECONDS);

            // Check if oss provider deleteObjects will return successDeleteKeys in deleted.
            this.deleteObjectsReturnSuccessKeys = asyncCheckDeleteObjectsReturnSuccessDeleteKeys(s3Context).get(30, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOGGER.error("Failed to write/read/delete object on S3 ", e);
            String exceptionMsg = String.format("Failed to write/read/delete object on S3. You are using s3Context: %s.", s3Context);

            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof SdkClientException) {
                if (cause.getMessage().contains("UnknownHostException")) {
                    Throwable rootCause = ExceptionUtils.getRootCause(cause);
                    exceptionMsg += "\nUnable to resolve Host \"" + rootCause.getMessage() + "\". Please check your S3 endpoint.";
                } else if (cause.getMessage().startsWith("Unable to execute HTTP request")) {
                    exceptionMsg += "\nUnable to execute HTTP request. Please check your network connection and make sure you can access S3.";
                }
            }

            if (e instanceof TimeoutException || cause instanceof TimeoutException) {
                exceptionMsg += "\nConnection timeout. Please check your network connection and make sure you can access S3.";
            }

            if (cause instanceof NoSuchBucketException) {
                exceptionMsg += "\nBucket \"" + bucket + "\" not found. Please check your bucket name.";
            }

            List<String> advices = s3Context.advices();
            if (!advices.isEmpty()) {
                exceptionMsg += "\nHere are some advices: \n" + String.join("\n", advices);
            }
            throw new RuntimeException(exceptionMsg, e);
        }
    }

    public S3AsyncClient newS3Client(String endpoint, String region, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders, int maxConcurrency) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(maxConcurrency)
            .build();
        builder.httpClient(httpClient);
        builder.serviceConfiguration(c -> c.pathStyleAccessEnabled(forcePathStyle));
        builder.credentialsProvider(newCredentialsProviderChain(credentialsProviders));
        builder.overrideConfiguration(b -> b.apiCallTimeout(Duration.ofMinutes(2))
            .apiCallAttemptTimeout(Duration.ofSeconds(60)));
        return builder.build();
    }

    private AwsCredentialsProvider newCredentialsProviderChain(List<AwsCredentialsProvider> credentialsProviders) {
        List<AwsCredentialsProvider> providers = new ArrayList<>(credentialsProviders);
        // Add default providers to the end of the chain
        providers.add(InstanceProfileCredentialsProvider.create());
        providers.add(AnonymousCredentialsProvider.create());
        return AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(providers)
            .build();
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
        static final int MAX_MERGE_READ_SIZE = 32 * 1024 * 1024;
        final String path;
        final List<ReadTask> readTasks = new ArrayList<>();
        long start;
        long end;
        long uniqueDataSize;
        float dataSparsityRate = 0f;
        float maxMergeReadSparsityRate;

        MergedReadTask(ReadTask readTask, float maxMergeReadSparsityRate) {
            this.path = readTask.path;
            this.start = readTask.start;
            this.end = readTask.end;
            this.readTasks.add(readTask);
            this.uniqueDataSize = readTask.end - readTask.start;
            this.maxMergeReadSparsityRate = maxMergeReadSparsityRate;
        }

        boolean tryMerge(ReadTask readTask) {
            if (!path.equals(readTask.path) || dataSparsityRate > this.maxMergeReadSparsityRate) {
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
                    ReadTask task = readTasks.get(i);
                    if (task.start >= readTask.start) {
                        readTasks.add(i, readTask);
                        // calculate data overlap
                        ReadTask prev = i > 0 ? readTasks.get(i - 1) : null;
                        ReadTask next = readTasks.get(i + 1);

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
                    ReadTask prev = i >= 1 ? readTasks.get(i - 1) : null;
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

        void handleReadCompleted(ByteBuf rst, Throwable ex) {
            if (ex != null) {
                readTasks.forEach(readTask -> readTask.cf.completeExceptionally(ex));
            } else {
                for (ReadTask readTask : readTasks) {
                    readTask.cf.complete(rst.retainedSlice((int) (readTask.start - start), (int) (readTask.end - readTask.start)));
                }
                rst.release();
            }
        }
    }

    static final class ReadTask {
        private final String path;
        private final long start;
        private final long end;
        private final CompletableFuture<ByteBuf> cf;

        ReadTask(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
            this.path = path;
            this.start = start;
            this.end = end;
            this.cf = cf;
        }

        public String path() {
            return path;
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
            var that = (ReadTask) obj;
            return Objects.equals(this.path, that.path) &&
                this.start == that.start &&
                this.end == that.end &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, start, end, cf);
        }

        @Override
        public String toString() {
            return "ReadTask[" +
                "path=" + path + ", " +
                "start=" + start + ", " +
                "end=" + end + ", " +
                "cf=" + cf + ']';
        }
    }

    public static class Builder {
        private String endpoint;
        private String region;
        private String bucket;
        private boolean forcePathStyle;
        private List<AwsCredentialsProvider> credentialsProviders;
        private boolean tagging;
        private AsyncNetworkBandwidthLimiter inboundLimiter;
        private AsyncNetworkBandwidthLimiter outboundLimiter;
        private boolean readWriteIsolate;
        private int maxReadConcurrency = 50;
        private int maxWriteConcurrency = 50;

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder forcePathStyle(boolean forcePathStyle) {
            this.forcePathStyle = forcePathStyle;
            return this;
        }

        public Builder credentialsProviders(List<AwsCredentialsProvider> credentialsProviders) {
            this.credentialsProviders = credentialsProviders;
            return this;
        }

        public Builder tagging(boolean tagging) {
            this.tagging = tagging;
            return this;
        }

        public Builder inboundLimiter(AsyncNetworkBandwidthLimiter inboundLimiter) {
            this.inboundLimiter = inboundLimiter;
            return this;
        }

        public Builder outboundLimiter(AsyncNetworkBandwidthLimiter outboundLimiter) {
            this.outboundLimiter = outboundLimiter;
            return this;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public DefaultS3Operator build() {
            return new DefaultS3Operator(endpoint, region, bucket, forcePathStyle, credentialsProviders, tagging,
                inboundLimiter, outboundLimiter, readWriteIsolate);
        }
    }
}
