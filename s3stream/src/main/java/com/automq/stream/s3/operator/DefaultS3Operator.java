/*
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

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static com.automq.stream.utils.FutureUtil.cause;

public class DefaultS3Operator implements S3Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Operator.class);
    public final float maxMergeReadSparsityRate;
    private final String bucket;
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
    private final ExecutorService readCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-read-cb-executor", true, LOGGER);
    private final ExecutorService writeCallbackExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-write-cb-executor", true, LOGGER);
    private final HashedWheelTimer timeoutDetect = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("s3-timeout-detect", true), 1, TimeUnit.SECONDS, 100);

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle, String accessKey,
        String secretKey) {
        this(endpoint, region, bucket, forcePathStyle, accessKey, secretKey, null, null, false);
    }

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle, String accessKey,
        String secretKey,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter, boolean readWriteIsolate) {
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter;
        this.writeS3Client = newS3Client(endpoint, region, forcePathStyle, accessKey, secretKey);
        this.readS3Client = readWriteIsolate ? newS3Client(endpoint, region, forcePathStyle, accessKey, secretKey) : writeS3Client;
        this.inflightWriteLimiter = new Semaphore(50);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(50) : inflightWriteLimiter;
        this.bucket = bucket;
        scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        checkConfig();
        S3Utils.S3Context s3Context = S3Utils.S3Context.builder()
            .setEndpoint(endpoint)
            .setRegion(region)
            .setBucketName(bucket)
            .setForcePathStyle(forcePathStyle)
            .setAccessKey(accessKey)
            .setSecretKey(secretKey)
            .build();
        LOGGER.info("You are using s3Context: {}", s3Context);
        checkAvailable(s3Context);
        S3StreamMetricsManager.registerInflightS3ReadQuotaSupplier(inflightReadLimiter::availablePermits);
        S3StreamMetricsManager.registerInflightS3WriteQuotaSupplier(inflightWriteLimiter::availablePermits);
    }

    public static Builder builder() {
        return new Builder();
    }

    // used for test only.
    public DefaultS3Operator(S3AsyncClient s3Client, String bucket) {
        this(s3Client, bucket, false);
    }

    // used for test only.
    DefaultS3Operator(S3AsyncClient s3Client, String bucket, boolean manualMergeRead) {
        this.maxMergeReadSparsityRate = Utils.getMaxMergeReadSparsityRate();
        this.writeS3Client = s3Client;
        this.readS3Client = s3Client;
        this.bucket = bucket;
        this.networkInboundBandwidthLimiter = null;
        this.networkOutboundBandwidthLimiter = null;
        this.inflightWriteLimiter = new Semaphore(50);
        this.inflightReadLimiter = new Semaphore(50);
        if (!manualMergeRead) {
            scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        }
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
        if (start >= end) {
            IllegalArgumentException ex = new IllegalArgumentException();
            LOGGER.error("[UNEXPECTED] rangeRead [{}, {})", start, end, ex);
            cf.completeExceptionally(ex);
            return cf;
        }
        if (networkInboundBandwidthLimiter != null) {
            TimerUtil timerUtil = new TimerUtil();
            networkInboundBandwidthLimiter.consume(throttleStrategy, end - start).whenCompleteAsync((v, ex) -> {
                S3StreamMetricsManager.recordNetworkLimiterQueueTime(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), AsyncNetworkBandwidthLimiter.Type.INBOUND);
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    rangeRead0(path, start, end, cf);
                }
            }, readLimiterCallbackExecutor);
        } else {
            rangeRead0(path, start, end, cf);
        }

        Timeout timeout = timeoutDetect.newTimeout((t) -> LOGGER.warn("rangeRead {} {}-{} timeout", path, start, end), 1, TimeUnit.MINUTES);
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
        readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
            .thenAccept(responsePublisher -> {
                S3StreamMetricsManager.recordObjectDownloadSize(size);
                CompositeByteBuf buf = DirectByteBufAlloc.compositeByteBuffer();
                responsePublisher.subscribe((bytes) -> {
                    // the aws client will copy DefaultHttpContent to heap ByteBuffer
                    buf.addComponent(true, Unpooled.wrappedBuffer(bytes));
                }).thenAccept(v -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] getObject from path: {}, {}-{}, size: {}, cost: {} ms",
                            path, start, end, size, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                    }
                    S3StreamMetricsManager.recordS3DownloadSize(size);
                    S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.GET_OBJECT, size);
                    cf.complete(buf);
                });
            })
            .exceptionally(ex -> {
                S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.GET_OBJECT, size, false);
                if (isUnrecoverable(ex)) {
                    LOGGER.error("GetObject for object {} [{}, {}) fail", path, start, end, ex);
                    cf.completeExceptionally(ex);
                } else {
                    LOGGER.warn("GetObject for object {} [{}, {}) fail, retry later", path, start, end, ex);
                    scheduler.schedule(() -> mergedRangeRead0(path, start, end, cf), 100, TimeUnit.MILLISECONDS);
                }
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
                S3StreamMetricsManager.recordNetworkLimiterQueueTime(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), AsyncNetworkBandwidthLimiter.Type.OUTBOUND);
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
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(path).build();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data.nioBuffers());
        writeS3Client.putObject(request, body).thenAccept(putObjectResponse -> {
            S3StreamMetricsManager.recordS3UploadSize(objectSize);
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.PUT_OBJECT, objectSize);
            LOGGER.debug("put object {} with size {}, cost {}ms", path, objectSize, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
            data.release();
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.PUT_OBJECT, objectSize, false);
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
    public Writer writer(String path, ThrottleStrategy throttleStrategy) {
        return new ProxyWriter(this, path, throttleStrategy);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        TimerUtil timerUtil = new TimerUtil();
        DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(path).build();
        return writeS3Client.deleteObject(request).thenAccept(deleteObjectResponse -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.DELETE_OBJECT);
            LOGGER.info("[ControllerS3Operator]: Delete object finished, path: {}, cost: {}", path, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.DELETE_OBJECT, false);
            LOGGER.info("[ControllerS3Operator]: Delete object failed, path: {}, cost: {}, ex: {}", path, timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        TimerUtil timerUtil = new TimerUtil();
        ObjectIdentifier[] toDeleteKeys = objectKeys.stream().map(key ->
            ObjectIdentifier.builder()
                .key(key)
                .build()
        ).toArray(ObjectIdentifier[]::new);
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(toDeleteKeys).build())
            .build();
        // TODO: handle not exist object, should we regard it as deleted or ignore it.
        return this.writeS3Client.deleteObjects(request).thenApply(resp -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.DELETE_OBJECTS);
            LOGGER.info("[ControllerS3Operator]: Delete objects finished, count: {}, cost: {}", resp.deleted().size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            return resp.deleted().stream().map(DeletedObject::key).collect(Collectors.toList());
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.DELETE_OBJECTS, false);
            LOGGER.info("[ControllerS3Operator]: Delete objects failed, count: {}, cost: {}, ex: {}", objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
            return Collections.emptyList();
        });
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
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder().bucket(bucket).key(path).build();
        writeS3Client.createMultipartUpload(request).thenAccept(createMultipartUploadResponse -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.CREATE_MULTI_PART_UPLOAD);
            cf.complete(createMultipartUploadResponse.uploadId());
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.CREATE_MULTI_PART_UPLOAD, false);
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
        cf.whenComplete((rst, ex) -> data.release());
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
            S3StreamMetricsManager.recordS3UploadSize(size);
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.UPLOAD_PART, size);
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.UPLOAD_PART, size, false);
            if (isUnrecoverable(ex)) {
                LOGGER.error("UploadPart for object {}-{} fail", path, partNumber, ex);
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
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.UPLOAD_PART_COPY);
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber)
                .eTag(uploadPartCopyResponse.copyPartResult().eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.UPLOAD_PART_COPY, false);
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
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.COMPLETE_MULTI_PART_UPLOAD);
            cf.complete(null);
        }).exceptionally(ex -> {
            S3StreamMetricsManager.recordOperationLatency(timerUtil.elapsedAs(TimeUnit.NANOSECONDS), S3Operation.COMPLETE_MULTI_PART_UPLOAD, false);
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

    private static boolean checkPartNumbers(CompletedMultipartUpload multipartUpload) {
        Optional<Integer> maxOpt = multipartUpload.parts().stream().map(CompletedPart::partNumber).max(Integer::compareTo);
        return maxOpt.isPresent() && maxOpt.get() == multipartUpload.parts().size();
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private static boolean isUnrecoverable(Throwable ex) {
        ex = cause(ex);
        if (ex instanceof S3Exception s3Ex) {
            return s3Ex.statusCode() == HttpStatusCode.FORBIDDEN || s3Ex.statusCode() == HttpStatusCode.NOT_FOUND;
        }
        return false;
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

    private void checkAvailable(S3Utils.S3Context s3Context) {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path = String.format("check_available/%d", System.nanoTime());
        String multipartPath = String.format("check_available_multipart/%d", System.nanoTime());
        try {
            // Check network and bucket
            readS3Client.headBucket(b -> b.bucket(bucket)).get(3, TimeUnit.SECONDS);

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
        } catch (Throwable e) {
            LOGGER.error("Failed to write/read/delete object on S3 ", e);
            String exceptionMsg = String.format("Failed to write/read/delete object on S3. You are using s3Context: %s.", s3Context);

            Throwable cause = e.getCause();
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

    public S3AsyncClient newS3Client(String endpoint, String region, boolean forcePathStyle, String accessKey,
        String secretKey) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        builder.serviceConfiguration(c -> c.pathStyleAccessEnabled(forcePathStyle));
        builder.credentialsProvider(AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(
                () -> AwsBasicCredentials.create(accessKey, secretKey),
                InstanceProfileCredentialsProvider.create(),
                AnonymousCredentialsProvider.create()
            ).build()
        );
        builder.overrideConfiguration(b -> b.apiCallTimeout(Duration.ofMinutes(1))
            .apiCallAttemptTimeout(Duration.ofSeconds(30)));
        return builder.build();
    }

    /**
     * Acquire read permit, permit will auto release when cf complete.
     *
     * @return retCf the retCf should be used as method return value to ensure release before following operations.
     */
    <T> CompletableFuture<T> acquireReadPermit(CompletableFuture<T> cf) {
        // TODO: async acquire?
        try {
            inflightReadLimiter.acquire();
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
            inflightWriteLimiter.acquire();
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

    record ReadTask(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
    }

    public static class Builder {
        private String endpoint;
        private String region;
        private String bucket;
        private boolean forcePathStyle;
        private String accessKey;
        private String secretKey;
        private AsyncNetworkBandwidthLimiter inboundLimiter;
        private AsyncNetworkBandwidthLimiter outboundLimiter;
        private boolean readWriteIsolate;

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

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
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
            return new DefaultS3Operator(endpoint, region, bucket, forcePathStyle, accessKey, secretKey,
                inboundLimiter, outboundLimiter, readWriteIsolate);
        }
    }
}
