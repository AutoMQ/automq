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
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.metrics.stats.S3ObjectMetricsStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
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
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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
import java.util.stream.Collectors;

import static com.automq.stream.utils.FutureUtil.cause;

public class DefaultS3Operator implements S3Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Operator.class);
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
    private final ExecutorService readLimiterCallbackExecutor = Threads.newFixedThreadPool(1,
            ThreadUtils.createThreadFactory("s3-read-limiter-cb-executor-%d", true), LOGGER);
    private final ExecutorService writeLimiterCallbackExecutor = Threads.newFixedThreadPool(1,
            ThreadUtils.createThreadFactory("s3-write-limiter-cb-executor-%d", true), LOGGER);
    private final ExecutorService readCallbackExecutor = Threads.newFixedThreadPool(1,
            ThreadUtils.createThreadFactory("s3-read-cb-executor-%d", true), LOGGER);
    private final ExecutorService writeCallbackExecutor = Threads.newFixedThreadPool(1,
            ThreadUtils.createThreadFactory("s3-write-cb-executor-%d", true), LOGGER);

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle, String accessKey, String secretKey) {
        this(endpoint, region, bucket, forcePathStyle, accessKey, secretKey, null, null, false);
    }

    public DefaultS3Operator(String endpoint, String region, String bucket, boolean forcePathStyle, String accessKey, String secretKey,
                             AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter, AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter, boolean readWriteIsolate) {
        this.networkInboundBandwidthLimiter = networkInboundBandwidthLimiter;
        this.networkOutboundBandwidthLimiter = networkOutboundBandwidthLimiter;
        this.writeS3Client = newS3Client(endpoint, region, forcePathStyle, accessKey, secretKey);
        this.readS3Client = readWriteIsolate ? newS3Client(endpoint, region, forcePathStyle, accessKey, secretKey) : writeS3Client;
        this.inflightWriteLimiter = new Semaphore(50);
        this.inflightReadLimiter = readWriteIsolate ? new Semaphore(50) : inflightWriteLimiter;
        this.bucket = bucket;
        scheduler.scheduleWithFixedDelay(this::tryMergeRead, 1, 1, TimeUnit.MILLISECONDS);
        checkConfig();
        checkAvailable();
        LOGGER.info("S3Operator init with endpoint={} region={} bucket={}", endpoint, region, bucket);
    }

    public static Builder builder() {
        return new Builder();
    }

    // used for test only.
    DefaultS3Operator(S3AsyncClient s3Client, String bucket) {
        this(s3Client, bucket, false);
    }

    // used for test only.
    DefaultS3Operator(S3AsyncClient s3Client, String bucket, boolean manualMergeRead) {
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
            networkInboundBandwidthLimiter.consume(throttleStrategy, end - start).whenCompleteAsync((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    rangeRead0(path, start, end, cf);
                }
            }, readLimiterCallbackExecutor);
        } else {
            rangeRead0(path, start, end, cf);
        }

        return cf;
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
        if (waitingReadTasks.isEmpty()) {
            return;
        }
        List<MergedReadTask> mergedReadTasks = new ArrayList<>();
        synchronized (waitingReadTasks) {
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
                            mergedReadTask = new MergedReadTask(readTask);
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
                mergedReadTask -> mergedRangeRead(mergedReadTask.path, mergedReadTask.start, mergedReadTask.end)
                        .whenComplete((rst, ex) -> FutureUtil.suppress(() -> mergedReadTask.handleReadCompleted(rst, ex), LOGGER))
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
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
        readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
                .thenAccept(responsePublisher -> {
                    OperationMetricsStats.getHistogram(S3Operation.GET_OBJECT).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    long size = end - start + 1;
                    S3ObjectMetricsStats.S3_OBJECT_DOWNLOAD_SIZE.update(size);
                    ByteBuf buf = DirectByteBufAlloc.byteBuffer((int) size, "merge_read");
                    responsePublisher.subscribe(buf::writeBytes).thenAccept(v -> cf.complete(buf));
                })
                .exceptionally(ex -> {
                    OperationMetricsStats.getHistogram(S3Operation.GET_OBJECT_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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
            networkOutboundBandwidthLimiter.consume(throttleStrategy, data.readableBytes()).whenCompleteAsync((v, ex) -> {
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
            OperationMetricsStats.getHistogram(S3Operation.PUT_OBJECT).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.debug("put object {} with size {}, cost {}ms", path, objectSize, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
            data.release();
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.PUT_OBJECT_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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
            OperationMetricsStats.getHistogram(S3Operation.DELETE_OBJECT).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("[ControllerS3Operator]: Delete object finished, path: {}, cost: {}", path, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.DELETE_OBJECT_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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
            OperationMetricsStats.getHistogram(S3Operation.DELETE_OBJECTS).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            LOGGER.info("[ControllerS3Operator]: Delete objects finished, count: {}, cost: {}", resp.deleted().size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            return resp.deleted().stream().map(DeletedObject::key).collect(Collectors.toList());
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.DELETE_OBJECTS_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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
            OperationMetricsStats.getHistogram(S3Operation.CREATE_MULTI_PART_UPLOAD).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(createMultipartUploadResponse.uploadId());
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.CREATE_MULTI_PART_UPLOAD_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex)) {
                LOGGER.error("CreateMultipartUpload for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("CreateMultipartUpload for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> createMultipartUpload0(path, cf), 100, TimeUnit.SECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data, ThrottleStrategy throttleStrategy) {
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

    private void uploadPart0(String path, String uploadId, int partNumber, ByteBuf part, CompletableFuture<CompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
        UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
                .partNumber(partNumber).build();
        CompletableFuture<UploadPartResponse> uploadPartCf = writeS3Client.uploadPart(request, body);
        uploadPartCf.thenAccept(uploadPartResponse -> {
            OperationMetricsStats.getHistogram(S3Operation.UPLOAD_PART).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.UPLOAD_PART_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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
    public CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber) {
        CompletableFuture<CompletedPart> cf = new CompletableFuture<>();
        CompletableFuture<CompletedPart> retCf = acquireWritePermit(cf);
        if (retCf.isDone()) {
            return retCf;
        }
        uploadPartCopy0(sourcePath, path, start, end, uploadId, partNumber, cf);
        return retCf;
    }

    private void uploadPartCopy0(String sourcePath, String path, long start, long end, String uploadId, int partNumber, CompletableFuture<CompletedPart> cf) {
        TimerUtil timerUtil = new TimerUtil();
        long inclusiveEnd = end - 1;
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
                .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, inclusiveEnd)).uploadId(uploadId).partNumber(partNumber).build();
        writeS3Client.uploadPartCopy(request).thenAccept(uploadPartCopyResponse -> {
            OperationMetricsStats.getHistogram(S3Operation.UPLOAD_PART_COPY).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber)
                    .eTag(uploadPartCopyResponse.copyPartResult().eTag()).build();
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.UPLOAD_PART_COPY_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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

    public void completeMultipartUpload0(String path, String uploadId, List<CompletedPart> parts, CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(parts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();

        writeS3Client.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> {
            OperationMetricsStats.getHistogram(S3Operation.COMPLETE_MULTI_PART_UPLOAD).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
        }).exceptionally(ex -> {
            OperationMetricsStats.getHistogram(S3Operation.COMPLETE_MULTI_PART_UPLOAD_FAIL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
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

    private void checkAvailable() {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path = String.format("/check_available/%d", System.currentTimeMillis());
        String multipartPath = String.format("/check_available_multipart/%d", System.currentTimeMillis());
        try {
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
            LOGGER.error("Try connect s3 fail, please re-check the server configs", e);
            throw new IllegalArgumentException("Try connect s3 fail, please re-check the server configs", e);
        }
    }

    private static S3AsyncClient newS3Client(String endpoint, String region, boolean forcePathStyle, String accessKey, String secretKey) {
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

        MergedReadTask(ReadTask readTask) {
            this.path = readTask.path;
            this.start = readTask.start;
            this.end = readTask.end;
            this.readTasks.add(readTask);
        }

        boolean tryMerge(ReadTask readTask) {
            if (!path.equals(readTask.path)) {
                return false;
            }
            long newStart = Math.min(start, readTask.start);
            long newEnd = Math.max(end, readTask.end);
            boolean merge = newEnd - newStart <= MAX_MERGE_READ_SIZE;
            if (merge) {
                readTasks.add(readTask);
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

    static class ReadTask {
        final String path;
        final long start;
        final long end;
        final CompletableFuture<ByteBuf> cf;

        public ReadTask(String path, long start, long end, CompletableFuture<ByteBuf> cf) {
            this.path = path;
            this.start = start;
            this.end = end;
            this.cf = cf;
        }
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
