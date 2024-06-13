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
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.automq.stream.utils.FutureUtil.cause;

public class AwsObjectStorage extends AbstractObjectStorage<DeleteObjectsResponse> {

    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;
    public static final String S3_API_NO_SUCH_KEY = "NoSuchKey";

    public AwsObjectStorage(String endpoint, Map<String, String> tagging, String region, String bucket, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate,
        S3DeleteResponseHandler s3DeleteResponseHandler) {
        super(bucket, tagging, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, s3DeleteResponseHandler, readWriteIsolate);
        s3DeleteResponseHandler.setDeleteObjectsReturnSuccessKeys(setDeleteObjectsMode());
        this.writeS3Client = newS3Client(endpoint, region, forcePathStyle, credentialsProviders, getMaxObjectStorageConcurrency());
        this.readS3Client = readWriteIsolate ? newS3Client(endpoint, region, forcePathStyle, credentialsProviders, getMaxObjectStorageConcurrency()) : writeS3Client;
    }

    @Override
    void doWrite(String path, ByteBuf data, CompletableFuture<Void> cf) {
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
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.error("PutObject for object {} fail", path, ex);
                cf.completeExceptionally(ex);
                data.release();
            } else {
                LOGGER.warn("PutObject for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> doWrite(path, data, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    void doCreateMultipartUpload(String path, CompletableFuture<String> cf) {
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
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.error("CreateMultipartUpload for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("CreateMultipartUpload for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> doCreateMultipartUpload(path, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    void doUploadPart(String path, String uploadId, int partNumber, ByteBuf part,
        CompletableFuture<ObjectStorageCompletedPart> cf) {
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
            ObjectStorageCompletedPart objectStorageCompletedPart = new ObjectStorageCompletedPart(partNumber, uploadPartResponse.eTag());
            cf.complete(objectStorageCompletedPart);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartStats(size, false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.error("UploadPart for object {}-{} fail", path, partNumber, ex);
                part.release();
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("UploadPart for object {}-{} fail, retry later", path, partNumber, ex);
                scheduler.schedule(() -> doUploadPart(path, uploadId, partNumber, part, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public void doCompleteMultipartUpload(String path, String uploadId, List<CompletedPart> parts,
        CompletableFuture<Void> cf) {
        TimerUtil timerUtil = new TimerUtil();
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(parts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();

        writeS3Client.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            cf.complete(null);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().completeMultiPartUploadStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.error("CompleteMultipartUpload for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else if (!checkPartNumbers(request.multipartUpload())) {
                LOGGER.error("CompleteMultipartUpload for object {} fail, part numbers are not continuous", path);
                cf.completeExceptionally(new IllegalArgumentException("Part numbers are not continuous"));
            } else {
                LOGGER.warn("CompleteMultipartUpload for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> doCompleteMultipartUpload(path, uploadId, parts, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    CompletableFuture<DeleteObjectsResponse> doDeleteObjects(List<String> objectKeys) {
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
    void doRangeRead(String path, long start, long end, CompletableFuture<ByteBuf> cf, Consumer<Throwable> failHandler) {
        TimerUtil timerUtil = new TimerUtil();
        long size = end - start + 1;
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
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
    void doUploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber,
        CompletableFuture<ObjectStorageCompletedPart> cf, long apiCallAttemptTimeout) {
        TimerUtil timerUtil = new TimerUtil();
        long inclusiveEnd = end - 1;
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
            .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, inclusiveEnd)).uploadId(uploadId).partNumber(partNumber)
            .overrideConfiguration(AwsRequestOverrideConfiguration.builder().apiCallAttemptTimeout(Duration.ofMillis(apiCallAttemptTimeout)).apiCallTimeout(Duration.ofMillis(apiCallAttemptTimeout)).build())
            .build();
        writeS3Client.uploadPartCopy(request).thenAccept(uploadPartCopyResponse -> {
            S3OperationStats.getInstance().uploadPartCopyStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            ObjectStorageCompletedPart completedPart = new ObjectStorageCompletedPart(partNumber, uploadPartCopyResponse.copyPartResult().eTag());
            cf.complete(completedPart);
        }).exceptionally(ex -> {
            S3OperationStats.getInstance().uploadPartCopyStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            if (isUnrecoverable(ex) || checkS3ApiMode) {
                LOGGER.warn("UploadPartCopy for object {}-{} [{}, {}] fail", path, partNumber, start, end, ex);
                cf.completeExceptionally(ex);
            } else {
                long nextApiCallAttemptTimeout = Math.min(apiCallAttemptTimeout * 2, TimeUnit.MINUTES.toMillis(10));
                LOGGER.warn("UploadPartCopy for object {}-{} [{}, {}] fail, retry later with apiCallAttemptTimeout={}", path, partNumber, start, end, nextApiCallAttemptTimeout, ex);
                scheduler.schedule(() -> doUploadPartCopy(sourcePath, path, start, end, uploadId, partNumber, cf, nextApiCallAttemptTimeout), 1000, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    boolean isUnrecoverable(Throwable ex) {
        ex = cause(ex);
        if (ex instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) ex;
            return s3Ex.statusCode() == HttpStatusCode.FORBIDDEN || s3Ex.statusCode() == HttpStatusCode.NOT_FOUND;
        }
        return false;
    }

    private boolean setDeleteObjectsMode() {
        try {
            return asyncCheckDeleteObjectsReturnSuccessDeleteKeys().get(30, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOGGER.error("Failed to check if the s3 `deleteObjects` api will return deleteKeys", e);
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Boolean> asyncCheckDeleteObjectsReturnSuccessDeleteKeys() {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path1 = String.format("check_available/deleteObjectsMode/%d", System.nanoTime());
        String path2 = String.format("check_available/deleteObjectsMode/%d", System.nanoTime() + 1);

        List<String> path = List.of(path1, path2);

        return CompletableFuture.allOf(
                write(path1, Unpooled.wrappedBuffer(content), WriteOptions.DEFAULT.throttleStrategy()),
                write(path2, Unpooled.wrappedBuffer(content), WriteOptions.DEFAULT.throttleStrategy())
            )
            .thenCompose(__ -> doDeleteObjects(path)
                .thenApply(resp -> checkIfDeleteObjectsWillReturnSuccessDeleteKeys(path, resp)));
    }

    private static boolean checkIfDeleteObjectsWillReturnSuccessDeleteKeys(List<String> path, DeleteObjectsResponse resp) {
        // BOS S3 API works as quiet mode
        // in this mode success delete objects won't be returned.
        // which could cause object not deleted in metadata.
        //
        // BOS doc: https://cloud.baidu.com/doc/BOS/s/tkc5twspg
        // S3 doc: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_RequestBody

        boolean hasDeleted = resp.hasDeleted() && !resp.deleted().isEmpty();
        boolean hasErrors = resp.hasErrors() && !resp.errors().isEmpty();
        boolean hasErrorsWithoutNoSuchKey = resp.errors().stream().filter(s3Error -> !S3_API_NO_SUCH_KEY.equals(s3Error.code())).count() != 0;
        boolean allDeleteKeyMatch = resp.deleted().stream().map(DeletedObject::key).sorted().collect(Collectors.toList()).equals(path);

        if (hasDeleted && !hasErrors && allDeleteKeyMatch) {
            LOGGER.info("call deleteObjects deleteObjectKeys returned.");

            return true;

        } else if (!hasDeleted && !hasErrorsWithoutNoSuchKey) {
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

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private static boolean checkPartNumbers(CompletedMultipartUpload multipartUpload) {
        Optional<Integer> maxOpt = multipartUpload.parts().stream().map(CompletedPart::partNumber).max(Integer::compareTo);
        return maxOpt.isPresent() && maxOpt.get() == multipartUpload.parts().size();
    }

    private S3AsyncClient newS3Client(String endpoint, String region, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders, int maxConcurrency) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        if (!OpenSsl.isAvailable()) {
            LOGGER.warn("OpenSSL is not available, using JDK SSL provider, which may have performance issue.", OpenSsl.unavailabilityCause());
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

}
