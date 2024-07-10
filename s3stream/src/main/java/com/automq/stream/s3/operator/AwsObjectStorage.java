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
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.CollectionHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;
import static com.automq.stream.utils.FutureUtil.cause;

@SuppressWarnings("this-escape")
public class AwsObjectStorage extends AbstractObjectStorage {
    public static final String S3_API_NO_SUCH_KEY = "NoSuchKey";
    public static final String PATH_STYLE = "pathStyle";
    public static final int AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER = 1000;

    private final String bucket;
    private final Tagging tagging;
    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;

    public AwsObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
        List<AwsCredentialsProvider> credentialsProviders,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter, NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate, boolean checkMode) {
        super(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkMode);
        this.bucket = bucketURI.bucket();
        this.tagging = tagging(tagging);
        Supplier<S3AsyncClient> clientSupplier = () -> newS3Client(bucketURI.endpoint(), bucketURI.region(), bucketURI.extensionBool(PATH_STYLE, false), credentialsProviders, getMaxObjectStorageConcurrency());
        this.writeS3Client = clientSupplier.get();
        this.readS3Client = readWriteIsolate ? clientSupplier.get() : writeS3Client;
        readinessCheck();
    }

    // used for test only
    public AwsObjectStorage(S3AsyncClient s3Client, String bucket) {
        super(BucketURI.parse("0@s3://b"), NetworkBandwidthLimiter.NOOP, NetworkBandwidthLimiter.NOOP, 50, 0, true, false, false);
        this.bucket = bucket;
        this.writeS3Client = s3Client;
        this.readS3Client = s3Client;
        this.tagging = null;
    }

    public static Builder builder() {
        return new Builder();
    }

    static void checkDeleteObjectsResponse(DeleteObjectsResponse response) throws Exception {
        int errDeleteCount = 0;
        ArrayList<String> failedKeys = new ArrayList<>();
        ArrayList<String> errorsMessages = new ArrayList<>();
        for (S3Error error : response.errors()) {
            if (S3_API_NO_SUCH_KEY.equals(error.code())) {
                // ignore for delete objects.
                continue;
            }
            if (errDeleteCount < 5) {
                LOGGER.error("Delete objects for key [{}] error code [{}] message [{}]",
                    error.key(), error.code(), error.message());
            }
            failedKeys.add(error.key());
            errorsMessages.add(error.message());
            errDeleteCount++;
        }
        if (errDeleteCount > 0) {
            throw new DeleteObjectsException("Failed to delete objects", failedKeys, errorsMessages);
        }
    }

    @Override
    CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
            .thenAccept(responsePublisher -> {
                CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
                responsePublisher.subscribe(bytes -> {
                    // the aws client will copy DefaultHttpContent to heap ByteBuffer
                    buf.addComponent(true, Unpooled.wrappedBuffer(bytes));
                }).whenComplete((rst, ex) -> {
                    if (ex != null) {
                        buf.release();
                        cf.completeExceptionally(ex);
                    } else {
                        cf.complete(buf);
                    }
                });
            })
            .exceptionally(ex -> {
                cf.completeExceptionally(ex);
                return null;
            });
        return cf;
    }

    @Override
    CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data) {
        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        PutObjectRequest request = builder.build();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data.nioBuffers());
        return writeS3Client.putObject(request, body).thenApply(rst -> null);
    }

    @Override
    CompletableFuture<String> doCreateMultipartUpload(WriteOptions options, String path) {
        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        CreateMultipartUploadRequest request = builder.build();
        return writeS3Client.createMultipartUpload(request).thenApply(CreateMultipartUploadResponse::uploadId);
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf part) {
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
        UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
            .partNumber(partNumber).build();
        return writeS3Client.uploadPart(request, body)
            .thenApply(resp -> new ObjectStorageCompletedPart(partNumber, resp.eTag()));
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath, String path,
        long start, long end, String uploadId, int partNumber) {
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
            .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, end)).uploadId(uploadId).partNumber(partNumber)
            .overrideConfiguration(
                AwsRequestOverrideConfiguration.builder()
                    .apiCallAttemptTimeout(Duration.ofMillis(options.apiCallAttemptTimeout()))
                    .apiCallTimeout(Duration.ofMillis(options.apiCallAttemptTimeout())).build()
            )
            .build();
        return writeS3Client.uploadPartCopy(request).thenApply(resp -> new ObjectStorageCompletedPart(partNumber, resp.copyPartResult().eTag()));
    }

    @Override
    public CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        List<CompletedPart> completedParts = parts.stream()
            .map(part -> CompletedPart.builder().partNumber(part.getPartNumber()).eTag(part.getPartId()).build())
            .collect(Collectors.toList());
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();
        return writeS3Client.completeMultipartUpload(request).thenApply(resp -> null);
    }

    public CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        return CompletableFuture.allOf(
            CollectionHelper.groupListByBatchSizeAsStream(objectKeys, AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER)
                .map(this::doDeleteObjects0).toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> doDeleteObjects0(List<String> objectKeys) {
        ObjectIdentifier[] toDeleteKeys = objectKeys.stream().map(key ->
            ObjectIdentifier.builder()
                .key(key)
                .build()
        ).toArray(ObjectIdentifier[]::new);

        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(toDeleteKeys).build())
            .build();

        CompletableFuture<Void> cf = new CompletableFuture<>();
        this.writeS3Client.deleteObjects(request)
            .thenAccept(resp -> {
                try {
                    checkDeleteObjectsResponse(resp);
                    cf.complete(null);
                } catch (Throwable ex) {
                    cf.completeExceptionally(ex);
                }
            })
            .exceptionally(ex -> {
                cf.completeExceptionally(ex);
                return null;
            });
        return cf;
    }

    @Override
    RetryStrategy toRetryStrategy(Throwable ex, S3Operation operation) {
        ex = cause(ex);
        if (ex instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) ex;
            switch (operation) {
                case COMPLETE_MULTI_PART_UPLOAD:
                    if (s3Ex.statusCode() == HttpStatusCode.FORBIDDEN) {
                        return RetryStrategy.ABORT;
                    } else if (s3Ex.statusCode() == HttpStatusCode.NOT_FOUND) {
                        return RetryStrategy.VISIBILITY_CHECK;
                    }
                    return RetryStrategy.RETRY;
                default:
                    return s3Ex.statusCode() == HttpStatusCode.FORBIDDEN || s3Ex.statusCode() == HttpStatusCode.NOT_FOUND ?
                        RetryStrategy.ABORT : RetryStrategy.RETRY;
            }
        }
        return RetryStrategy.RETRY;
    }

    @Override
    void doClose() {
        writeS3Client.close();
        if (readS3Client != writeS3Client) {
            readS3Client.close();
        }
    }

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        return readS3Client.listObjectsV2(builder -> builder.bucket(bucket).prefix(prefix))
            .thenApply(resp ->
                resp.contents()
                    .stream()
                    .map(object -> new ObjectInfo(bucketURI.bucketId(), object.key(), object.lastModified().toEpochMilli(), object.size()))
                    .collect(Collectors.toList()));
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        // the range end is inclusive end
        return "bytes=" + start + "-" + (end - 1);
    }

    private void readinessCheck() {
        try {
            String path = "__automq/readiness_check/%d" + System.nanoTime();
            byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
            doWrite(new WriteOptions(), path, Unpooled.wrappedBuffer(content)).get();
            doDeleteObjects(List.of(path)).get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
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

    public static class Builder {
        private BucketURI bucketURI;
        private List<AwsCredentialsProvider> credentialsProviders;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
        private NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;
        private boolean readWriteIsolate;
        private int maxReadConcurrency = 50;
        private int maxWriteConcurrency = 50;
        private boolean checkS3ApiModel = false;

        public Builder bucket(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
            return this;
        }

        public Builder credentialsProviders(List<AwsCredentialsProvider> credentialsProviders) {
            this.credentialsProviders = credentialsProviders;
            return this;
        }

        public Builder tagging(Map<String, String> tagging) {
            this.tagging = tagging;
            return this;
        }

        public Builder inboundLimiter(NetworkBandwidthLimiter inboundLimiter) {
            this.inboundLimiter = inboundLimiter;
            return this;
        }

        public Builder outboundLimiter(NetworkBandwidthLimiter outboundLimiter) {
            this.outboundLimiter = outboundLimiter;
            return this;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public Builder checkS3ApiModel(boolean checkS3ApiModel) {
            this.checkS3ApiModel = checkS3ApiModel;
            return this;
        }

        public AwsObjectStorage build() {
            return new AwsObjectStorage(bucketURI, tagging, credentialsProviders,
                inboundLimiter, outboundLimiter, readWriteIsolate, checkS3ApiModel);
        }
    }
}
