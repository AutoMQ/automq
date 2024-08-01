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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.CollectionHelper;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;
import static com.automq.stream.s3.metrics.operations.S3Operation.COMPLETE_MULTI_PART_UPLOAD;
import static com.automq.stream.s3.metrics.operations.S3Operation.GET_OBJECT;
import static com.automq.stream.utils.FutureUtil.cause;

@SuppressWarnings({"this-escape", "NPathComplexity"})
public class AwsObjectStorage extends AbstractObjectStorage {
    public static final String S3_API_NO_SUCH_KEY = "NoSuchKey";
    public static final String PATH_STYLE_KEY = "pathStyle";
    public static final String AUTH_TYPE_KEY = "authType";
    public static final String STATIC_AUTH_TYPE = "static";
    public static final String INSTANCE_AUTH_TYPE = "instance";

    public static final int AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER = 1000;

    private final String bucket;
    private final Tagging tagging;
    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;

    private volatile static InstanceProfileCredentialsProvider instanceProfileCredentialsProvider;

    public AwsObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter, NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate, boolean checkMode) {
        super(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkMode);
        this.bucket = bucketURI.bucket();
        this.tagging = tagging(tagging);
        List<AwsCredentialsProvider> credentialsProviders = credentialsProviders();
        Supplier<S3AsyncClient> clientSupplier = () -> newS3Client(bucketURI.endpoint(), bucketURI.region(), bucketURI.extensionBool(PATH_STYLE_KEY, false), credentialsProviders, getMaxObjectStorageConcurrency());
        this.writeS3Client = clientSupplier.get();
        this.readS3Client = readWriteIsolate ? clientSupplier.get() : writeS3Client;
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
    Pair<RetryStrategy, Throwable> toRetryStrategyAndCause(Throwable ex, S3Operation operation) {
        Throwable cause = cause(ex);
        RetryStrategy strategy = RetryStrategy.RETRY;
        if (cause instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) cause;
            switch (s3Ex.statusCode()) {
                case HttpStatusCode.FORBIDDEN:
                case HttpStatusCode.NOT_FOUND:
                    strategy = RetryStrategy.ABORT;
                    break;
                default:
                    strategy = RetryStrategy.RETRY;
            }
            if (COMPLETE_MULTI_PART_UPLOAD == operation) {
                if (cause instanceof NoSuchUploadException) {
                    strategy = RetryStrategy.VISIBILITY_CHECK;
                }
            }
            if (GET_OBJECT == operation) {
                if (cause instanceof NoSuchKeyException) {
                    cause = new ObjectNotFoundException(cause);
                }
            }
        }
        return Pair.of(strategy, cause);
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

    protected List<AwsCredentialsProvider> credentialsProviders() {
        String authType = bucketURI.extensionString(AUTH_TYPE_KEY, STATIC_AUTH_TYPE);
        switch (authType) {
            case STATIC_AUTH_TYPE: {
                String accessKey = bucketURI.extensionString(BucketURI.ACCESS_KEY_KEY, System.getenv("KAFKA_S3_ACCESS_KEY"));
                String secretKey = bucketURI.extensionString(BucketURI.SECRET_KEY_KEY, System.getenv("KAFKA_S3_SECRET_KEY"));
                if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
                    return Collections.emptyList();
                }
                return List.of(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            }
            case INSTANCE_AUTH_TYPE: {
                return List.of(instanceProfileCredentialsProvider());
            }
            default:
                throw new UnsupportedOperationException("Unsupported auth type: " + authType);
        }
    }

    protected AwsCredentialsProvider instanceProfileCredentialsProvider() {
        if (instanceProfileCredentialsProvider == null) {
            synchronized (AwsObjectStorage.class) {
                if (instanceProfileCredentialsProvider == null) {
                    instanceProfileCredentialsProvider = InstanceProfileCredentialsProvider.builder().build();
                }
            }
        }
        return instanceProfileCredentialsProvider;
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        // the range end is inclusive end
        return "bytes=" + start + "-" + (end - 1);
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

    public boolean readinessCheck() {
        return new ReadinessCheck().readinessCheck();
    }

    class ReadinessCheck {
        public boolean readinessCheck() {
            LOGGER.info("Start readiness check for {}", bucketURI);
            String path = "__automq/readiness_check/normal_obj/%d" + System.nanoTime();
            try {
                writeS3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(path).build()).get();
            } catch (Throwable e) {
                // 权限 / endpoint / xxx
                Throwable cause = FutureUtil.cause(e);
                if (cause instanceof SdkClientException) {
                    LOGGER.error("Cannot connect to s3, please check the s3 endpoint config", cause);
                } else if (cause instanceof S3Exception) {
                    int code = ((S3Exception) cause).statusCode();
                    switch (code) {
                        case HttpStatusCode.NOT_FOUND:
                            break;
                        case HttpStatusCode.FORBIDDEN:
                            LOGGER.error("Please check whether config is correct", cause);
                            return false;
                        default:
                            LOGGER.error("Please check config is correct", cause);
                    }
                }
            }

            try {
                byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
                doWrite(new WriteOptions(), path, Unpooled.wrappedBuffer(content)).get();
            } catch (Throwable e) {
                Throwable cause = FutureUtil.cause(e);
                if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() == HttpStatusCode.NOT_FOUND) {
                    LOGGER.error("Cannot find the bucket={}", bucket, cause);
                } else {
                    LOGGER.error("Please check the identity have the permission to do Write Object operation", cause);
                }
                return false;
            }

            try {
                doDeleteObjects(List.of(path)).get();
            } catch (Throwable e) {
                LOGGER.error("Please check the identity have the permission to do Delete Object operation", FutureUtil.cause(e));
                return false;
            }

            String multiPartPath = "__automq/readiness_check/multi_obj/%d" + System.nanoTime();
            try {
                WriteOptions options = new WriteOptions();
                String uploadId = doCreateMultipartUpload(options, multiPartPath).get();
                byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
                ObjectStorageCompletedPart part = doUploadPart(options, multiPartPath, uploadId, 1, Unpooled.wrappedBuffer(content)).get();
                doCompleteMultipartUpload(options, multiPartPath, uploadId, List.of(part)).get();

                ByteBuf buf = doRangeRead(new ReadOptions(), multiPartPath, 0, -1L).get();
                byte[] readContent = new byte[buf.readableBytes()];
                buf.readBytes(readContent);
                buf.release();
                if (!Arrays.equals(content, readContent)) {
                    LOGGER.error("Read get mismatch content from multi-part upload object, expect {}, but {}", content, readContent);
                }
                doDeleteObjects(List.of(path)).get();
            } catch (Throwable e) {
                LOGGER.error("Please check the identity have the permission to do MultiPart Object operation", FutureUtil.cause(e));
                return false;
            }

            LOGGER.info("Readiness check pass!");
            return true;
        }

    }

    public static class Builder {
        private BucketURI bucketURI;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
        private NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;
        private boolean readWriteIsolate;
        private boolean checkS3ApiModel = false;

        public Builder bucket(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
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
            return new AwsObjectStorage(bucketURI, tagging, inboundLimiter, outboundLimiter, readWriteIsolate, checkS3ApiModel);
        }
    }
}
