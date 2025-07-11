/*
 * Copyright 2025, AutoMQ HK Limited.
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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.FutureUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
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
    // use the root logger to log the error to both log file and stdout
    private static final Logger READINESS_CHECK_LOGGER = LoggerFactory.getLogger("ObjectStorageReadinessCheck");
    public static final String S3_API_NO_SUCH_KEY = "NoSuchKey";
    public static final String PATH_STYLE_KEY = "pathStyle";
    public static final String CHECKSUM_ALGORITHM_KEY = "checksumAlgorithm";

    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    // The maximum number of keys that can be deleted in a single request is 1000.
    public static final int AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER = 1000;

    private final String bucket;
    private final Tagging tagging;
    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;

    private final ChecksumAlgorithm checksumAlgorithm;

    public AwsObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
        NetworkBandwidthLimiter networkInboundBandwidthLimiter, NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate, boolean checkMode, String threadPrefix) {
        super(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkMode, threadPrefix);
        this.bucket = bucketURI.bucket();
        this.tagging = tagging(tagging);
        List<AwsCredentialsProvider> credentialsProviders = credentialsProviders();

        String checksumAlgorithmStr = bucketURI.extensionString(CHECKSUM_ALGORITHM_KEY);
        if (checksumAlgorithmStr != null) {
            checksumAlgorithmStr = checksumAlgorithmStr.toUpperCase(Locale.ROOT);
        }

        ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.fromValue(checksumAlgorithmStr);
        if (checksumAlgorithm == null) {
            checksumAlgorithm = ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION;
        }
        this.checksumAlgorithm = checksumAlgorithm;

        Supplier<S3AsyncClient> clientSupplier = () -> newS3Client(bucketURI.endpoint(), bucketURI.region(), bucketURI.extensionBool(PATH_STYLE_KEY, false), credentialsProviders, getMaxObjectStorageConcurrency());
        this.writeS3Client = clientSupplier.get();
        this.readS3Client = readWriteIsolate ? clientSupplier.get() : writeS3Client;
    }

    // used for test only
    public AwsObjectStorage(S3AsyncClient s3Client, String bucket) {
        super(BucketURI.parse("0@s3://b"), NetworkBandwidthLimiter.NOOP, NetworkBandwidthLimiter.NOOP, 50, 0, true, false, false, "test");
        this.bucket = bucket;
        this.writeS3Client = s3Client;
        this.readS3Client = s3Client;
        this.tagging = null;
        this.checksumAlgorithm = ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION;
    }

    public static Builder builder() {
        return new Builder();
    }

    void checkDeleteObjectsResponse(DeleteObjectsResponse response) throws Exception {
        int errDeleteCount = 0;
        ArrayList<String> failedKeys = new ArrayList<>();
        ArrayList<String> errorsMessages = new ArrayList<>();
        for (S3Error error : response.errors()) {
            if (S3_API_NO_SUCH_KEY.equals(error.code())) {
                // ignore for delete objects.
                continue;
            }
            if (errDeleteCount < 5) {
                logger.error("Delete objects for key [{}] error code [{}] message [{}]",
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
        GetObjectRequest.Builder builder = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end));

        if (checksumAlgorithm != ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION) {
            builder.checksumMode(ChecksumMode.ENABLED);
        }

        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        readS3Client.getObject(builder.build(), AsyncResponseTransformer.toPublisher())
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

        if (checksumAlgorithm != ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION) {
            builder.checksumAlgorithm(checksumAlgorithm);
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

        if (checksumAlgorithm != ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION) {
            builder.checksumAlgorithm(checksumAlgorithm);
        }

        CreateMultipartUploadRequest request = builder.build();
        return writeS3Client.createMultipartUpload(request).thenApply(CreateMultipartUploadResponse::uploadId);
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf part) {
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
        UploadPartRequest.Builder builder = UploadPartRequest.builder()
            .bucket(bucket)
            .key(path)
            .uploadId(uploadId)
            .partNumber(partNumber);

        if (checksumAlgorithm != ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION) {
            builder.checksumAlgorithm(checksumAlgorithm);
        }

        return writeS3Client.uploadPart(builder.build(), body)
            .thenApply(resp -> {
                String checksum;
                switch (checksumAlgorithm) {
                    case CRC32_C:
                        checksum = resp.checksumCRC32C();
                        break;
                    case CRC32:
                        checksum = resp.checksumCRC32();
                        break;
                    case SHA1:
                        checksum = resp.checksumSHA1();
                        break;
                    case SHA256:
                        checksum = resp.checksumSHA256();
                        break;
                    default:
                        checksum = null;
                }
                return new ObjectStorageCompletedPart(partNumber, resp.eTag(), checksum);
            });
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
        return writeS3Client.uploadPartCopy(request).thenApply(resp -> new ObjectStorageCompletedPart(partNumber, resp.copyPartResult().eTag(), resp.copyPartResult().checksumCRC32C()));
    }

    @Override
    public CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        List<CompletedPart> completedParts = parts.stream()
            .map(part -> CompletedPart.builder().partNumber(part.getPartNumber()).eTag(part.getPartId()).checksumCRC32C(part.getCheckSum()).build())
            .collect(Collectors.toList());
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();
        return writeS3Client.completeMultipartUpload(request).thenApply(resp -> null);
    }

    public CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
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
                    cause = new ObjectNotExistException(cause);
                }
            }
        } else if (cause instanceof ApiCallAttemptTimeoutException) {
            cause = new TimeoutException(cause.getMessage());
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
        CompletableFuture<List<ObjectInfo>> resultFuture = new CompletableFuture<>();
        List<ObjectInfo> allObjects = new ArrayList<>();
        listNextBatch(prefix, null, allObjects, resultFuture);
        return resultFuture;
    }

    private void listNextBatch(String prefix, String continuationToken, List<ObjectInfo> allObjects,
        CompletableFuture<List<ObjectInfo>> resultFuture) {
        readS3Client.listObjectsV2(builder -> {
            builder.bucket(bucket).prefix(prefix);
            if (continuationToken != null) {
                builder.continuationToken(continuationToken);
            }
        }).thenAccept(resp -> {
            resp.contents()
                .stream()
                .map(object -> new ObjectInfo(bucketURI.bucketId(), object.key(), object.lastModified().toEpochMilli(), object.size()))
                .forEach(allObjects::add);
            if (resp.isTruncated()) {
                listNextBatch(prefix, resp.nextContinuationToken(), allObjects, resultFuture);
            } else {
                resultFuture.complete(allObjects);
            }
        }).exceptionally(ex -> {
            resultFuture.completeExceptionally(ex);
            return null;
        });
    }

    @Override
    protected DeleteObjectsAccumulator newDeleteObjectsAccumulator() {
        return new DeleteObjectsAccumulator(AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER, getMaxObjectStorageConcurrency(), this::doDeleteObjects);
    }

    protected List<AwsCredentialsProvider> credentialsProviders() {
        return credentialsProviders0(bucketURI);
    }

    protected List<AwsCredentialsProvider> credentialsProviders0(BucketURI bucketURI) {
        return List.of(new AutoMQStaticCredentialsProvider(bucketURI), DefaultCredentialsProvider.create());
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        // the range end is inclusive end
        return "bytes=" + start + "-" + (end - 1);
    }

    protected S3AsyncClient newS3Client(String endpoint, String region, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders, int maxConcurrency) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        if (!OpenSsl.isAvailable()) {
            logger.warn("OpenSSL is not available, using JDK SSL provider, which may have performance issue.", OpenSsl.unavailabilityCause());
        }
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(maxConcurrency)
            .build();
        builder.httpClient(httpClient);
        builder.serviceConfiguration(c -> c.pathStyleAccessEnabled(forcePathStyle));
        builder.credentialsProvider(newCredentialsProviderChain(credentialsProviders));
        builder.overrideConfiguration(clientOverrideConfiguration());
        return builder.build();
    }

    protected ClientOverrideConfiguration clientOverrideConfiguration() {
        return ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofSeconds(30))
            .apiCallAttemptTimeout(Duration.ofSeconds(10))
            .build();
    }

    protected AwsCredentialsProvider newCredentialsProviderChain(List<AwsCredentialsProvider> credentialsProviders) {
        List<AwsCredentialsProvider> providers = new ArrayList<>(credentialsProviders);
        // Add default providers to the end of the chain
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
            READINESS_CHECK_LOGGER.info("Start readiness check for {}", bucketURI);
            String normalPath = String.format("__automq/readiness_check/normal_obj/%d", System.nanoTime());
            try {
                writeS3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(normalPath).build()).get();
            } catch (Throwable e) {
                Throwable cause = FutureUtil.cause(e);
                if (cause instanceof SdkClientException) {
                    READINESS_CHECK_LOGGER.error("Cannot connect to s3, please check the s3 endpoint config", cause);
                } else if (cause instanceof S3Exception) {
                    int code = ((S3Exception) cause).statusCode();
                    switch (code) {
                        case HttpStatusCode.NOT_FOUND:
                            break;
                        case HttpStatusCode.FORBIDDEN:
                            READINESS_CHECK_LOGGER.error("Please check whether config is correct", cause);
                            return false;
                        default:
                            READINESS_CHECK_LOGGER.error("Please check config is correct", cause);
                    }
                }
            }

            try {
                byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
                doWrite(new WriteOptions(), normalPath, Unpooled.wrappedBuffer(content)).get();
            } catch (Throwable e) {
                Throwable cause = FutureUtil.cause(e);
                if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() == HttpStatusCode.NOT_FOUND) {
                    READINESS_CHECK_LOGGER.error("Cannot find the bucket={}", bucket, cause);
                } else {
                    READINESS_CHECK_LOGGER.error("Please check the identity have the permission to do Write Object operation", cause);
                }
                return false;
            }

            try {
                doDeleteObjects(List.of(normalPath)).get();
            } catch (Throwable e) {
                READINESS_CHECK_LOGGER.error("Please check the identity have the permission to do Delete Object operation", FutureUtil.cause(e));
                return false;
            }

            String multiPartPath = String.format("__automq/readiness_check/multi_obj/%d", System.nanoTime());
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
                    READINESS_CHECK_LOGGER.error("Read get mismatch content from multi-part upload object, expect {}, but {}", content, readContent);
                }
                doDeleteObjects(List.of(multiPartPath)).get();
            } catch (Throwable e) {
                READINESS_CHECK_LOGGER.error("Please check the identity have the permission to do MultiPart Object operation", FutureUtil.cause(e));
                return false;
            }

            READINESS_CHECK_LOGGER.info("Readiness check pass!");
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
        private String threadPrefix = "";

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

        public Builder threadPrefix(String threadPrefix) {
            this.threadPrefix = threadPrefix;
            return this;
        }

        public AwsObjectStorage build() {
            return new AwsObjectStorage(bucketURI, tagging, inboundLimiter, outboundLimiter, readWriteIsolate, checkS3ApiModel, threadPrefix);
        }
    }
}
