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
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;
import static com.automq.stream.utils.FutureUtil.cause;

public class AwsObjectStorage extends AbstractObjectStorage {

    private final String bucket;
    private final Tagging tagging;
    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;
    private static final String S3_API_NO_SUCH_KEY = "NoSuchKey";
    private boolean deleteObjectsReturnSuccessKeys;


    public AwsObjectStorage(String endpoint, Map<String, String> tagging, String region, String bucket, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate,
        boolean checkMode) {
        super(networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkMode);
        this.deleteObjectsReturnSuccessKeys = getDeleteObjectsMode();
        this.bucket = bucket;
        this.tagging = tagging(tagging);
        this.writeS3Client = newS3Client(endpoint, region, forcePathStyle, credentialsProviders, getMaxObjectStorageConcurrency());
        this.readS3Client = readWriteIsolate ? newS3Client(endpoint, region, forcePathStyle, credentialsProviders, getMaxObjectStorageConcurrency()) : writeS3Client;
    }

    // used for test only
    public AwsObjectStorage(S3AsyncClient s3Client, String bucket, boolean manualMergeRead) {
        super(manualMergeRead);
        this.bucket = bucket;
        this.writeS3Client = s3Client;
        this.readS3Client = s3Client;
        this.tagging = null;
    }

    @Override
    void doRangeRead(String path, long start, long end,
        Consumer<Throwable> failHandler, Consumer<CompositeByteBuf> successHandler) {
        GetObjectRequest request = GetObjectRequest.builder()
            .bucket(bucket)
            .key(path)
            .range(range(start, end))
            .build();
        readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
            .thenAccept(responsePublisher -> {
                CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
                responsePublisher.subscribe(bytes -> {
                    // the aws client will copy DefaultHttpContent to heap ByteBuffer
                    buf.addComponent(true, Unpooled.wrappedBuffer(bytes));
                }).thenAccept(v -> {
                    successHandler.accept(buf);
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
    void doWrite(String path, ByteBuf data,
        Consumer<Throwable> failHandler, Runnable successHandler) {
        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        PutObjectRequest request = builder.build();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data.nioBuffers());
        writeS3Client.putObject(request, body).thenAccept(putObjectResponse -> {
            successHandler.run();
        }).exceptionally(ex -> {
            failHandler.accept(ex);
            return null;
        });
    }

    @Override
    void doCreateMultipartUpload(String path,
        Consumer<Throwable> failHandler, Consumer<String> successHandler) {
        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder().bucket(bucket).key(path);
        if (null != tagging) {
            builder.tagging(tagging);
        }
        CreateMultipartUploadRequest request = builder.build();
        writeS3Client.createMultipartUpload(request).thenAccept(createMultipartUploadResponse -> {
            successHandler.accept(createMultipartUploadResponse.uploadId());
        }).exceptionally(ex -> {
            failHandler.accept(ex);
            return null;
        });
    }

    @Override
    void doUploadPart(String path, String uploadId, int partNumber, ByteBuf part,
        Consumer<Throwable> failHandler, Consumer<ObjectStorageCompletedPart> successHandler) {
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
        UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
            .partNumber(partNumber).build();
        CompletableFuture<UploadPartResponse> uploadPartCf = writeS3Client.uploadPart(request, body);
        uploadPartCf.thenAccept(uploadPartResponse -> {
            ObjectStorageCompletedPart objectStorageCompletedPart = new ObjectStorageCompletedPart(partNumber, uploadPartResponse.eTag());
            successHandler.accept(objectStorageCompletedPart);
        }).exceptionally(ex -> {
            failHandler.accept(ex);
            return null;
        });
    }

    @Override
    void doUploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber, long apiCallAttemptTimeout,
        Consumer<Throwable> failHandler, Consumer<ObjectStorageCompletedPart> successHandler) {
        long inclusiveEnd = end - 1;
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
            .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, inclusiveEnd)).uploadId(uploadId).partNumber(partNumber)
            .overrideConfiguration(AwsRequestOverrideConfiguration.builder().apiCallAttemptTimeout(Duration.ofMillis(apiCallAttemptTimeout)).apiCallTimeout(Duration.ofMillis(apiCallAttemptTimeout)).build())
            .build();
        writeS3Client.uploadPartCopy(request).thenAccept(uploadPartCopyResponse -> {
            ObjectStorageCompletedPart completedPart = new ObjectStorageCompletedPart(partNumber, uploadPartCopyResponse.copyPartResult().eTag());
            successHandler.accept(completedPart);
        }).exceptionally(ex -> {
            failHandler.accept(ex);
            return null;
        });
    }

    @Override
    public void doCompleteMultipartUpload(String path, String uploadId, List<ObjectStorageCompletedPart> parts,
        Consumer<Throwable> failHandler, Runnable successHandler) {
        List<CompletedPart> completedParts = parts.stream()
            .map(part -> CompletedPart.builder().partNumber(part.getPartNumber()).eTag(part.getPartId()).build())
            .collect(Collectors.toList());
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();
        writeS3Client.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> {
            successHandler.run();
        }).exceptionally(ex -> {
            failHandler.accept(ex);
            return null;
        });
    }

    public void doDeleteObjects(List<String> objectKeys,
        Consumer<Throwable> failHandler, Runnable successHandler) {
        ObjectIdentifier[] toDeleteKeys = objectKeys.stream().map(key ->
            ObjectIdentifier.builder()
                .key(key)
                .build()
        ).toArray(ObjectIdentifier[]::new);

        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(toDeleteKeys).build())
            .build();

        this.writeS3Client.deleteObjects(request)
            .thenAccept(resp -> {
                try {
                    handleDeleteObjectsResponse(resp, deleteObjectsReturnSuccessKeys);
                    successHandler.run();
                } catch (Exception ex) {
                    failHandler.accept(ex);
                }
            })
            .exceptionally(ex -> {
                failHandler.accept(ex);
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

    @Override
    void doClose() {
        writeS3Client.close();
        if (readS3Client != writeS3Client) {
            readS3Client.close();
        }
    }

    static void handleDeleteObjectsResponse(DeleteObjectsResponse response, boolean deleteObjectsReturnSuccessKeys) throws Exception {
        int errDeleteCount = 0;
        ArrayList<String> failedKeys = new ArrayList<>();
        ArrayList<String> errorsMessages = new ArrayList<>();
        if (deleteObjectsReturnSuccessKeys) {
            // expect NoSuchKey is not response because s3 api won't return this in errors.
            for (S3Error error : response.errors()) {
                if (errDeleteCount < 30) {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                        error.key(), error.code(), error.message());
                }
                failedKeys.add(error.key());
                errorsMessages.add(error.message());
                errDeleteCount++;

            }
        } else {
            for (S3Error error : response.errors()) {
                if (S3_API_NO_SUCH_KEY.equals(error.code())) {
                    // ignore for delete objects.
                    continue;
                }
                if (errDeleteCount < 30) {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                        error.key(), error.code(), error.message());
                }
                failedKeys.add(error.key());
                errorsMessages.add(error.message());
                errDeleteCount++;
            }
        }
        if (errDeleteCount > 0) {
            throw new DeleteObjectsException("Failed to delete objects", failedKeys, errorsMessages);
        }
    }


    private String range(long start, long end) {
        if (end == -2L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private boolean getDeleteObjectsMode() {
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

        ObjectIdentifier[] toDeleteKeys = path.stream().map(key ->
            ObjectIdentifier.builder()
                .key(key)
                .build()
        ).toArray(ObjectIdentifier[]::new);
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(toDeleteKeys).build())
            .build();

        return CompletableFuture.allOf(
                write(path1, Unpooled.wrappedBuffer(content), WriteOptions.DEFAULT.throttleStrategy()),
                write(path2, Unpooled.wrappedBuffer(content), WriteOptions.DEFAULT.throttleStrategy())
            )
            .thenCompose(__ -> this.writeS3Client.deleteObjects(request))
            .thenApply(resp -> checkIfDeleteObjectsWillReturnSuccessDeleteKeys(path, resp));
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
