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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.tagging;
import static com.automq.stream.utils.FutureUtil.cause;

public class AwsObjectStorage extends AbstractObjectStorage {

    private final String bucket;
    private final Tagging tagging;
    private final S3AsyncClient readS3Client;
    private final S3AsyncClient writeS3Client;

    public AwsObjectStorage(String endpoint, Map<String, String> tagging, String region, String bucket, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders,
        AsyncNetworkBandwidthLimiter networkInboundBandwidthLimiter,
        AsyncNetworkBandwidthLimiter networkOutboundBandwidthLimiter,
        boolean readWriteIsolate,
        boolean checkMode) {
        super(networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkMode);
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

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
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
