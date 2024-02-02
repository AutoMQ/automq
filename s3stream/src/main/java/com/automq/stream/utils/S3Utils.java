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

package com.automq.stream.utils;

import com.automq.stream.s3.DirectByteBufAlloc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3Utils {

    /**
     * Check s3 access with context.
     * This method is generally used to help users figure out problems in using S3.
     *
     * @param context s3 context.
     */
    public static void checkS3Access(S3Context context) {
        try (ObjectOperationTask task = new ObjectOperationTask(context)) {
            task.run();
        } catch (Throwable e) {
            System.out.println("ERROR: " + ExceptionUtils.getRootCause(e));
            System.exit(1);
        }

        try (S3MultipartUploadTestTask task = new S3MultipartUploadTestTask(context)) {
            task.run();
        } catch (Throwable e) {
            System.out.println("ERROR: " + ExceptionUtils.getRootCause(e));
            System.exit(1);
        }
    }

    private static String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private static S3AsyncClient newS3AsyncClient(String endpoint, String region, boolean forcePathStyle,
        List<AwsCredentialsProvider> credentialsProviders) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        builder.serviceConfiguration(c -> c.pathStyleAccessEnabled(forcePathStyle));
        builder.credentialsProvider(AwsCredentialsProviderChain.builder().credentialsProviders(credentialsProviders).build());
        builder.overrideConfiguration(b -> b.apiCallTimeout(Duration.ofMinutes(1))
            .apiCallAttemptTimeout(Duration.ofSeconds(30)));
        return builder.build();
    }

    private static abstract class S3CheckTask implements AutoCloseable {
        protected final S3AsyncClient client;
        protected final String bucketName;
        private final String taskName;

        public S3CheckTask(S3Context context, String taskName) {
            this.client = newS3AsyncClient(context.endpoint, context.region, context.forcePathStyle, context.credentialsProviders);
            this.bucketName = context.bucketName;
            this.taskName = taskName;
        }

        protected static void showErrorInfo(Exception e) {
            if (e.getCause() instanceof S3Exception) {
                S3Exception se = (S3Exception) e.getCause();
                // Do not use system.err because automq admin tool suppress system.err
                System.out.println("get S3 exception: ");
                se.printStackTrace(System.out);
            } else {
                System.out.println("get other exception: ");
                e.printStackTrace(System.out);
            }
        }

        protected void run() {
        }

        public String getTaskName() {
            return taskName;
        }

        @Override
        public void close() {
            if (this.client != null) {
                client.close();
            }
        }
    }

    // This task is used to test s3 multipart upload
    private static class S3MultipartUploadTestTask extends ObjectOperationTask {
        private Random random = new Random();
        public S3MultipartUploadTestTask(S3Context context) {
            super(context, S3MultipartUploadTestTask.class.getSimpleName());
        }

        @Override
        public void run() {
            ByteBuf byteBuf = null;
            try {
                // Simple write/read/delete
                String uploadId = createMultipartUpload(client, bucketName, path).get();
                List<CompletedPart> parts = new ArrayList<>();
                int data1Size = 1024 * 1024 * 5;
                int data2Size = 1024;
                int totalSize = data1Size + data2Size;

                byte[] randomBytes = new byte[data1Size];
                random.nextBytes(randomBytes);
                ByteBuf data1 = Unpooled.wrappedBuffer(randomBytes);
                writePart(uploadId, path, bucketName, data1, 1).thenAccept(parts::add).get();

                byte[] randomBytes2 = new byte[data2Size];
                random.nextBytes(randomBytes2);
                ByteBuf data2 = Unpooled.wrappedBuffer(randomBytes2);
                writePart(uploadId, path, bucketName, data2, 2).thenAccept(parts::add).get();

                System.out.println("[ OK ] Write S3 object");

                completeMultipartUpload(client, path, bucketName, uploadId, parts).get();
                System.out.println("[ OK ] Upload s3 multipart object");

                CompletableFuture<ByteBuf> readCf = new CompletableFuture<>();
                readRange(client, path, readCf, bucketName, 0, -1);
                byteBuf = readCf.get();
                if (byteBuf == null) {
                    System.out.println("[ FAILED ] Read s3 multipart object");
                    throw new RuntimeException("read multipart object " + path + " fail. got null");
                } else if (byteBuf.readableBytes() != totalSize) {
                    System.out.println("[ FAILED ] Read s3 multipart object");
                    throw new RuntimeException("read multipart object " + path + " fail. expected size " + totalSize + ", actual size " + byteBuf.readableBytes());
                }
                System.out.println("[ OK ] Read s3 multipart object");
            } catch (ExecutionException | InterruptedException e) {
                showErrorInfo(e);
                throw new RuntimeException(e);
            } finally {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        }

        private CompletableFuture<String> createMultipartUpload(S3AsyncClient writeS3Client, String bucketName,
            String path) {
            CompletableFuture<String> cf = new CompletableFuture<>();
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder().bucket(bucketName).key(path).build();
            writeS3Client.createMultipartUpload(request).thenAccept(createMultipartUploadResponse -> {
                cf.complete(createMultipartUploadResponse.uploadId());
            }).exceptionally(ex -> {
                System.out.println("[ FAILED ] Upload s3 multipart object");
                cf.completeExceptionally(ex);
                return null;
            });
            return cf;
        }

        public CompletableFuture<Void> completeMultipartUpload(S3AsyncClient writeS3Client, String path, String bucket,
            String uploadId, List<CompletedPart> parts) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(parts).build();
            CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();

            writeS3Client.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> {
                cf.complete(null);
            }).exceptionally(ex -> {
                System.out.println("[ FAILED ] Upload s3 multipart object, upload id is " + uploadId);
                cf.completeExceptionally(ex);
                return null;
            });
            return cf;
        }

        private CompletableFuture<CompletedPart> writePart(String uploadId, String path, String bucket, ByteBuf data,
            int partNum) {
            CompletableFuture<CompletedPart> cf = new CompletableFuture<>();
            uploadPart(client, cf, path, uploadId, partNum, bucket, data);
            return cf;
        }

        private void uploadPart(S3AsyncClient writeS3Client, CompletableFuture<CompletedPart> cf, String path,
            String uploadId, int partNumber, String bucket, ByteBuf part) {
            AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(part.nioBuffers());
            UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
                .partNumber(partNumber).build();
            CompletableFuture<UploadPartResponse> uploadPartCf = writeS3Client.uploadPart(request, body);
            uploadPartCf.thenAccept(uploadPartResponse -> {
                CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
                cf.complete(completedPart);
            }).exceptionally(ex -> {
                cf.completeExceptionally(ex);
                return null;
            });
            cf.whenComplete((rst, ex) -> part.release());
        }
    }

    private static class ObjectOperationTask extends S3CheckTask {
        protected final String path;

        public ObjectOperationTask(S3Context context) {
            this(context, ObjectOperationTask.class.getSimpleName());
        }

        protected ObjectOperationTask(S3Context context, String taskName) {
            super(context, taskName);
            this.path = String.format("%d/%s", System.nanoTime(), getTaskName());
        }

        @Override
        public void run() {
            byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
            ByteBuf byteBuf = null;
            try {
                // Simple write/read/delete
                CompletableFuture<Void> writeCf = new CompletableFuture<>();
                writeObject(client, path, ByteBuffer.wrap(content), writeCf, bucketName);
                writeCf.get();
                System.out.println("[ OK ] Write s3 object");

                CompletableFuture<ByteBuf> readCf = new CompletableFuture<>();
                readRange(client, path, readCf, bucketName, 0, -1);
                byteBuf = readCf.get();
                if (byteBuf == null) {
                    System.out.println("[ Failed ] Read s3 object");
                    throw new RuntimeException("read object " + path + " fail. got null");
                } else if (byteBuf.readableBytes() != content.length) {
                    System.out.println("[ Failed ] Read s3 object");
                    throw new RuntimeException("read object " + path + " fail. expected size " + content.length + ", actual size " + byteBuf.readableBytes());
                }
                byte[] readContent = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(readContent);
                if (!StringUtils.equals(new String(readContent, StandardCharsets.UTF_8), new String(content, StandardCharsets.UTF_8))) {
                    System.out.println("[ Failed ] Read s3 object");
                    throw new RuntimeException("read object " + path + " fail. expected content " + new String(content, StandardCharsets.UTF_8) + ", actual content " + new String(readContent, StandardCharsets.UTF_8));
                }
                System.out.println("[ OK ] Read s3 object");
            } catch (ExecutionException | InterruptedException e) {
                showErrorInfo(e);
                throw new RuntimeException(e);
            } finally {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        }

        private void writeObject(S3AsyncClient writeS3Client, String path, ByteBuffer data, CompletableFuture<Void> cf,
            String bucket) {
            PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(path).build();
            AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data);
            writeS3Client.putObject(request, body).thenAccept(putObjectResponse -> {
                cf.complete(null);
            }).exceptionally(ex -> {
                System.out.printf("[ Failed ] Write s3 object. PutObject for object %s fail with msg %s %n", path, ex.getMessage());
                cf.completeExceptionally(ex);
                return null;
            });
        }

        protected void readRange(S3AsyncClient readS3Client, String path, CompletableFuture<ByteBuf> cf, String bucket,
            long start, long end) {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
            readS3Client.getObject(request, AsyncResponseTransformer.toPublisher())
                .thenAccept(responsePublisher -> {
                    CompositeByteBuf buf = DirectByteBufAlloc.compositeByteBuffer();
                    responsePublisher.subscribe((bytes) -> {
                        // the aws client will copy DefaultHttpContent to heap ByteBuffer
                        buf.addComponent(true, Unpooled.wrappedBuffer(bytes));
                    }).thenAccept(v -> {
                        cf.complete(buf);
                    });
                }).exceptionally(ex -> {
                    cf.completeExceptionally(ex);
                    return null;
                });
        }

        protected void deleteObject(S3AsyncClient deleteS3Client, String path, CompletableFuture<Void> cf,
            String bucket) {
            DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(path).build();
            deleteS3Client.deleteObject(request).thenAccept(deleteObjectResponse -> {
                cf.complete(null);
            }).exceptionally(ex -> {
                System.out.printf("[ FAILED ] Delete s3 object. Delete object %s fail with msg %s %n", path, ex.getMessage());
                cf.completeExceptionally(ex);
                return null;
            });
        }

        @Override
        public void close() {
            try {
                CompletableFuture<Void> deleteCf = new CompletableFuture<>();
                deleteObject(client, path, deleteCf, bucketName);
                deleteCf.get();
            } catch (InterruptedException | ExecutionException e) {

                System.out.println("[ FAILED ] Delete s3 object. NOTICE: please delete object " + path + " manually!!!");
                showErrorInfo(e);
                throw new RuntimeException(e);
            } finally {
                super.close();
            }
            System.out.println("[ OK ] Delete s3 object");

        }
    }

    public static class S3Context {
        private final String endpoint;
        private final List<AwsCredentialsProvider> credentialsProviders;
        private final String bucketName;
        private final String region;
        private final boolean forcePathStyle;

        public S3Context(String endpoint, List<AwsCredentialsProvider> credentialsProviders, String bucketName,
            String region,
            boolean forcePathStyle) {
            this.endpoint = endpoint;
            this.credentialsProviders = credentialsProviders;
            this.bucketName = bucketName;
            this.region = region;
            this.forcePathStyle = forcePathStyle;
        }

        public static Builder builder() {
            return new Builder();
        }

        public List<String> advices() {
            List<String> advises = new ArrayList<>();
            if (StringUtils.isBlank(bucketName)) {
                advises.add("bucketName is blank. Please supply a valid bucketName.");
            }
            if (StringUtils.isBlank(endpoint)) {
                advises.add("endpoint is blank. Please supply a valid endpoint.");
            } else {
                if (endpoint.startsWith("https")) {
                    advises.add("You are using https endpoint. Please make sure your object storage service supports https.");
                }
                String[] splits = endpoint.split("//");
                if (splits.length < 2) {
                    advises.add("endpoint is invalid. Please supply a valid endpoint.");
                } else {
                    String[] dotSplits = splits[1].split("\\.");
                    if (dotSplits.length == 0 || StringUtils.isBlank(dotSplits[0])) {
                        advises.add("endpoint is invalid. Please supply a valid endpoint.");
                    } else if (!StringUtils.isBlank(bucketName) && Objects.equals(bucketName.toLowerCase(), dotSplits[0].toLowerCase())) {
                        advises.add("bucket name should not be included in endpoint.");
                    }
                }
            }
            if (credentialsProviders == null || credentialsProviders.isEmpty()) {
                advises.add("no credentials provider is supplied. Please supply a credentials provider.");
            }
            try (AwsCredentialsProviderChain chain = AwsCredentialsProviderChain.builder().credentialsProviders(credentialsProviders).build()) {
                chain.resolveCredentials();
            } catch (SdkClientException e) {
                advises.add("all provided credentials providers are invalid. Please supply a valid credentials provider. Error msg: " + e.getMessage());
            }
            if (StringUtils.isBlank(region)) {
                advises.add("region is blank. Please supply a valid region.");
            }
            if (!forcePathStyle) {
                advises.add("forcePathStyle is set as false. Please set it as true if you are using minio.");
            }
            return advises;
        }

        @Override
        public String toString() {
            return "S3CheckContext{" +
                "endpoint='" + endpoint + '\'' +
                ", credentialsProviders=" + credentialsProviders +
                ", bucketName='" + bucketName + '\'' +
                ", region='" + region + '\'' +
                ", forcePathStyle=" + forcePathStyle +
                '}';
        }

        public static class Builder {
            private String endpoint;
            private List<AwsCredentialsProvider> credentialsProviders;
            private String bucketName;
            private String region;
            private boolean forcePathStyle;

            public Builder setEndpoint(String endpoint) {
                this.endpoint = endpoint;
                return this;
            }

            public Builder setCredentialsProviders(List<AwsCredentialsProvider> credentialsProviders) {
                this.credentialsProviders = credentialsProviders;
                return this;
            }

            public Builder setBucketName(String bucketName) {
                this.bucketName = bucketName;
                return this;
            }

            public Builder setRegion(String region) {
                this.region = region;
                return this;
            }

            public Builder setForcePathStyle(boolean forcePathStyle) {
                this.forcePathStyle = forcePathStyle;
                return this;
            }

            public S3Context build() {
                return new S3Context(endpoint, credentialsProviders, bucketName, region, forcePathStyle);
            }
        }

    }
}
