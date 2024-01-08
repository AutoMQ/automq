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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
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
        System.out.println("You are using s3 context: " + context);
        System.out.println("====== 1/2: object operation task starting ======");
        try (ObjectOperationTask task = new ObjectOperationTask(context)) {
            task.run();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.out.println("====== 1/2: object operation task passed ======");

        System.out.println("====== 2/2: multipart object operation task starting ======");
        try (MultipartObjectOperationTask task = new MultipartObjectOperationTask(context)) {
            task.run();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.out.println("====== 2/2: multipart object operation task passed ======");

        System.out.println("====== Congratulations! You have passed all checks!!! ======");

    }

    private static String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private static S3AsyncClient newS3AsyncClient(String endpoint, String region, boolean forcePathStyle,
        String accessKey, String secretKey) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        builder.serviceConfiguration(c -> c.pathStyleAccessEnabled(forcePathStyle));
        builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        builder.overrideConfiguration(b -> b.apiCallTimeout(Duration.ofMinutes(1))
            .apiCallAttemptTimeout(Duration.ofSeconds(30)));
        return builder.build();
    }

    private static String hideSecret(String secret) {
        if (secret == null) {
            return null;
        }
        if (secret.length() < 6) {
            return "*".repeat(secret.length());
        }
        return secret.substring(0, 3) + "*".repeat(secret.length() - 6) + secret.substring(secret.length() - 3);
    }

    private static abstract class S3CheckTask implements AutoCloseable {
        protected final S3AsyncClient client;
        protected final String bucketName;
        private final String taskName;

        public S3CheckTask(S3Context context, String taskName) {
            this.client = newS3AsyncClient(context.endpoint, context.region, context.forcePathStyle, context.accessKey, context.secretKey);
            this.bucketName = context.bucketName;
            this.taskName = taskName;
        }

        protected static void showErrorInfo(Exception e) {
            if (e.getCause() instanceof S3Exception se) {
                System.err.println("get S3 exception: ");
                se.printStackTrace();
            } else {
                System.err.println("get other exception: ");
                e.printStackTrace();
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

    private static class MultipartObjectOperationTask extends ObjectOperationTask {
        public MultipartObjectOperationTask(S3Context context) {
            super(context, MultipartObjectOperationTask.class.getSimpleName());
        }

        @Override
        public void run() {
            ByteBuf byteBuf = null;
            try {
                // Simple write/read/delete
                System.out.println("1) Trying to write multipart object " + path + " ...");
                String uploadId = createMultipartUpload(client, bucketName, path).get();
                List<CompletedPart> parts = new ArrayList<>();
                int data1Size = 1024 * 1024 * 5;
                int data2Size = 1024;
                int totalSize = data1Size + data2Size;

                byte[] randomBytes = new byte[data1Size];
                new Random().nextBytes(randomBytes);
                ByteBuf data1 = Unpooled.wrappedBuffer(randomBytes);
                writePart(uploadId, path, bucketName, data1, 1).thenAccept(parts::add).get();
                System.out.println("writing part 1 passed");

                byte[] randomBytes2 = new byte[data2Size];
                new Random().nextBytes(randomBytes2);
                ByteBuf data2 = Unpooled.wrappedBuffer(randomBytes2);
                writePart(uploadId, path, bucketName, data2, 2).thenAccept(parts::add).get();
                System.out.println("writing part 2 passed");

                completeMultipartUpload(client, path, bucketName, uploadId, parts).get();
                System.out.println("writing and uploading multipart object passed");

                System.out.println("2) Trying to read multipart object " + path + " ...");
                CompletableFuture<ByteBuf> readCf = new CompletableFuture<>();
                readRange(client, path, readCf, bucketName, 0, -1);
                byteBuf = readCf.get();
                if (byteBuf == null) {
                    throw new RuntimeException("read multipart object " + path + " fail. got null");
                } else if (byteBuf.readableBytes() != totalSize) {
                    throw new RuntimeException("read multipart object " + path + " fail. expected size " + totalSize + ", actual size " + byteBuf.readableBytes());
                }
                System.out.println("read passed");
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
                System.out.println("created upload id: " + createMultipartUploadResponse.uploadId());
                cf.complete(createMultipartUploadResponse.uploadId());
            }).exceptionally(ex -> {
                System.err.println("failed to create upload id.");
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
                System.out.println("completed upload with id " + uploadId);
                cf.complete(null);
            }).exceptionally(ex -> {
                System.err.println("failed to upload with id " + uploadId);
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
                System.out.println("1) Trying to write object " + path + " ...");
                CompletableFuture<Void> writeCf = new CompletableFuture<>();
                writeObject(client, path, ByteBuffer.wrap(content), writeCf, bucketName);
                writeCf.get();

                System.out.println("2) Trying to read object " + path + " ...");
                CompletableFuture<ByteBuf> readCf = new CompletableFuture<>();
                readRange(client, path, readCf, bucketName, 0, -1);
                byteBuf = readCf.get();
                if (byteBuf == null) {
                    throw new RuntimeException("read object " + path + " fail. got null");
                } else if (byteBuf.readableBytes() != content.length) {
                    throw new RuntimeException("read object " + path + " fail. expected size " + content.length + ", actual size " + byteBuf.readableBytes());
                }
                byte[] readContent = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(readContent);
                if (!StringUtils.equals(new String(readContent, StandardCharsets.UTF_8), new String(content, StandardCharsets.UTF_8))) {
                    throw new RuntimeException("read object " + path + " fail. expected content " + new String(content, StandardCharsets.UTF_8) + ", actual content " + new String(readContent, StandardCharsets.UTF_8));
                }
                System.out.println("read passed");
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
            int objectSize = data.remaining();
            PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(path).build();
            AsyncRequestBody body = AsyncRequestBody.fromByteBuffersUnsafe(data);
            writeS3Client.putObject(request, body).thenAccept(putObjectResponse -> {
                System.out.printf("put object %s with size %d%n", path, objectSize);
                cf.complete(null);
            }).exceptionally(ex -> {
                System.err.printf("PutObject for object %s fail with msg %s %n", path, ex.getMessage());
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
                System.out.printf("deleted object %s%n", path);
                cf.complete(null);
            }).exceptionally(ex -> {
                System.err.printf("delete object %s fail with msg %s %n", path, ex.getMessage());
                cf.completeExceptionally(ex);
                return null;
            });
        }

        @Override
        public void close() {
            System.out.println("3) Trying to delete object " + path + " ...");
            try {
                CompletableFuture<Void> deleteCf = new CompletableFuture<>();
                deleteObject(client, path, deleteCf, bucketName);
                deleteCf.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println(" NOTICE: please delete object " + path + " manually!!!");
                showErrorInfo(e);
                throw new RuntimeException(e);
            } finally {
                super.close();
            }
        }
    }

    public static class S3Context {
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;
        private final String bucketName;
        private final String region;
        private final boolean forcePathStyle;

        public S3Context(String endpoint, String accessKey, String secretKey, String bucketName, String region,
            boolean forcePathStyle) {
            this.endpoint = endpoint;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
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
            if (StringUtils.isBlank(accessKey)) {
                advises.add("accessKey is blank. Please supply a valid accessKey.");
            }
            if (StringUtils.isBlank(secretKey)) {
                advises.add("secretKey is blank. Please supply a valid secretKey.");
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
                ", accessKey='" + hideSecret(accessKey) + '\'' +
                ", secretKey='" + hideSecret(secretKey) + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", region='" + region + '\'' +
                ", forcePathStyle=" + forcePathStyle +
                '}';
        }

        public static class Builder {
            private String endpoint;
            private String accessKey;
            private String secretKey;
            private String bucketName;
            private String region;
            private boolean forcePathStyle;

            public Builder setEndpoint(String endpoint) {
                this.endpoint = endpoint;
                return this;
            }

            public Builder setAccessKey(String accessKey) {
                this.accessKey = accessKey;
                return this;
            }

            public Builder setSecretKey(String secretKey) {
                this.secretKey = secretKey;
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
                return new S3Context(endpoint, accessKey, secretKey, bucketName, region, forcePathStyle);
            }
        }

    }
}
