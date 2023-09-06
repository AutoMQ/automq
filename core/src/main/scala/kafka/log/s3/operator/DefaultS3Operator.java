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

package kafka.log.s3.operator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import kafka.log.es.metrics.Counter;
import kafka.log.es.metrics.Timer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
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
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DefaultS3Operator implements S3Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Operator.class);
    public static final String ACCESS_KEY_NAME = "KAFKA_S3_ACCESS_KEY";
    public static final String SECRET_KEY_NAME = "KAFKA_S3_SECRET_KEY";
    private final String bucket;
    private final S3AsyncClient s3;
    private static final Timer PART_UPLOAD_COST = new Timer();
    private static final Timer OBJECT_UPLOAD_COST = new Timer();
    private static final Counter OBJECT_UPLOAD_SIZE = new Counter();
    private static final AtomicLong LAST_LOG_TIMESTAMP = new AtomicLong(System.currentTimeMillis());
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3operator", true));

    public DefaultS3Operator(String endpoint, String region, String bucket) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        String accessKey = System.getenv(ACCESS_KEY_NAME);
        String secretKey = System.getenv(SECRET_KEY_NAME);
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            builder.credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            builder.credentialsProvider(() -> AwsBasicCredentials.create("noop", "noop"));
            LOGGER.info("Cannot find s3 credential {} {} from environment", ACCESS_KEY_NAME, SECRET_KEY_NAME);
        }
        this.s3 = builder.build();
        this.bucket = bucket;
        LOGGER.info("S3Operator init with endpoint={} region={} bucket={}", endpoint, region, bucket);
    }

    @Override
    public void close() {
        // TODO: complete inflight CompletableFuture with ClosedException.
        s3.close();
        scheduler.shutdown();
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ByteBufAllocator alloc) {
        end = end - 1;
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        ByteBuf buf = alloc.heapBuffer((int) (end - start));
        rangeRead0(path, start, end, buf, cf);
        return cf;
    }

    private void rangeRead0(String path, long start, long end, ByteBuf buf, CompletableFuture<ByteBuf> cf) {
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(path).range(range(start, end)).build();
        // TODO: self defined transformer to avoid bytes copy.
        s3.getObject(request, AsyncResponseTransformer.toBytes()).thenAccept(responseBytes -> {
            buf.writeBytes(responseBytes.asByteArrayUnsafe());
            cf.complete(buf);
        }).exceptionally(ex -> {
            if (isUnrecoverable(ex)) {
                LOGGER.error("GetObject for object {} [{}, {})fail", path, start, end, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("GetObject for object {} [{}, {})fail, retry later", path, start, end, ex);
                scheduler.schedule(() -> rangeRead0(path, start, end, buf, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        write0(path, data, cf);
        return cf;
    }

    private void write0(String path, ByteBuf data, CompletableFuture<Void> cf) {
        long now = System.currentTimeMillis();
        int objectSize = data.readableBytes();
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(path).build();
        AsyncRequestBody body = AsyncRequestBody.fromByteBuffer(data.nioBuffer());
        s3.putObject(request, body).thenAccept(putObjectResponse -> {
            LOGGER.debug("put object {} with size {}, cost {}ms", path, objectSize, System.currentTimeMillis() - now);
            cf.complete(null);
        }).exceptionally(ex -> {
            if (isUnrecoverable(ex)) {
                LOGGER.error("PutObject for object {} fail", path, ex);
                cf.completeExceptionally(ex);
            } else {
                LOGGER.warn("PutObject for object {} fail, retry later", path, ex);
                scheduler.schedule(() -> write0(path, data, cf), 100, TimeUnit.MILLISECONDS);
            }
            return null;
        });
    }

    @Override
    public Writer writer(String path) {
        return new DefaultWriter(path);
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        long now = System.currentTimeMillis();
        DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(path).build();
        return s3.deleteObject(request).thenApply(deleteObjectResponse -> {
            LOGGER.debug("delete object {}, cost {}ms", path, System.currentTimeMillis() - now);
            return null;
        });
    }

    private String range(long start, long end) {
        if (end == -1L) {
            return "bytes=" + start + "-";
        }
        return "bytes=" + start + "-" + end;
    }

    private static boolean isUnrecoverable(Throwable ex) {
        return ex instanceof NoSuchKeyException || ex instanceof NoSuchBucketException;
    }


    class DefaultWriter implements Writer {
        private final String path;
        private final CompletableFuture<String> uploadIdCf = new CompletableFuture<>();
        private volatile String uploadId;
        private final List<CompletableFuture<CompletedPart>> parts = new LinkedList<>();
        private final AtomicInteger nextPartNumber = new AtomicInteger(1);
        private CompletableFuture<Void> closeCf;
        private final long start = System.nanoTime();

        public DefaultWriter(String path) {
            this.path = path;
            init();
        }

        private void init() {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder().bucket(bucket).key(path).build();
            s3.createMultipartUpload(request).thenAccept(createMultipartUploadResponse -> {
                uploadId = createMultipartUploadResponse.uploadId();
                uploadIdCf.complete(createMultipartUploadResponse.uploadId());
            }).exceptionally(ex -> {
                if (isUnrecoverable(ex)) {
                    LOGGER.error("CreateMultipartUpload for object {} fail", path, ex);
                    uploadIdCf.completeExceptionally(ex);
                } else {
                    LOGGER.warn("CreateMultipartUpload for object {} fail, retry later", path, ex);
                    scheduler.schedule(this::init, 100, TimeUnit.MILLISECONDS);
                }
                return null;
            });
        }

        @Override
        public void write(ByteBuf part) {
            long start = System.nanoTime();
            OBJECT_UPLOAD_SIZE.inc(part.readableBytes());
            int partNumber = nextPartNumber.getAndIncrement();
            CompletableFuture<CompletedPart> partCf = new CompletableFuture<>();
            parts.add(partCf);
            uploadIdCf.thenAccept(uploadId -> write0(uploadId, partNumber, part, partCf));
            partCf.whenComplete((nil, ex) -> {
                PART_UPLOAD_COST.update(System.nanoTime() - start);
                part.release();
            });
        }

        private void write0(String uploadId, int partNumber, ByteBuf part, CompletableFuture<CompletedPart> partCf) {
            AsyncRequestBody body = AsyncRequestBody.fromByteBuffer(part.nioBuffer());
            UploadPartRequest request = UploadPartRequest.builder().bucket(bucket).key(path).uploadId(uploadId)
                    .partNumber(partNumber).build();
            CompletableFuture<UploadPartResponse> uploadPartCf = s3.uploadPart(request, body);
            uploadPartCf.thenAccept(uploadPartResponse -> {
                CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
                partCf.complete(completedPart);
            }).exceptionally(ex -> {
                if (isUnrecoverable(ex)) {
                    LOGGER.error("UploadPart for object {}-{} fail", path, partNumber, ex);
                    partCf.completeExceptionally(ex);
                } else {
                    LOGGER.warn("UploadPart for object {}-{} fail, retry later", path, partNumber, ex);
                    scheduler.schedule(() -> write0(uploadId, partNumber, part, partCf), 100, TimeUnit.MILLISECONDS);
                }
                return null;
            });
        }

        @Override
        public void copyWrite(String sourcePath, long start, long end) {
            long inclusiveEnd = end - 1;
            int partNumber = nextPartNumber.getAndIncrement();
            CompletableFuture<CompletedPart> partCf = new CompletableFuture<>();
            parts.add(partCf);
            uploadIdCf.thenAccept(uploadId -> {
                UploadPartCopyRequest request = UploadPartCopyRequest.builder().sourceBucket(bucket).sourceKey(sourcePath)
                        .destinationBucket(bucket).destinationKey(path).copySourceRange(range(start, inclusiveEnd)).uploadId(uploadId).partNumber(partNumber).build();
                copyWrite0(partNumber, request, partCf);
            });
        }

        private void copyWrite0(int partNumber, UploadPartCopyRequest request, CompletableFuture<CompletedPart> partCf) {
            s3.uploadPartCopy(request).thenAccept(uploadPartCopyResponse -> {
                CompletedPart completedPart = CompletedPart.builder().partNumber(partNumber)
                        .eTag(uploadPartCopyResponse.copyPartResult().eTag()).build();
                partCf.complete(completedPart);
            }).exceptionally(ex -> {
                LOGGER.warn("UploadPartCopy for object {}-{} fail, retry later", path, partNumber, ex);
                scheduler.schedule(() -> copyWrite0(partNumber, request, partCf), 100, TimeUnit.MILLISECONDS);
                return null;
            });
        }

        @Override
        public CompletableFuture<Void> close() {
            if (closeCf != null) {
                return closeCf;
            }
            System.out.println("start await close: " + (System.nanoTime() - start) / 1000 / 1000);
            closeCf = new CompletableFuture<>();
            CompletableFuture<Void> uploadDoneCf = uploadIdCf.thenCompose(uploadId -> CompletableFuture.allOf(parts.toArray(new CompletableFuture[0])));
            uploadDoneCf.thenAccept(nil -> {
                System.out.println("start complete: " + (System.nanoTime() - start) / 1000 / 1000);
                CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(genCompleteParts()).build();
                CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).multipartUpload(multipartUpload).build();
                close0(request);
            });
            closeCf.whenComplete((nil, ex) -> {
                System.out.println("complete: " + (System.nanoTime() - start) / 1000 / 1000);
                OBJECT_UPLOAD_COST.update(System.nanoTime() - start);
                long now = System.currentTimeMillis();
                if (now - LAST_LOG_TIMESTAMP.get() > 10000) {
                    LAST_LOG_TIMESTAMP.set(now);
                    LOGGER.info("upload s3 metrics, object_timer {}, object_size {}, part_timer {}",
                            OBJECT_UPLOAD_COST.getAndReset(), OBJECT_UPLOAD_SIZE.getAndReset(), PART_UPLOAD_COST.getAndReset());
                }
            });
            return closeCf;
        }

        private void close0(CompleteMultipartUploadRequest request) {
            s3.completeMultipartUpload(request).thenAccept(completeMultipartUploadResponse -> closeCf.complete(null)).exceptionally(ex -> {
                if (isUnrecoverable(ex)) {
                    LOGGER.error("CompleteMultipartUpload for object {} fail", path, ex);
                    closeCf.completeExceptionally(ex);
                } else {
                    LOGGER.warn("CompleteMultipartUpload for object {} fail, retry later", path, ex);
                    scheduler.schedule(() -> close0(request), 100, TimeUnit.MILLISECONDS);
                }
                return null;
            });
        }

        private List<CompletedPart> genCompleteParts() {
            return this.parts.stream().map(cf -> {
                try {
                    return cf.get();
                } catch (Throwable e) {
                    // won't happen.
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }
    }
}
