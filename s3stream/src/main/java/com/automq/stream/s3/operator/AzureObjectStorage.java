package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.FutureUtil;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlockBlobStageBlockOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metrics.operations.S3Operation.COMPLETE_MULTI_PART_UPLOAD;
import static com.automq.stream.s3.metrics.operations.S3Operation.GET_OBJECT;
import static com.automq.stream.s3.operator.AwsObjectStorage.AUTH_TYPE_KEY;
import static com.automq.stream.s3.operator.AwsObjectStorage.INSTANCE_AUTH_TYPE;
import static com.automq.stream.s3.operator.AwsObjectStorage.STATIC_AUTH_TYPE;
import static com.azure.storage.blob.models.BlobErrorCode.AUTHORIZATION_FAILURE;
import static com.azure.storage.blob.models.BlobErrorCode.BLOB_NOT_FOUND;

public class AzureObjectStorage extends AbstractObjectStorage {

    static final Logger LOGGER = LoggerFactory.getLogger(AzureObjectStorage.class);
    private final BlobContainerAsyncClient blobContainerWriteAsyncClient;
    private final BlobContainerAsyncClient blobContainerReadAsyncClient;
    private final BlobServiceAsyncClient blobServiceAsyncClient;
    private final AtomicLong uploadIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<String, BlockBlobAsyncClient> blockBlobAsyncClientHashMap = new ConcurrentHashMap<>();

    // Each batch request supports a maximum of 256 sub requests.
    // https://docs.microsoft.com/en-us/rest/api/storageservices/blob-batch
    private static final int AZURE_DELETE_OBJECTS_MAX_BATCH_SIZE = 256;
    private static final int AZURE_DELETE_OBJECTS_MAX_CONCURRENT_REQUEST_NUMBER = getMaxObjectStorageConcurrency();
    private static final String AZURE_BLOB_CONNECTION_PROVIDER_NAME = "azure-connection-provider";
    private static final String AZURE_MANAGED_IDENTITY_CLIENT_ID = "role";
    private static final String AZURE_SERVICE_PRINCIPAL_TENANT_ID = "tenant";
    private static final String ENV_AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    private static final String ENV_AZURE_TENANT_ID = "AZURE_TENANT_ID";
    private static final String ENV_KAFKA_S3_SECRET_KEY = "KAFKA_S3_SECRET_KEY";
    private static final String ENV_KAFKA_S3_ACCESS_KEY = "KAFKA_S3_ACCESS_KEY";
    private static final Duration SAS_TOKEN_REFRESH_THRESHOLD = Duration.ofMinutes(30);

    private volatile String sasToken;
    private volatile OffsetDateTime sasTokenExpiryTime;

    // used for test only
    public AzureObjectStorage(String bucketStr) {
        super(BucketURI.parse(bucketStr), NetworkBandwidthLimiter.NOOP, NetworkBandwidthLimiter.NOOP, 50, 0, true, false, false, "test");
        BucketURI bucketURI = BucketURI.parse(bucketStr);
        blobContainerWriteAsyncClient = newBlobServiceAsyncClient(bucketURI).getBlobContainerAsyncClient(bucketURI.bucket());
        blobContainerReadAsyncClient = blobContainerWriteAsyncClient;
        blobServiceAsyncClient = blobContainerWriteAsyncClient.getServiceAsyncClient();
    }

    public AzureObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
                              NetworkBandwidthLimiter networkInboundBandwidthLimiter,
                              NetworkBandwidthLimiter networkOutboundBandwidthLimiter,
                              boolean readWriteIsolate, boolean checkS3ApiMode, String threadPrefix) {
        super(bucketURI, networkInboundBandwidthLimiter, networkOutboundBandwidthLimiter, readWriteIsolate, checkS3ApiMode, threadPrefix);
        blobContainerWriteAsyncClient = newBlobServiceAsyncClient(bucketURI).getBlobContainerAsyncClient(bucketURI.bucket());
        blobContainerReadAsyncClient = readWriteIsolate ? newBlobServiceAsyncClient(bucketURI).getBlobContainerAsyncClient(bucketURI.bucket()) : blobContainerWriteAsyncClient;
        blobServiceAsyncClient = blobContainerWriteAsyncClient.getServiceAsyncClient();
    }

    @Override
    public CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        BlobAsyncClient client = blobContainerReadAsyncClient.getBlobAsyncClient(path);
        CompositeByteBuf compositeByteBuf = ByteBufAlloc.compositeByteBuffer();

        client.downloadStreamWithResponse(range(start, end), null, null, false)
            .toFuture()
            .thenCompose(response -> response.getValue()
                .map(Unpooled::wrappedBuffer)
                .reduce(compositeByteBuf, (acc, buf) -> {
                    acc.addComponent(true, buf);
                    return acc;
                })
                .toFuture())
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    compositeByteBuf.release();
                    cf.completeExceptionally(ex);
                } else {
                    cf.complete(compositeByteBuf);
                }
            });
        return cf;
    }

    @Override
    CompletableFuture<Void> doWrite(WriteOptions writeOptions, String path, ByteBuf data) {
        BlobAsyncClient blobAsyncClient = blobContainerWriteAsyncClient.getBlobAsyncClient(path);
        BinaryData binaryData = BinaryData.fromListByteBuffer(List.of(data.nioBuffers()));
        BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(binaryData)
            .setComputeMd5(true);
        return blobAsyncClient.uploadWithResponse(uploadOptions).then().toFuture();
    }

    @Override
    CompletableFuture<String> doCreateMultipartUpload(WriteOptions writeOptions, String path) {
        BlockBlobAsyncClient blockBlobAsyncClient = blobContainerWriteAsyncClient.getBlobAsyncClient(path).getBlockBlobAsyncClient();
        String uploadId = String.valueOf(uploadIdGenerator.incrementAndGet());
        blockBlobAsyncClientHashMap.put(uploadId, blockBlobAsyncClient);
        return CompletableFuture.completedFuture(uploadId);
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions writeOptions, String path, String uploadId, int partNumber, ByteBuf part) {

        if (part.readableBytes() > 100 * 1024 * 1024) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Part size exceeds 100MB limit"));
        }
        BinaryData binaryData = BinaryData.fromListByteBuffer(List.of(part.nioBuffers()));
        BlockBlobAsyncClient blockBlobAsyncClient = blockBlobAsyncClientHashMap.get(uploadId);
        if (blockBlobAsyncClient == null) {
            return CompletableFuture.failedFuture(NoSuchUploadException.builder().message(String.format("No such upload, uploadId: %s, path: %s", uploadId, path)).build());
        }

        String base64BlockId = generateBlockId(partNumber);
        long timeoutMs = writeOptions.apiCallAttemptTimeout();
        BlockBlobStageBlockOptions blockBlobStageBlockOptions = new BlockBlobStageBlockOptions(base64BlockId, binaryData);
        Mono<Response<Void>> stageBlockMono = blockBlobAsyncClient.stageBlockWithResponse(blockBlobStageBlockOptions);
        if (timeoutMs > 0) {
            stageBlockMono = stageBlockMono.timeout(Duration.ofMillis(timeoutMs));
        }
        return stageBlockMono
            .toFuture()
            .thenApply(response ->
                new ObjectStorageCompletedPart(partNumber, base64BlockId, "")
            )
            .exceptionally(ex -> {
                LOGGER.error("Upload part failed [uploadId={}, partNumber={}]", uploadId, partNumber, ex);
                throw new CompletionException(ex);
            });
    }

    @Override
    public CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions writeOptions, String sourcePath, String path, long start, long end, String uploadId, int partNumber) {

        if (start > end) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(String.format("Invalid range: start=%s, end=%s", start, end)));
        }

        BlobAsyncClient sourceBlobClient = blobContainerReadAsyncClient.getBlobAsyncClient(sourcePath);
        String sasToken = refreshSasTokenSync();
        String sourceUrl = sourceBlobClient.getBlobUrl() + "?" + sasToken;
        BlockBlobAsyncClient blockBlobAsyncClient = blockBlobAsyncClientHashMap.get(uploadId);
        if (blockBlobAsyncClient == null) {
            return CompletableFuture.failedFuture(NoSuchUploadException.builder().message(String.format("No such upload, uploadId: %s, path: %s", uploadId, path)).build());
        }
        String base64BlockId = generateBlockId(partNumber);
        long timeoutMs = writeOptions.apiCallAttemptTimeout();
        Mono<Void> stageBlockMono = blockBlobAsyncClient.stageBlockFromUrl(base64BlockId, sourceUrl, range(start, end));
        if (timeoutMs > 0) {
            stageBlockMono = stageBlockMono.timeout(Duration.ofMillis(timeoutMs));
        }
        return stageBlockMono
            .toFuture()
            .thenApply(response -> new ObjectStorageCompletedPart(
                partNumber,
                base64BlockId,
                ""
            ))
            .exceptionally(ex -> {
                LOGGER.error("Upload part copy failed [uploadId={}, partNumber={}]", uploadId, partNumber, ex);
                throw new CompletionException("Upload part copy failed", ex);
            });
    }

    @Override
    public CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions writeOptions, String path, String uploadId, List<ObjectStorageCompletedPart> parts) {
        BlockBlobAsyncClient blockBlobAsyncClient = blockBlobAsyncClientHashMap.get(uploadId);
        if (blockBlobAsyncClient == null) {
            return CompletableFuture.failedFuture(NoSuchUploadException.builder().message(String.format("No such upload, uploadId: %s, path: %s", uploadId, path)).build());
        }
        List<String> base64BlockIds = parts.stream()
            .sorted(Comparator.comparingInt(ObjectStorageCompletedPart::getPartNumber))
            .map(ObjectStorageCompletedPart::getPartId)
            .collect(Collectors.toList());
        return blockBlobAsyncClient.commitBlockList(base64BlockIds).then().toFuture()
            .thenCompose(nil -> {
                blockBlobAsyncClientHashMap.remove(uploadId);
                return CompletableFuture.completedFuture(null);
            });
    }

    @Override
    public CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        List<String> blobUrls = objectKeys.stream()
            .map(blobContainerReadAsyncClient::getBlobAsyncClient)
            .map(BlobAsyncClient::getBlobUrl)
            .collect(Collectors.toList());
        getBlobBatchAsyncClient(blobServiceAsyncClient).deleteBlobs(blobUrls, DeleteSnapshotsOptionType.INCLUDE)
            .subscribe(
                success -> cf.complete(null),
                ex -> {
                    if (ex instanceof BlobStorageException) {
                        BlobStorageException blobStorageException = (BlobStorageException) ex;
                        LOGGER.error("The batch request is malformed", blobStorageException);
                        cf.completeExceptionally(blobStorageException);
                    } else if (ex instanceof BlobBatchStorageException) {
                        try {
                            checkDeleteObjectsError(ex);
                            cf.complete(null);
                        } catch (DeleteObjectsException e) {
                            cf.completeExceptionally(e);
                        }
                    } else {
                        LOGGER.error("Unexpected error during batch delete", ex);
                        cf.completeExceptionally(ex);
                    }
                });
        return cf;
    }

    @Override
    protected DeleteObjectsAccumulator newDeleteObjectsAccumulator() {
        return new DeleteObjectsAccumulator(AZURE_DELETE_OBJECTS_MAX_BATCH_SIZE, AZURE_DELETE_OBJECTS_MAX_CONCURRENT_REQUEST_NUMBER, this::doDeleteObjects);
    }

    private BlobServiceAsyncClient newBlobServiceAsyncClient(BucketURI bucketURI) {
        String authType = bucketURI.extensionString(AUTH_TYPE_KEY, STATIC_AUTH_TYPE);
        String accountName = bucketURI.extensionString(BucketURI.ACCESS_KEY_KEY, System.getenv(ENV_KAFKA_S3_ACCESS_KEY));

        ConnectionProvider connectionProvider = ConnectionProvider.builder(AZURE_BLOB_CONNECTION_PROVIDER_NAME)
            .maxConnections(getMaxObjectStorageConcurrency())
            .build();
        HttpClient asyncHttpClient = new NettyAsyncHttpClientBuilder()
            .connectionProvider(connectionProvider)
            .build();
        BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
            .endpoint(bucketURI.endpoint())
            .httpClient(asyncHttpClient);

        return getBlobServiceAsyncClient(authType, accountName, clientBuilder);
    }

    private BlobServiceAsyncClient getBlobServiceAsyncClient(String authType, String accountName, BlobServiceClientBuilder clientBuilder) {
        switch (authType) {
            case STATIC_AUTH_TYPE: {
                String accountKey = bucketURI.extensionString(BucketURI.SECRET_KEY_KEY, System.getenv(ENV_KAFKA_S3_SECRET_KEY));
                String tenant = bucketURI.extensionString(AZURE_SERVICE_PRINCIPAL_TENANT_ID, System.getenv(ENV_AZURE_TENANT_ID));
                if (StringUtils.isEmpty(accountName) || StringUtils.isEmpty(accountKey) || StringUtils.isEmpty(tenant)) {
                    throw new IllegalArgumentException("Azure account name and key must be provided for static auth type");
                }
                return getStaticBlobContainerAsyncClient(accountName, accountKey, tenant, clientBuilder);
            }
            case INSTANCE_AUTH_TYPE: {
                return getInstanceProfileBlobContainerAsyncClient(clientBuilder,
                    bucketURI.extensionString(AZURE_MANAGED_IDENTITY_CLIENT_ID, System.getenv(ENV_AZURE_CLIENT_ID)));
            }
            default :
                throw new IllegalArgumentException("Unsupported auth type: " + authType);
        }
    }

    private BlobServiceAsyncClient getStaticBlobContainerAsyncClient(String accountName, String accountKey, String tenant, BlobServiceClientBuilder clientBuilder) {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
            .clientSecret(accountKey)
            .clientId(accountName)
            .tenantId(tenant)
            .build();
        return clientBuilder.credential(clientSecretCredential).buildAsyncClient();
    }

    private BlobServiceAsyncClient getInstanceProfileBlobContainerAsyncClient(BlobServiceClientBuilder clientBuilder, String managedIdentityClientId) {
        DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder().managedIdentityClientId(managedIdentityClientId).build();
        return clientBuilder.credential(defaultAzureCredential).buildAsyncClient();
    }

    private void checkDeleteObjectsError(Throwable ex) throws DeleteObjectsException {
        int errDeleteCount = 0;
        List<String> errorMessages = new ArrayList<>();
        List<String> failedKeys = new ArrayList<>();

        if (ex instanceof BlobBatchStorageException) {
            BlobBatchStorageException blobBatchEx = (BlobBatchStorageException) ex;
            for (BlobStorageException batchException : blobBatchEx.getBatchExceptions()) {
                if (batchException.getErrorCode() != BlobErrorCode.BLOB_NOT_FOUND) {
                    String failedKey = batchException.getResponse().getRequest().getUrl().getPath();
                    if (errDeleteCount < 5) {
                        LOGGER.error("Delete failed for key [{}], error: {}", failedKey, batchException.getMessage());
                    }
                    errDeleteCount++;
                    failedKeys.add(failedKey);
                    errorMessages.add(batchException.getMessage());
                }
            }
        }

        if (errDeleteCount > 0) {
            throw new DeleteObjectsException("Failed to delete " + errDeleteCount + " objects", failedKeys, errorMessages);
        }
    }

    @Override
    Pair<RetryStrategy, Throwable> toRetryStrategyAndCause(Throwable throwable, S3Operation s3Operation) {
        Throwable cause = FutureUtil.cause(throwable);
        RetryStrategy strategy = RetryStrategy.RETRY;
        if (cause instanceof BlobStorageException) {
            BlobStorageException blobStorageException = (BlobStorageException) cause;
            BlobErrorCode errorCode = blobStorageException.getErrorCode();
            if (errorCode == BLOB_NOT_FOUND ||
                errorCode == AUTHORIZATION_FAILURE) {
                strategy = RetryStrategy.ABORT;
            }
            if (COMPLETE_MULTI_PART_UPLOAD == s3Operation) {
                if (errorCode == BlobErrorCode.BLOB_ALREADY_EXISTS) {
                    strategy = RetryStrategy.VISIBILITY_CHECK;
                }
            }
            if (GET_OBJECT == s3Operation) {
                if (errorCode == BLOB_NOT_FOUND) {
                    cause = new ObjectNotExistException(cause);
                }
            }
        }
        if (COMPLETE_MULTI_PART_UPLOAD == s3Operation) {
            if (cause instanceof NoSuchUploadException) {
                strategy = RetryStrategy.VISIBILITY_CHECK;
            }
        }

        return Pair.of(strategy, cause);
    }

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        ListBlobsOptions options = new ListBlobsOptions()
            .setPrefix(prefix);
        return blobContainerReadAsyncClient
            .listBlobs(options)
            .collectList()
            .map(blobItems -> blobItems.stream()
                .map(blobItem -> new ObjectInfo(
                    bucketURI.bucketId(),
                    blobItem.getName(),
                    blobItem.getProperties().getLastModified().toInstant().toEpochMilli(),
                    blobItem.getProperties().getContentLength()))
                .collect(Collectors.toList()))
            .toFuture();
    }

    @Override
    public boolean readinessCheck() {
        LOGGER.info("Start readiness check for {}", bucketURI);
        String normalPath = String.format("__automq/readiness_check/normal_obj/%d", System.nanoTime());
        try {
            boolean exists = blobContainerReadAsyncClient.exists().toFuture().get();
            if (!exists) {
                LOGGER.error("Please check config is correct");
            }
        } catch (Throwable e) {
            Throwable cause = FutureUtil.cause(e);
            if (cause instanceof BlobStorageException) {
                BlobStorageException blobStorageException = (BlobStorageException) cause;
                BlobErrorCode errorCode = blobStorageException.getErrorCode();
                if (errorCode == AUTHORIZATION_FAILURE) {
                    LOGGER.error("Please check whether config is correct", blobStorageException);
                    return false;
                } else if (!(errorCode == BLOB_NOT_FOUND)) {
                    LOGGER.error("Please check config is correct", cause);
                }
            } else {
                LOGGER.error("Cannot connect to blob, please check the s3 endpoint config", cause);
            }
        }

        // check write
        try {
            byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
            doWrite(new WriteOptions(), normalPath, Unpooled.wrappedBuffer(content)).get();
        } catch (Throwable e) {
            Throwable cause = FutureUtil.cause(e);
            if (cause instanceof BlobStorageException) {
                BlobStorageException blobStorageException = (BlobStorageException) cause;
                if (blobStorageException.getErrorCode() == BlobErrorCode.CONTAINER_NOT_FOUND) {
                    LOGGER.error("Cannot find the bucket={}", bucketURI.bucket(), cause);
                } else {
                    LOGGER.error("Please check the identity have the permission to do Write Object operation", cause);
                }
            }
            return false;
        }

        // check delete
        try {
            doDeleteObjects(List.of(normalPath)).get();
        } catch (Throwable e) {
            LOGGER.error("Please check the identity have the permission to do Delete Object operation", FutureUtil.cause(e));
            return false;
        }

        // check multipart
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
                LOGGER.error("Read get mismatch content from multi-part upload object, expect {}, but {}", content, readContent);
            }
            doDeleteObjects(List.of(multiPartPath)).get();
        } catch (Throwable e) {
            LOGGER.error("Please check the identity have the permission to do MultiPart Object operation", FutureUtil.cause(e));
            return false;
        }

        LOGGER.info("Pass readiness check");
        return true;
    }

    public boolean write() {
        String normalPath = "__automq/readiness_check/multi_obj/override_test";
        try {
            byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
            doWrite(new WriteOptions(), normalPath, Unpooled.wrappedBuffer(content)).get();
        } catch (Throwable e) {
            Throwable cause = FutureUtil.cause(e);
            if (cause instanceof BlobStorageException) {
                BlobStorageException blobStorageException = (BlobStorageException) cause;
                if (blobStorageException.getErrorCode() == BlobErrorCode.CONTAINER_NOT_FOUND) {
                    LOGGER.error("Cannot find the bucket={}", bucketURI.bucket(), cause);
                } else {
                    LOGGER.error("Please check the identity have the permission to do Write Object operation", cause);
                }
            }
            return false;
        }
        return true;
    }

    public boolean multipartUpload() {
        // check multipart
        String multiPartPath = String.format("__automq/readiness_check/multi_obj/override_test", System.nanoTime());
        try {
            WriteOptions options = new WriteOptions();
            String uploadId = doCreateMultipartUpload(options, multiPartPath).get();

            int partSize = 1024 * 1024;
            byte[] fullContent = new byte[10 * 1024 * 1024];
            Arrays.fill(fullContent, (byte)'A');
            int totalParts = (fullContent.length + partSize - 1) / partSize;

            List<ObjectStorageCompletedPart> parts = new ArrayList<>();
            for (int partNumber = 1; partNumber <= totalParts; partNumber++) {
                int start = (partNumber - 1) * partSize;
                int end = Math.min(partNumber * partSize, fullContent.length);
                byte[] partContent = Arrays.copyOfRange(fullContent, start, end);

                ObjectStorageCompletedPart part = doUploadPart(
                    options,
                    multiPartPath,
                    uploadId,
                    partNumber,
                    Unpooled.wrappedBuffer(partContent)
                ).get();
                parts.add(part);
            }

            doCompleteMultipartUpload(options, multiPartPath, uploadId, parts).get();

            ByteBuf buf = doRangeRead(new ReadOptions(), multiPartPath, 0, -1L).get();
            byte[] readContent = new byte[buf.readableBytes()];
            buf.readBytes(readContent);
            buf.release();
            if (!Arrays.equals(fullContent, readContent)) {
                LOGGER.error("Read get mismatch content from multi-part upload object, expect {}, but {}",
                    new String(fullContent), new String(readContent));
            }

            for (int partNumber = 1; partNumber <= totalParts; partNumber++) {
                int start = (partNumber - 1) * partSize;
                long end = Math.min(partNumber * partSize, fullContent.length) - 1;
                buf = doRangeRead(new ReadOptions(), multiPartPath, start, end).get();
                byte[] partRead = new byte[buf.readableBytes()];
                buf.readBytes(partRead);
                buf.release();
                byte[] expectedPart = Arrays.copyOfRange(fullContent, start, (int)end);
                if (!Arrays.equals(partRead, expectedPart)) {
                    LOGGER.error("Part {} range read mismatch, expect {}, but {}",
                        partNumber, new String(expectedPart), new String(partRead));
                }
            }

//            doDeleteObjects(List.of(multiPartPath)).get();
            return true;
        } catch (Throwable e) {
            LOGGER.error("Multi-part upload verification failed", FutureUtil.cause(e));
            return false;
        }
    }

    @Override
    void doClose() {

    }

    private BlobBatchAsyncClient getBlobBatchAsyncClient(BlobServiceAsyncClient blobServiceAsyncClient) {
        return new BlobBatchClientBuilder(blobServiceAsyncClient).buildAsyncClient();
    }

    private BlobRange range(long start, long end) {
        if (end == -1L) {
            return new BlobRange(start);
        }
        return new BlobRange(start, end - start);
    }

    private String generateBlockId(int partNumber) {
        String blockIdRaw = String.format("%016d", partNumber);
        return Base64.getEncoder().encodeToString(blockIdRaw.getBytes(StandardCharsets.UTF_8));
    }

    public static class Builder {
        private BucketURI bucketURI;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter;
        private NetworkBandwidthLimiter outboundLimiter;
        private boolean readWriteIsolate;
        private boolean checkS3ApiMode;
        private String threadPrefix;

        public Builder bucketURI(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
            return this;
        }

        public Builder inboundLimiter(NetworkBandwidthLimiter networkInboundBandwidthLimiter) {
            this.inboundLimiter = networkInboundBandwidthLimiter;
            return this;
        }

        public Builder outboundLimiter(NetworkBandwidthLimiter networkOutboundBandwidthLimiter) {
            this.outboundLimiter = networkOutboundBandwidthLimiter;
            return this;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public Builder checkS3ApiMode(boolean checkS3ApiMode) {
            this.checkS3ApiMode = checkS3ApiMode;
            return this;
        }

        public Builder threadPrefix(String threadPrefix) {
            this.threadPrefix = threadPrefix;
            return this;
        }

        public AzureObjectStorage build() {
            return new AzureObjectStorage(bucketURI, tagging, inboundLimiter, outboundLimiter, readWriteIsolate, checkS3ApiMode, threadPrefix);
        }
    }

    private String refreshSasTokenSync() {
        if (sasToken == null || OffsetDateTime.now().plus(SAS_TOKEN_REFRESH_THRESHOLD).isAfter(sasTokenExpiryTime)) {
            synchronized (this) {
                if (sasToken == null || OffsetDateTime.now().plus(SAS_TOKEN_REFRESH_THRESHOLD).isAfter(sasTokenExpiryTime)) {
                    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
                    AccountSasPermission accountSasPermission = new AccountSasPermission().setReadPermission(true);
                    AccountSasService services = new AccountSasService().setBlobAccess(true);
                    AccountSasResourceType resourceTypes = new AccountSasResourceType().setObject(true);
                    AccountSasSignatureValues accountSasValues =
                        new AccountSasSignatureValues(expiryTime, accountSasPermission, services, resourceTypes);
                    sasToken = blobServiceAsyncClient.generateAccountSas(accountSasValues);
                    sasTokenExpiryTime = expiryTime;
                }
            }
        }
        return sasToken;
    }
}
