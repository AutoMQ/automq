package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metrics.operations.S3Operation;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class AwsObjectStorageTest {

    /**
     * The default CRT client keeps PUTs single-request while AutoMQ retains multipart uploads and precomputed checksums.
     */
    @Tag("S3Unit")
    @Test
    void testDefaultCrtDisablesAutomaticMultipart() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket", ChecksumAlgorithm.CRC32);
        var builder = mock(software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder.class, org.mockito.Mockito.RETURNS_SELF);
        when(builder.build()).thenReturn(mock(S3AsyncClient.class));
        try (var factory = org.mockito.Mockito.mockStatic(S3AsyncClient.class)) {
            factory.when(S3AsyncClient::crtBuilder).thenReturn(builder);
            BucketURI uri = BucketURI.parse("0@s3://bucket?region=us-east-1");
            try (S3AsyncClient ignored = storage.newS3Client(uri, List.of(), AwsObjectStorage.Configuration.fromBucketURI(uri))) {
                org.mockito.Mockito.verify(builder).thresholdInBytes(Long.MAX_VALUE);
            }
        } finally {
            storage.close();
        }
    }

    /**
     * Each URI supplies its own SDK and tuning options; configuring one bucket leaves another bucket's defaults intact.
     */
    @Tag("S3Unit")
    @Test
    void testPerBucketClientOptions() {
        BucketURI uri = BucketURI.parse("0@s3://one?region=us-east-1&sdkImpl=crt&connectionTimeoutMs=1500"
            + "&numRetries=0&maxConcurrency=12&crtTargetThroughputInGbps=2.5&crtMinimumThroughputInBps=1048576"
            + "&crtMinimumThroughputTimeoutSeconds=3&crtMaxNativeMemoryLimitInBytes=1073741824");
        AwsObjectStorage.Configuration options = AwsObjectStorage.Configuration.fromBucketURI(uri);
        Assertions.assertEquals("crt", options.sdkImplementation());
        Assertions.assertEquals(java.time.Duration.ofMillis(1500), options.connectionTimeout());
        Assertions.assertEquals(0, options.numRetries());
        Assertions.assertEquals(12, options.maxConcurrency());
        Assertions.assertEquals(2.5, options.crtTargetThroughputGbps());
        Assertions.assertEquals(1048576L, options.crtMinimumThroughputBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(3), options.crtMinimumThroughputTimeout());
        Assertions.assertEquals(1073741824L, options.crtMaxNativeMemoryBytes());
        Assertions.assertEquals(AwsObjectStorage.Configuration.builder().build(),
            AwsObjectStorage.Configuration.fromBucketURI(BucketURI.parse("1@s3://two?region=us-east-1")));
    }

    /**
     * Netty consumes the shared retry setting, preserves API deadlines, and ignores CRT-only settings.
     */
    @Tag("S3Unit")
    @Test
    void testNettySharedRetryOptions() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        try {
            storage.bucketURI.addExtension("sdkImpl", "netty");
            storage.bucketURI.addExtension("numRetries", "0");
            storage.bucketURI.addExtension("connectionTimeoutMs", "1500");
            storage.bucketURI.addExtension("maxConcurrency", "12");
            storage.bucketURI.addExtension("crtTargetThroughputInGbps", "ignored");
            var configuration = AwsObjectStorage.Configuration.fromBucketURI(storage.bucketURI);
            var overrides = storage.clientOverrideConfiguration(configuration);
            Assertions.assertEquals(1, overrides.retryStrategy().orElseThrow().maxAttempts());
            Assertions.assertEquals(java.time.Duration.ofMillis(30000), overrides.apiCallTimeout().orElseThrow());
            Assertions.assertEquals(java.time.Duration.ofMillis(10000), overrides.apiCallAttemptTimeout().orElseThrow());
            try (S3AsyncClient client = storage.newS3Client(
                BucketURI.parse("0@s3://bucket?region=us-east-1&endpoint=http://localhost:9000&pathStyle=true"),
                List.of(() -> AwsBasicCredentials.create("test", "test")), configuration)) {
                Assertions.assertEquals("s3", client.serviceName());
            }
        } finally {
            storage.close();
        }
    }

    /**
     * Unsupported SDK names and CRT health windows below two seconds fail during URI option parsing.
     */
    @Tag("S3Unit")
    @Test
    void testBucketClientOptionValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsObjectStorage.Configuration.fromBucketURI(
            BucketURI.parse("0@s3://one?sdkImpl=unknown")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsObjectStorage.Configuration.fromBucketURI(
            BucketURI.parse("0@s3://one?sdkImpl=crt&crtMinimumThroughputTimeoutSeconds=1")));
    }

    /**
     * Both SDK implementations build with endpoint and path-style overrides using the existing runtime dependencies.
     */
    @Tag("S3Unit")
    @Test
    void testSdkClientSelection() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        try {
            for (String implementation : List.of("netty", "crt")) {
                storage.bucketURI.addExtension("sdkImpl", implementation);
                try (S3AsyncClient client = storage.newS3Client(
                    BucketURI.parse("0@s3://bucket?region=us-east-1&endpoint=http://localhost:9000&pathStyle=true"),
                    List.of(() -> AwsBasicCredentials.create("test", "test")),
                    AwsObjectStorage.Configuration.fromBucketURI(storage.bucketURI))) {
                    Assertions.assertEquals("s3", client.serviceName());
                }
            }
        } finally {
            storage.bucketURI.addExtension("sdkImpl", "netty");
            storage.close();
        }
    }

    /**
     * Builder mutations preserve previously built snapshots and unmodified fields retain their defaults.
     */
    @Tag("S3Unit")
    @Test
    void testConfigurationSnapshot() {
        AwsObjectStorage.Configuration.Builder builder = AwsObjectStorage.Configuration.builder();
        AwsObjectStorage.Configuration defaults = builder.build();
        AwsObjectStorage.Configuration custom = builder.sdkImplementation("netty")
            .crtMinimumThroughputBps(1024).crtMinimumThroughputTimeout(java.time.Duration.ofSeconds(3))
            .crtTargetThroughputGbps(2.5).build();
        Assertions.assertEquals("crt", defaults.sdkImplementation());
        Assertions.assertEquals(32L * 1024 * 1024, defaults.crtMinimumThroughputBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(2), defaults.crtMinimumThroughputTimeout());
        Assertions.assertEquals("netty", custom.sdkImplementation());
        Assertions.assertEquals(10.0, defaults.crtTargetThroughputGbps());
        Assertions.assertEquals(2.5, custom.crtTargetThroughputGbps());
        Assertions.assertEquals(1024, custom.crtMinimumThroughputBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(3), custom.crtMinimumThroughputTimeout());
        Assertions.assertEquals(-1, defaults.maxConcurrency());
        Assertions.assertEquals(-1L, defaults.crtMaxNativeMemoryBytes());
        Assertions.assertEquals(java.time.Duration.ofSeconds(2), defaults.connectionTimeout());
        Assertions.assertEquals(2, defaults.numRetries());
    }

    /**
     * Automatic concurrency inherits the existing cap, while native memory stays unset until explicitly configured.
     */
    @Tag("S3Unit")
    @Test
    void testCrtBuilderConfiguration() {
        software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder builder = mock(
            software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder.class, org.mockito.Mockito.RETURNS_SELF);
        org.mockito.Mockito.doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration.Builder> configure = invocation.getArgument(0);
            var http = software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration.builder();
            configure.accept(http);
            var result = http.build();
            Assertions.assertEquals(java.time.Duration.ofSeconds(2), result.connectionTimeout());
            Assertions.assertEquals(32L * 1024 * 1024, result.healthConfiguration().minimumThroughputInBps());
            Assertions.assertEquals(java.time.Duration.ofSeconds(2), result.healthConfiguration().minimumThroughputTimeout());
            return builder;
        }).when(builder).httpConfiguration(org.mockito.ArgumentMatchers
            .<Consumer<software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration.Builder>>any());
        org.mockito.Mockito.doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration.Builder> configure = invocation.getArgument(0);
            var retry = software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration.builder();
            configure.accept(retry);
            Assertions.assertEquals(2, retry.build().numRetries());
            return builder;
        }).when(builder).retryConfiguration(org.mockito.ArgumentMatchers
            .<Consumer<software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration.Builder>>any());
        AwsObjectStorage.Configuration.builder().build().configureCrtBuilder(builder, 77);
        org.mockito.Mockito.verify(builder).maxConcurrency(77);
        org.mockito.Mockito.verify(builder).thresholdInBytes(Long.MAX_VALUE);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.never()).maxNativeMemoryLimitInBytes(any());
        AwsObjectStorage.Configuration.builder().maxConcurrency(12).crtMaxNativeMemoryBytes(1024L * 1024 * 1024)
            .build().configureCrtBuilder(builder, 77);
        org.mockito.Mockito.verify(builder).maxConcurrency(12);
        org.mockito.Mockito.verify(builder).maxNativeMemoryLimitInBytes(1024L * 1024 * 1024);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.times(2)).thresholdInBytes(Long.MAX_VALUE);
    }

    /**
     * Given CopyObject permission is missing, when readiness is checked, then the check fails and still deletes both
     * probe paths so repeated checks do not leak objects.
     */
    @Test
    void testReadinessCheckFailsAndCleansUpWhenCopyFails() {
        S3AsyncClient s3 = mock(S3AsyncClient.class);
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
        AwsObjectStorage storage = spy(new AwsObjectStorage(s3, "bucket"));
        doReturn(CompletableFuture.completedFuture(null)).when(storage)
            .doWrite(any(), anyString(), any(ByteBuf.class));
        doReturn(CompletableFuture.failedFuture(new RuntimeException("copy denied"))).when(storage)
            .doCopy(eq("bucket"), anyString(), anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(storage).doDeleteObjects(anyList());

        Assertions.assertFalse(storage.readinessCheck());

        verify(storage).doCopy(eq("bucket"),
            argThat(path -> path.startsWith("__automq/readiness_check/normal_obj/")),
            argThat(path -> path.startsWith("__automq/readiness_check/copy_obj/")));
        verify(storage).doDeleteObjects(argThat(paths -> paths.size() == 2
            && paths.stream().anyMatch(path -> path.startsWith("__automq/readiness_check/normal_obj/"))
            && paths.stream().anyMatch(path -> path.startsWith("__automq/readiness_check/copy_obj/"))));
    }

    /**
     * Given LIST and COPY failures, when the storage layer classifies them, then only transport and transient service
     * failures are retried.
     */
    @Test
    void testListAndCopyRetryabilityClassification() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");

        for (S3Operation operation : List.of(S3Operation.LIST_OBJECTS, S3Operation.COPY_OBJECT)) {
            Assertions.assertEquals(RetryStrategy.RETRY, storage.toRetryStrategyAndCause(
                software.amazon.awssdk.core.exception.SdkClientException.create("connection reset"), operation)
                .getLeft());
            Assertions.assertEquals(RetryStrategy.RETRY,
                storage.toRetryStrategyAndCause(S3Exception.builder().statusCode(503).build(), operation).getLeft());
            Assertions.assertEquals(RetryStrategy.ABORT,
                storage.toRetryStrategyAndCause(S3Exception.builder().statusCode(403).build(), operation).getLeft());
            Assertions.assertEquals(RetryStrategy.ABORT,
                storage.toRetryStrategyAndCause(new IllegalArgumentException("invalid request"), operation).getLeft());
        }
    }

    /**
     * Given a bounded LIST spanning provider pages, when listing through ObjectStorage, then the adapter returns one
     * ordered result and uses the remaining caller limit on each internal page.
     */
    @Test
    void testDoListCollectsMultiplePagesWithinBound() throws Exception {
        S3AsyncClient s3 = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(s3, "bucket");
        List<ListObjectsV2Request> requests = new ArrayList<>();

        when(s3.listObjectsV2(org.mockito.ArgumentMatchers
            .<Consumer<ListObjectsV2Request.Builder>>any())).thenAnswer(invocation -> {
                ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
                Consumer<ListObjectsV2Request.Builder> requestBuilder = invocation.getArgument(0);
                requestBuilder.accept(builder);
                ListObjectsV2Request request = builder.build();
                requests.add(request);
                if (request.continuationToken() == null) {
                    return CompletableFuture.completedFuture(ListObjectsV2Response.builder()
                        .contents(object("archive/1/001"), object("archive/1/003"))
                        .isTruncated(true)
                        .nextContinuationToken("page-2")
                        .build());
                }
                return CompletableFuture.completedFuture(ListObjectsV2Response.builder()
                    .contents(object("archive/1/005"))
                    .isTruncated(true)
                    .nextContinuationToken("page-3")
                    .build());
            });

        List<String> keys = storage.list(new ObjectStorage.ListOptions("archive/1/")
                .startAfter("archive/1/000")
                .maxKeys(3))
            .get().stream().map(ObjectStorage.ObjectPath::key).toList();

        Assertions.assertEquals(List.of("archive/1/001", "archive/1/003", "archive/1/005"), keys);
        Assertions.assertEquals(2, requests.size());
        Assertions.assertEquals("archive/1/", requests.get(0).prefix());
        Assertions.assertEquals("archive/1/000", requests.get(0).startAfter());
        Assertions.assertEquals(3, requests.get(0).maxKeys());
        Assertions.assertNull(requests.get(1).startAfter());
        Assertions.assertEquals("page-2", requests.get(1).continuationToken());
        Assertions.assertEquals(1, requests.get(1).maxKeys());
    }

    @Test
    void testDoWriteSetsContentMd5() throws Exception {
        S3AsyncClient s3 = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(s3, "bucket");
        ByteBuf data = TestUtils.random(128);
        List<PutObjectRequest> requests = new java.util.ArrayList<>();

        when(s3.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        try {
            storage.doWrite(ObjectStorage.WriteOptions.DEFAULT, "test-key", data).get();

            Assertions.assertEquals(1, requests.size());
            Assertions.assertEquals(md5Base64(data), requests.get(0).contentMD5());
        } finally {
            data.release();
        }
    }

    /**
     * Given a source in another data bucket, when Archive renames it into the first bucket, then CopyObject carries
     * both bucket identities and both keys without transferring payload through the Broker.
     */
    @Test
    void testDoCopySetsCrossBucketSourceAndRenamedDestination() throws Exception {
        S3AsyncClient s3 = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(s3, "archive-bucket");
        List<CopyObjectRequest> requests = new ArrayList<>();
        when(s3.copyObject(any(CopyObjectRequest.class))).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(CopyObjectResponse.builder().build());
        });

        storage.doCopy("source-bucket", "data/source-key", "archive/renamed-key").get();

        Assertions.assertEquals(1, requests.size());
        Assertions.assertEquals("source-bucket", requests.get(0).sourceBucket());
        Assertions.assertEquals("data/source-key", requests.get(0).sourceKey());
        Assertions.assertEquals("archive-bucket", requests.get(0).destinationBucket());
        Assertions.assertEquals("archive/renamed-key", requests.get(0).destinationKey());
    }

    @Test
    void testDoWriteSetsPrecomputedChecksumWhenChecksumAlgorithmIsConfigured() throws Exception {
        S3AsyncClient s3 = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(s3, "bucket", ChecksumAlgorithm.CRC32_C);
        ByteBuf data = Unpooled.wrappedBuffer("hello checksum".getBytes(StandardCharsets.UTF_8));
        List<PutObjectRequest> requests = new java.util.ArrayList<>();

        when(s3.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        try {
            storage.doWrite(ObjectStorage.WriteOptions.DEFAULT, "test-key", data).get();

            Assertions.assertEquals(1, requests.size());
            Assertions.assertNull(requests.get(0).contentMD5());
            Assertions.assertNull(requests.get(0).checksumAlgorithm());
            Assertions.assertEquals(crc32cBase64(data), requests.get(0).checksumCRC32C());
        } finally {
            data.release();
        }
    }

    @Test
    void testCredentialsProviderChain() {
        AwsObjectStorage storage = mock(AwsObjectStorage.class);
        doCallRealMethod().when(storage).credentialsProviders0(any());
        doCallRealMethod().when(storage).newCredentialsProviderChain(any());

        AwsCredentialsProvider provider = storage.newCredentialsProviderChain(storage.credentialsProviders0(
            BucketURI.parse("0@s3://bucket?region=us-east-1&accessKey=ak&secretKey=sk")));
        AwsCredentials credentials = provider.resolveCredentials();
        Assertions.assertInstanceOf(AwsBasicCredentials.class, credentials);
        AwsBasicCredentials basicCredentials = (AwsBasicCredentials) credentials;
        Assertions.assertEquals("ak", basicCredentials.accessKeyId());
        Assertions.assertEquals("sk", basicCredentials.secretAccessKey());

        // test fallback to system property credential provider

        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "ak");
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "sk");
        provider = storage.newCredentialsProviderChain(storage.credentialsProviders0(
            BucketURI.parse("0@s3://bucket?region=us-east-1&accessKey=&secretKey=")));
        credentials = provider.resolveCredentials();
        Assertions.assertInstanceOf(AwsBasicCredentials.class, credentials);
        basicCredentials = (AwsBasicCredentials) credentials;
        Assertions.assertEquals("ak", basicCredentials.accessKeyId());
        Assertions.assertEquals("sk", basicCredentials.secretAccessKey());
    }

    @Test
    void testCheckDeleteObjectsResponseClassifiesPerKeyErrors() {
        AwsObjectStorage storage = new AwsObjectStorage(null, "bucket");
        DeleteObjectsResponse response = DeleteObjectsResponse.builder()
            .errors(
                S3Error.builder().key("missing").code("NoSuchKey").message("missing").build(),
                S3Error.builder().key("retry").code("SlowDown").message("slow down").build(),
                S3Error.builder().key("fail").code("AccessDenied").message("denied").build())
            .build();

        DeleteObjectsException ex = Assertions.assertThrows(
            DeleteObjectsException.class,
            () -> storage.checkDeleteObjectsResponse(response, List.of("ok", "missing", "retry", "fail")));

        Assertions.assertEquals(Set.of("ok", "missing"), ex.getSuccessKeys());
        Assertions.assertTrue(ex.getRetriableKeys().containsKey("retry"));
        Assertions.assertTrue(ex.getFailedKeyErrors().containsKey("fail"));
    }

    private static String md5Base64(ByteBuf data) throws Exception {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        for (ByteBuffer buffer : data.nioBuffers()) {
            md5.update(buffer.duplicate());
        }
        return Base64.getEncoder().encodeToString(md5.digest());
    }

    private static S3Object object(String key) {
        return S3Object.builder().key(key).lastModified(Instant.EPOCH).size(1L).build();
    }

    private static String crc32cBase64(ByteBuf data) {
        CRC32C crc32c = new CRC32C();
        for (ByteBuffer buffer : data.nioBuffers()) {
            crc32c.update(buffer.duplicate());
        }
        ByteBuffer value = ByteBuffer.allocate(Integer.BYTES);
        value.putInt((int) crc32c.getValue());
        return Base64.getEncoder().encodeToString(value.array());
    }


    /**
     * Given a NoSuchUploadException during UPLOAD_PART,
     * When toRetryStrategyAndCause is called,
     * Then the strategy should be ABORT because retrying with a dead upload ID is futile.
     */
    @Test
    void testNoSuchUploadExceptionAbortsUploadPart() {
        AwsObjectStorage storage = new AwsObjectStorage(null, "bucket");
        NoSuchUploadException cause = NoSuchUploadException.builder()
            .message("The specified upload does not exist")
            .statusCode(404)
            .build();

        Pair<RetryStrategy, Throwable> result = storage.toRetryStrategyAndCause(cause, S3Operation.UPLOAD_PART);
        Assertions.assertEquals(RetryStrategy.ABORT, result.getLeft(),
            "UPLOAD_PART with NoSuchUploadException should ABORT, not retry indefinitely");
    }

    /**
     * Given a NoSuchUploadException during UPLOAD_PART_COPY,
     * When toRetryStrategyAndCause is called,
     * Then the strategy should be ABORT for the same reason as UPLOAD_PART.
     */
    @Test
    void testNoSuchUploadExceptionAbortsUploadPartCopy() {
        AwsObjectStorage storage = new AwsObjectStorage(null, "bucket");
        NoSuchUploadException cause = NoSuchUploadException.builder()
            .message("The specified upload does not exist")
            .statusCode(404)
            .build();

        Pair<RetryStrategy, Throwable> result = storage.toRetryStrategyAndCause(cause, S3Operation.UPLOAD_PART_COPY);
        Assertions.assertEquals(RetryStrategy.ABORT, result.getLeft(),
            "UPLOAD_PART_COPY with NoSuchUploadException should ABORT, not retry indefinitely");
    }

    /**
     * Given a NoSuchUploadException during COMPLETE_MULTI_PART_UPLOAD,
     * When toRetryStrategyAndCause is called,
     * Then the strategy should be VISIBILITY_CHECK because the upload may have already completed
     * and the object may just not be visible yet.
     */
    @Test
    void testNoSuchUploadExceptionVisibilityCheckForComplete() {
        AwsObjectStorage storage = new AwsObjectStorage(null, "bucket");
        NoSuchUploadException cause = NoSuchUploadException.builder()
            .message("The specified upload does not exist")
            .statusCode(404)
            .build();

        Pair<RetryStrategy, Throwable> result = storage.toRetryStrategyAndCause(cause, S3Operation.COMPLETE_MULTI_PART_UPLOAD);
        Assertions.assertEquals(RetryStrategy.VISIBILITY_CHECK, result.getLeft(),
            "COMPLETE_MULTI_PART_UPLOAD with NoSuchUploadException should VISIBILITY_CHECK");
    }

}
