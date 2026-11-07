package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
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
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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
     * An unspecified implementation selects CRT HTTP and the public MD5 compatibility plugin on the Java S3 client.
     */
    @Test
    void testDefaultUsesCrtHttp() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        var javaBuilder = mock(software.amazon.awssdk.services.s3.S3AsyncClientBuilder.class,
            org.mockito.Mockito.RETURNS_SELF);
        var httpBuilder = mock(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient.Builder.class,
            org.mockito.Mockito.RETURNS_SELF);
        when(javaBuilder.build()).thenReturn(mock(S3AsyncClient.class));
        try (var s3Factory = org.mockito.Mockito.mockStatic(S3AsyncClient.class);
             var httpFactory = org.mockito.Mockito.mockStatic(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient.class)) {
            s3Factory.when(S3AsyncClient::builder).thenReturn(javaBuilder);
            httpFactory.when(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient::builder).thenReturn(httpBuilder);
            BucketURI uri = BucketURI.parse("0@s3://bucket?region=us-east-1");
            try (S3AsyncClient ignored = storage.newS3Client(uri, List.of(), AwsObjectStorage.Configuration.fromBucketURI(uri))) {
                verify(javaBuilder).httpClientBuilder(httpBuilder);
                verify(javaBuilder).addPlugin(any(software.amazon.awssdk.services.s3.LegacyMd5Plugin.class));
                verify(javaBuilder).requestChecksumCalculation(software.amazon.awssdk.core.checksums.RequestChecksumCalculation.WHEN_REQUIRED);
                verify(javaBuilder).responseChecksumValidation(software.amazon.awssdk.core.checksums.ResponseChecksumValidation.WHEN_REQUIRED);
            }
        } finally {
            storage.close();
        }
    }

    /**
     * The native CRT client keeps PUTs single-request while AutoMQ retains multipart uploads and precomputed checksums.
     */
    @Tag("S3Unit")
    @Test
    void testNativeCrtDisablesAutomaticMultipart() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket", ChecksumAlgorithm.CRC32);
        var builder = mock(software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder.class, org.mockito.Mockito.RETURNS_SELF);
        when(builder.build()).thenReturn(mock(S3AsyncClient.class));
        try (var factory = org.mockito.Mockito.mockStatic(S3AsyncClient.class)) {
            factory.when(S3AsyncClient::crtBuilder).thenReturn(builder);
            BucketURI uri = BucketURI.parse("0@s3://bucket?region=us-east-1&sdkImpl=crt");
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
        AwsObjectStorage.Configuration controlOptions = options.forControl();
        Assertions.assertEquals(1L, controlOptions.crtMinimumThroughputBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(30), controlOptions.crtMinimumThroughputTimeout());
        Assertions.assertEquals(options.sdkImplementation(), controlOptions.sdkImplementation());
        Assertions.assertEquals(options.crtTargetThroughputGbps(), controlOptions.crtTargetThroughputGbps());
        Assertions.assertEquals(options.crtMaxNativeMemoryBytes(), controlOptions.crtMaxNativeMemoryBytes());
        AwsObjectStorage.Configuration moreLenientOptions = AwsObjectStorage.Configuration.builder()
            .crtMinimumThroughputBps(1).crtMinimumThroughputTimeout(java.time.Duration.ofSeconds(60)).build();
        Assertions.assertEquals(1L, moreLenientOptions.forControl().crtMinimumThroughputBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(60), moreLenientOptions.forControl().crtMinimumThroughputTimeout());
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
        Assertions.assertThrows(IllegalArgumentException.class, () -> AwsObjectStorage.Configuration.fromBucketURI(
            BucketURI.parse("0@s3://one?sdkImpl=crt-http&crtMinimumThroughputTimeoutSeconds=1")));
    }

    /**
     * All three SDK implementations build with endpoint and path-style overrides using the existing runtime dependencies.
     */
    @Tag("S3Unit")
    @Test
    void testSdkClientSelection() {
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        try {
            for (String implementation : List.of("crt-http", "netty", "crt")) {
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
        Assertions.assertEquals("crt-http", defaults.sdkImplementation());
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
        AtomicReference<software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration> capturedHttp = new AtomicReference<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration.Builder> configure = invocation.getArgument(0);
            var http = software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration.builder();
            configure.accept(http);
            capturedHttp.set(http.build());
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
        Assertions.assertEquals(java.time.Duration.ofSeconds(2), capturedHttp.get().connectionTimeout());
        Assertions.assertEquals(32L * 1024 * 1024, capturedHttp.get().healthConfiguration().minimumThroughputInBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(2), capturedHttp.get().healthConfiguration().minimumThroughputTimeout());
        org.mockito.Mockito.verify(builder).maxConcurrency(77);
        org.mockito.Mockito.verify(builder).thresholdInBytes(Long.MAX_VALUE);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.never()).maxNativeMemoryLimitInBytes(any());
        AwsObjectStorage.Configuration.builder().maxConcurrency(12).crtMaxNativeMemoryBytes(1024L * 1024 * 1024)
            .build().configureCrtBuilder(builder, 77);
        org.mockito.Mockito.verify(builder).maxConcurrency(12);
        org.mockito.Mockito.verify(builder).maxNativeMemoryLimitInBytes(1024L * 1024 * 1024);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.times(2)).thresholdInBytes(Long.MAX_VALUE);
        AwsObjectStorage.Configuration.builder().build().forControl().configureCrtBuilder(builder, 77);
        Assertions.assertEquals(1L, capturedHttp.get().healthConfiguration().minimumThroughputInBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(30), capturedHttp.get().healthConfiguration().minimumThroughputTimeout());
    }

    /**
     * CRT HTTP shares native CRT connection health defaults and overrides, while ignoring native transfer options.
     */
    @Test
    void testCrtHttpBuilderConfiguration() {
        var builder = mock(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient.Builder.class,
            org.mockito.Mockito.RETURNS_SELF);
        AtomicReference<software.amazon.awssdk.http.crt.ConnectionHealthConfiguration> capturedHealth = new AtomicReference<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.http.crt.ConnectionHealthConfiguration.Builder> configure = invocation.getArgument(0);
            var health = software.amazon.awssdk.http.crt.ConnectionHealthConfiguration.builder();
            configure.accept(health);
            capturedHealth.set(health.build());
            return builder;
        }).when(builder).connectionHealthConfiguration(org.mockito.ArgumentMatchers
            .<Consumer<software.amazon.awssdk.http.crt.ConnectionHealthConfiguration.Builder>>any());
        var defaults = AwsObjectStorage.Configuration.fromBucketURI(BucketURI.parse(
            "0@s3://bucket?crtTargetThroughputInGbps=ignored&crtMaxNativeMemoryLimitInBytes=ignored"));
        defaults.configureCrtHttpBuilder(builder, 77);
        Assertions.assertEquals(32L * 1024 * 1024, capturedHealth.get().minimumThroughputInBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(2), capturedHealth.get().minimumThroughputTimeout());
        verify(builder).maxConcurrency(77);
        verify(builder).connectionTimeout(java.time.Duration.ofSeconds(2));
        var custom = AwsObjectStorage.Configuration.fromBucketURI(BucketURI.parse(
            "0@s3://bucket?maxConcurrency=12&connectionTimeoutMs=1500&numRetries=0&apiCallTimeoutMs=5000&apiCallAttemptTimeoutMs=2000"
                + "&crtMinimumThroughputInBps=1048576&crtMinimumThroughputTimeoutSeconds=3"));
        custom.configureCrtHttpBuilder(builder, 77);
        Assertions.assertEquals(1048576L, capturedHealth.get().minimumThroughputInBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(3), capturedHealth.get().minimumThroughputTimeout());
        custom.forControl().configureCrtHttpBuilder(builder, 77);
        Assertions.assertEquals(1L, capturedHealth.get().minimumThroughputInBps());
        Assertions.assertEquals(java.time.Duration.ofSeconds(30), capturedHealth.get().minimumThroughputTimeout());
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.times(2)).maxConcurrency(12);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.times(2))
            .connectionTimeout(java.time.Duration.ofMillis(1500));
        AwsObjectStorage storage = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        try {
            var overrides = storage.clientOverrideConfiguration(custom);
            Assertions.assertEquals(1, overrides.retryStrategy().orElseThrow().maxAttempts());
            Assertions.assertEquals(java.time.Duration.ofSeconds(5), overrides.apiCallTimeout().orElseThrow());
            Assertions.assertEquals(java.time.Duration.ofSeconds(2), overrides.apiCallAttemptTimeout().orElseThrow());
        } finally {
            storage.close();
        }
    }

    /**
     * Java clients retain Content-MD5 over the actual escaped DeleteObjects XML, including the default CRT HTTP transport.
     */
    @Test
    void testJavaClientsSendBatchDeleteMd5() throws Exception {
        for (String implementation : List.of("crt-http", "netty")) {
            AtomicReference<byte[]> requestBody = new AtomicReference<>();
            AtomicReference<String> requestMd5 = new AtomicReference<>();
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                requestBody.set(exchange.getRequestBody().readAllBytes());
                requestMd5.set(exchange.getRequestHeaders().getFirst("Content-MD5"));
                byte[] response = ("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    + "<Deleted><Key>key&amp;&lt;tag&gt;</Key></Deleted></DeleteResult>")
                    .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                try (var output = exchange.getResponseBody()) {
                    output.write(response);
                } finally {
                    exchange.close();
                }
            });
            server.start();
            AwsObjectStorage factory = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
            try {
                BucketURI uri = BucketURI.parse("0@s3://bucket?region=us-east-1&pathStyle=true&endpoint=http://127.0.0.1:"
                    + server.getAddress().getPort() + "&sdkImpl=" + implementation);
                S3AsyncClient client = factory.newS3Client(uri, List.of(() -> AwsBasicCredentials.create("test", "test")),
                    AwsObjectStorage.Configuration.fromBucketURI(uri));
                AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
                try {
                    storage.doDeleteObjects(List.of("key&<tag>")).get(10, TimeUnit.SECONDS);
                    Assertions.assertNotNull(requestBody.get());
                    Assertions.assertEquals(Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5")
                        .digest(requestBody.get())), requestMd5.get());
                } finally {
                    storage.close();
                }
            } finally {
                factory.close();
                server.stop(0);
            }
        }
    }

    /**
     * A normal connection close after a short body must fail consumption instead of leaving the read future pending.
     */
    @Test
    void testCrtHttpFailsTruncatedBody() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().set("Content-Range", "bytes 0-65535/65536");
            exchange.sendResponseHeaders(206, 65536);
            try (var output = exchange.getResponseBody()) {
                output.write(new byte[1024]);
            } finally {
                exchange.close();
            }
        });
        server.start();
        AwsObjectStorage factory = new AwsObjectStorage(mock(S3AsyncClient.class), "bucket");
        try {
            BucketURI uri = BucketURI.parse("0@s3://bucket?region=us-east-1&pathStyle=true&numRetries=0&endpoint=http://127.0.0.1:"
                + server.getAddress().getPort());
            S3AsyncClient client = factory.newS3Client(uri, List.of(() -> AwsBasicCredentials.create("test", "test")),
                AwsObjectStorage.Configuration.fromBucketURI(uri));
            AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
            try {
                var exception = Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> storage.doRangeRead(new ObjectStorage.ReadOptions(), "key", 0, 65536).get(10, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(java.io.IOException.class, exception.getCause());
            } finally {
                storage.close();
            }
        } finally {
            factory.close();
            server.stop(0);
        }
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
        S3AsyncClient dataClient = mock(S3AsyncClient.class);
        S3AsyncClient controlClient = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(dataClient, controlClient, "bucket",
            ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION);
        List<ListObjectsV2Request> requests = new ArrayList<>();

        when(controlClient.listObjectsV2(org.mockito.ArgumentMatchers
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
        org.mockito.Mockito.verify(dataClient, org.mockito.Mockito.never()).listObjectsV2(
            org.mockito.ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any());
        storage.close();
        verify(controlClient).close();
        verify(dataClient).close();
    }

    /**
     * Given CRT HTTP, low-throughput requests share one lazily created client while UploadPart uses the write client.
     */
    @Test
    void testCrtControlClientRoutesLowThroughputOperations() throws Exception {
        S3AsyncClient dataClient = mock(S3AsyncClient.class);
        S3AsyncClient controlClient = mock(S3AsyncClient.class);
        S3AsyncClientBuilder s3Builder = mock(S3AsyncClientBuilder.class, org.mockito.Mockito.RETURNS_SELF);
        var httpBuilder = mock(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient.Builder.class,
            org.mockito.Mockito.RETURNS_SELF);
        when(s3Builder.build()).thenReturn(dataClient, controlClient);
        when(controlClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder().uploadId("upload").build()));
        when(controlClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CompleteMultipartUploadResponse.builder().build()));
        when(controlClient.uploadPartCopy(any(UploadPartCopyRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(UploadPartCopyResponse.builder()
                .copyPartResult(CopyPartResult.builder().eTag("copied").build()).build()));
        when(controlClient.deleteObjects(any(DeleteObjectsRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build()));
        when(controlClient.listObjectsV2(org.mockito.ArgumentMatchers
            .<Consumer<ListObjectsV2Request.Builder>>any()))
            .thenReturn(CompletableFuture.completedFuture(ListObjectsV2Response.builder().isTruncated(false).build()));
        when(dataClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(UploadPartResponse.builder().eTag("part").build()));

        try (var s3Factory = org.mockito.Mockito.mockStatic(S3AsyncClient.class);
             var httpFactory = org.mockito.Mockito.mockStatic(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient.class)) {
            s3Factory.when(S3AsyncClient::builder).thenReturn(s3Builder);
            httpFactory.when(software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient::builder).thenReturn(httpBuilder);
            AwsObjectStorage storage = new AwsObjectStorage(BucketURI.parse("0@s3://bucket?region=us-east-1"),
                Map.of(), NetworkBandwidthLimiter.NOOP, NetworkBandwidthLimiter.NOOP, false, false, "test");
            try {
                verify(s3Builder).build();
                ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
                    .apiCallAttemptTimeout(TimeUnit.MINUTES.toMillis(2));
                String uploadId = storage.doCreateMultipartUpload(options, "key").get();
                Assertions.assertEquals("upload", uploadId);
                storage.doUploadPartCopy(options, "source", "key", 0, 1, uploadId, 1).get();
                storage.doCompleteMultipartUpload(options, "key", uploadId,
                    List.of(new ObjectStorage.ObjectStorageCompletedPart(1, "etag", null))).get();
                storage.doDeleteObjects(List.of("key")).get();
                storage.doList(new ObjectStorage.ListOptions("prefix")).get();
                ByteBuf part = Unpooled.wrappedBuffer(new byte[] {1});
                try {
                    storage.doUploadPart(options, "key", uploadId, 1, part).get();
                } finally {
                    part.release();
                }
                org.mockito.Mockito.verify(s3Builder, org.mockito.Mockito.times(2)).build();
                verify(controlClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
                verify(controlClient).uploadPartCopy(any(UploadPartCopyRequest.class));
                verify(controlClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
                verify(controlClient).deleteObjects(any(DeleteObjectsRequest.class));
                verify(controlClient).listObjectsV2(org.mockito.ArgumentMatchers
                    .<Consumer<ListObjectsV2Request.Builder>>any());
                verify(dataClient).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
                org.mockito.Mockito.verify(controlClient, org.mockito.Mockito.never())
                    .uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
            } finally {
                storage.close();
            }
            verify(dataClient).close();
            verify(controlClient).close();
        }
    }

    /**
     * Given non-CRT routing with a distinct LIST client, low-throughput writes stay on the write client.
     */
    @Test
    void testNettyControlWritesUseWriteClient() throws Exception {
        S3AsyncClient writeClient = mock(S3AsyncClient.class);
        S3AsyncClient listClient = mock(S3AsyncClient.class);
        AwsObjectStorage storage = new AwsObjectStorage(writeClient, listClient, "bucket",
            ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION);
        when(writeClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder().uploadId("upload").build()));
        when(writeClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CompleteMultipartUploadResponse.builder().build()));
        when(writeClient.uploadPartCopy(any(UploadPartCopyRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(UploadPartCopyResponse.builder()
                .copyPartResult(CopyPartResult.builder().eTag("copied").build()).build()));
        when(writeClient.deleteObjects(any(DeleteObjectsRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build()));
        try {
            ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
                .apiCallAttemptTimeout(TimeUnit.MINUTES.toMillis(2));
            String uploadId = storage.doCreateMultipartUpload(options, "key").get();
            storage.doUploadPartCopy(options, "source", "key", 0, 1, uploadId, 1).get();
            storage.doCompleteMultipartUpload(options, "key", uploadId,
                List.of(new ObjectStorage.ObjectStorageCompletedPart(1, "etag", null))).get();
            storage.doDeleteObjects(List.of("key")).get();
            org.mockito.Mockito.verify(listClient, org.mockito.Mockito.never())
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
            org.mockito.Mockito.verify(listClient, org.mockito.Mockito.never())
                .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            org.mockito.Mockito.verify(listClient, org.mockito.Mockito.never())
                .uploadPartCopy(any(UploadPartCopyRequest.class));
            org.mockito.Mockito.verify(listClient, org.mockito.Mockito.never())
                .deleteObjects(any(DeleteObjectsRequest.class));
        } finally {
            storage.close();
        }
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
