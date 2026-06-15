package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metrics.operations.S3Operation;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AwsObjectStorageTest {

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
