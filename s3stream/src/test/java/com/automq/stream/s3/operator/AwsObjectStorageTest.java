package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class AwsObjectStorageTest {

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
}
