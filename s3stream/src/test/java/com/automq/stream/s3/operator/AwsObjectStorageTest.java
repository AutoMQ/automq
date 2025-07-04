package com.automq.stream.s3.operator;

import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void testListObjectsPagination() throws Exception {
        // Mock S3AsyncClient
        S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
        
        // Create test objects for first page (truncated response)
        S3Object obj1 = S3Object.builder()
            .key("test-object-1")
            .lastModified(Instant.now())
            .size(100L)
            .build();
        S3Object obj2 = S3Object.builder()
            .key("test-object-2")
            .lastModified(Instant.now())
            .size(200L)
            .build();
            
        // Create test objects for second page (final response)
        S3Object obj3 = S3Object.builder()
            .key("test-object-3")
            .lastModified(Instant.now())
            .size(300L)
            .build();
            
        // First response (truncated)
        ListObjectsV2Response firstResponse = ListObjectsV2Response.builder()
            .contents(obj1, obj2)
            .isTruncated(true)
            .nextContinuationToken("token123")
            .build();
            
        // Second response (final)
        ListObjectsV2Response secondResponse = ListObjectsV2Response.builder()
            .contents(obj3)
            .isTruncated(false)
            .build();
        
        // Mock client responses based on continuation token
        when(mockS3Client.listObjectsV2(any(Consumer.class)))
            .thenAnswer(invocation -> {
                Consumer<ListObjectsV2Request.Builder> builderConsumer = invocation.getArgument(0);
                ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
                builderConsumer.accept(builder);
                ListObjectsV2Request request = builder.build();
                
                if (request.continuationToken() == null) {
                    // First request (no continuation token)
                    return CompletableFuture.completedFuture(firstResponse);
                } else {
                    // Second request (with continuation token)
                    return CompletableFuture.completedFuture(secondResponse);
                }
            });
        
        // Create AwsObjectStorage instance with mocked client
        AwsObjectStorage storage = new AwsObjectStorage(mockS3Client, "test-bucket");
        
        // Test the doList method
        CompletableFuture<List<ObjectInfo>> result = storage.doList("test-prefix");
        List<ObjectInfo> objects = result.get();
        
        // Verify all objects from both pages are returned
        Assertions.assertEquals(3, objects.size());
        Assertions.assertEquals("test-object-1", objects.get(0).key());
        Assertions.assertEquals("test-object-2", objects.get(1).key());
        Assertions.assertEquals("test-object-3", objects.get(2).key());
        
        // Verify object sizes
        Assertions.assertEquals(100L, objects.get(0).size());
        Assertions.assertEquals(200L, objects.get(1).size());
        Assertions.assertEquals(300L, objects.get(2).size());
    }

    @Test
    void testListObjectsNoPagination() throws Exception {
        // Test case where all objects fit in single response (no pagination needed)
        S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
        
        S3Object obj1 = S3Object.builder()
            .key("single-object")
            .lastModified(Instant.now())
            .size(500L)
            .build();
            
        ListObjectsV2Response response = ListObjectsV2Response.builder()
            .contents(obj1)
            .isTruncated(false) // No pagination needed
            .build();
        
        when(mockS3Client.listObjectsV2(any(Consumer.class)))
            .thenReturn(CompletableFuture.completedFuture(response));
        
        AwsObjectStorage storage = new AwsObjectStorage(mockS3Client, "test-bucket");
        
        CompletableFuture<List<ObjectInfo>> result = storage.doList("single-prefix");
        List<ObjectInfo> objects = result.get();
        
        Assertions.assertEquals(1, objects.size());
        Assertions.assertEquals("single-object", objects.get(0).key());
        Assertions.assertEquals(500L, objects.get(0).size());
    }

    @Test
    void testListObjectsEmptyResult() throws Exception {
        // Test case where no objects match the prefix
        S3AsyncClient mockS3Client = mock(S3AsyncClient.class);
        
        ListObjectsV2Response response = ListObjectsV2Response.builder()
            .contents(List.of()) // Empty list - explicit type
            .isTruncated(false)
            .build();
        
        when(mockS3Client.listObjectsV2(any(Consumer.class)))
            .thenReturn(CompletableFuture.completedFuture(response));
        
        AwsObjectStorage storage = new AwsObjectStorage(mockS3Client, "test-bucket");
        
        CompletableFuture<List<ObjectInfo>> result = storage.doList("nonexistent-prefix");
        List<ObjectInfo> objects = result.get();
        
        Assertions.assertEquals(0, objects.size());
    }
}
