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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.apache.kafka.common.utils.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static net.sourceforge.argparse4j.impl.Arguments.store;


/**
 * S3BucketAccessTester is a tool for testing access to S3 buckets.
 */
public class S3BucketAccessTester implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3BucketAccessTester.class);

    public static void main(String[] args) throws Exception {
        Namespace namespace = null;
        ArgumentParser parser = TestConfig.parser();
        try {
            namespace = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            Exit.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
        TestConfig config = new TestConfig(namespace);

        try (S3BucketAccessTester tester = new S3BucketAccessTester(config)) {
            tester.run();
        }
    }

    static class TestConfig {
        final String endpoint;
        final String region;
        final String bucket;

        TestConfig(Namespace res) {
            this.endpoint = res.getString("endpoint");
            this.region = res.getString("region");
            this.bucket = res.getString("bucket");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                    .newArgumentParser("S3BucketAccessTester")
                    .defaultHelp(true)
                    .description("Test access to S3 buckets");
            parser.addArgument("-e", "--endpoint")
                    .action(store())
                    .setDefault((String) null)
                    .type(String.class)
                    .dest("endpoint")
                    .metavar("ENDPOINT")
                    .help("S3 endpoint");
            parser.addArgument("-r", "--region")
                    .action(store())
                    .setDefault((String) null)
                    .type(String.class)
                    .dest("region")
                    .metavar("REGION")
                    .help("S3 region");
            parser.addArgument("-b", "--bucket")
                    .action(store())
                    .required(true)
                    .type(String.class)
                    .dest("bucket")
                    .metavar("BUCKET")
                    .help("S3 bucket");
            return parser;
        }
    }

    private final S3AsyncClient s3Client;

    private final String bucket;

    S3BucketAccessTester(TestConfig config) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder();
        if (config.endpoint != null) {
            builder.endpointOverride(URI.create(config.endpoint));
        }
        if (config.region != null) {
            builder.region(Region.of(config.region));
        }
        this.s3Client = builder.build();

        this.bucket = config.bucket;
    }

    private void run() throws Exception {
        LOGGER.info("Testing access to bucket {}", bucket);

        // Test existence of bucket
        testBucket();

        byte[] content1 = new byte[5 << 20];
        Arrays.fill(content1, (byte) 0x42);
        byte[] content2 = new byte[1 << 20];
        Arrays.fill(content2, (byte) 0x84);

        // Simple test for put/get/delete object
        String path = String.format("test-%d", System.currentTimeMillis());
        testPutObject(path, content1);
        testGetObject(path, content1);
        testDeleteObject(path);

        // Test for multipart upload
        String multipartPath = String.format("test-multipart-%d", System.currentTimeMillis());
        String uploadId = testCreateMultiPartUpload(multipartPath);
        List<CompletedPart> parts = testUploadPart(uploadId, multipartPath, content1, content2);
        testCompleteMultiPartUpload(uploadId, multipartPath, parts);
        testGetObject(multipartPath, content1, content2);
        testDeleteObject(multipartPath);

        LOGGER.info("Access to bucket {} is OK", bucket);
    }

    private void testBucket() {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucket)).get();
            LOGGER.info("Bucket exists, bucket: {}", bucket);
        } catch (Exception e) {
            throw new RuntimeException(String.format("cannot access bucket %s", bucket), e);
        }
    }

    private void testPutObject(String path, byte[] content) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .expires(Instant.now().plusSeconds(120))
                    .build();
            AsyncRequestBody body = AsyncRequestBody.fromBytes(content);
            s3Client.putObject(request, body).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to put object to bucket %s", bucket), e);
        }
        LOGGER.info("Put object to bucket, bucket: {}, path: {}, size: {}", bucket, path, content.length);
    }

    private void testGetObject(String path, byte[]... expectedContents) throws IOException {
        ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();
        for (byte[] content : expectedContents) {
            expectedStream.write(content);
        }
        byte[] expectedContent = expectedStream.toByteArray();

        byte[] content;
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            content = s3Client.getObject(request, AsyncResponseTransformer.toBytes())
                    .get()
                    .asByteArray();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to get object from bucket %s", bucket), e);
        }
        if (!Arrays.equals(expectedContent, content)) {
            throw new RuntimeException(String.format("content of object %s in bucket %s is not expected", path, bucket));
        }
        LOGGER.info("Get object from bucket, bucket: {}, path: {}", bucket, path);
    }

    private void testDeleteObject(String path) {
        try {
            s3Client.deleteObject(builder -> builder.bucket(bucket).key(path)).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to delete object from bucket %s", bucket), e);
        }
        LOGGER.info("Delete object from bucket, bucket: {}, path: {}", bucket, path);
    }

    private String testCreateMultiPartUpload(String path) {
        CreateMultipartUploadResponse response;
        try {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .expires(Instant.now().plusSeconds(120))
                    .build();
            response = s3Client.createMultipartUpload(request).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to create multipart upload from bucket %s", bucket), e);
        }
        LOGGER.info("Create multipart upload, bucket: {}, path: {}", bucket, path);
        return response.uploadId();
    }

    private List<CompletedPart> testUploadPart(String uploadId, String path, byte[]... contents) {
        List<CompletedPart> parts = new ArrayList<>();
        try {
            for (int i = 0; i < contents.length; i++) {
                UploadPartRequest request = UploadPartRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .uploadId(uploadId)
                        .partNumber(i + 1)
                        .build();
                AsyncRequestBody body = AsyncRequestBody.fromBytes(contents[i]);
                UploadPartResponse response = s3Client.uploadPart(request, body).get();
                parts.add(CompletedPart.builder().partNumber(i + 1).eTag(response.eTag()).build());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to upload part to bucket %s", bucket), e);
        }
        LOGGER.info("Upload parts, bucket: {}, path: {}, count: {}, size: {}",
                bucket, path, parts.size(), Arrays.stream(contents).mapToInt(b -> b.length).sum());
        return parts;
    }

    private void testCompleteMultiPartUpload(String uploadId, String path, List<CompletedPart> parts) {
        try {
            CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                    .build();
            s3Client.completeMultipartUpload(request).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to complete multipart upload to bucket %s", bucket), e);
        }
        LOGGER.info("Complete multipart upload, bucket: {}, path: {}", bucket, path);
    }

    @Override
    public void close() throws Exception {
        s3Client.close();
    }
}
