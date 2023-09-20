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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

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

    private void run() {
        LOGGER.info("Testing access to bucket {}", bucket);

        testBucket();

        String path = String.format("test-%d", System.currentTimeMillis());
        String content = String.format("test-%d", System.currentTimeMillis());
        testPutObject(path, content);
        testGetObject(path, content);
        testDeleteObject(path);

        LOGGER.info("Access to bucket {} is OK", bucket);
    }

    private void testBucket() {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucket)).get();
            LOGGER.info("Bucket {} exists", bucket);
        } catch (Exception e) {
            throw new RuntimeException(String.format("bucket %s does not exist", bucket), e);
        }
    }

    private void testPutObject(String path, String content) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .expires(Instant.now().plusSeconds(120))
                    .build();
            AsyncRequestBody body = AsyncRequestBody.fromString(content, StandardCharsets.UTF_8);
            s3Client.putObject(request, body).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to put object to bucket %s", bucket), e);
        }
        LOGGER.info("Put object {} to bucket {} successfully", path, bucket);
    }

    private void testGetObject(String path, String expectedContent) {
        String content;
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            content = s3Client.getObject(request, AsyncResponseTransformer.toBytes())
                    .get()
                    .asString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to get object from bucket %s", bucket), e);
        }
        if (!expectedContent.equals(content)) {
            throw new RuntimeException(String.format("content of object %s in bucket %s is not expected, expected: %s, actual: %s",
                    path, bucket, expectedContent, content));
        }
        LOGGER.info("Get object {} from bucket {} successfully", path, bucket);
    }

    private void testDeleteObject(String path) {
        try {
            s3Client.deleteObject(builder -> builder.bucket(bucket).key(path)).get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to delete object from bucket %s", bucket), e);
        }
        LOGGER.info("Delete object {} from bucket {} successfully", path, bucket);
    }

    @Override
    public void close() throws Exception {
        s3Client.close();
    }
}
