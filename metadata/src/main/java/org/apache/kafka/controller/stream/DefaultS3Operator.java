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

package org.apache.kafka.controller.stream;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.metadata.stream.S3Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.utils.StringUtils;


public class DefaultS3Operator implements S3Operator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Operator.class);

    public static final String ACCESS_KEY_NAME = "KAFKA_S3_ACCESS_KEY";

    public static final String SECRET_KEY_NAME = "KAFKA_S3_SECRET_KEY";

    private final String bucket;
    private final S3AsyncClient s3;

    public DefaultS3Operator(S3Config config) {
        String bucket = config.bucket();
        String endpoint = config.endpoint();
        String region = config.region();
        S3AsyncClientBuilder builder = S3AsyncClient.builder().region(Region.of(region));
        if (StringUtils.isNotBlank(region)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        builder.credentialsProvider(AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(
                () -> AwsBasicCredentials.create(System.getenv(ACCESS_KEY_NAME), System.getenv(SECRET_KEY_NAME)),
                InstanceProfileCredentialsProvider.create(),
                AnonymousCredentialsProvider.create()
            ).build()
        );
        this.s3 = builder.build();
        this.bucket = bucket;
        LOGGER.info("[ControllerS3Operator]: init with endpoint: {}, region: {}, bucket: {}", endpoint, region, bucket);
    }

    @Override
    public void close() {
        s3.close();
        LOGGER.info("[ControllerS3Operator]: closed");
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        ObjectIdentifier[] toDeleteKeys = objectKeys.stream().map(key ->
            ObjectIdentifier.builder()
                .key(key)
                .build()
        ).toArray(ObjectIdentifier[]::new);
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(toDeleteKeys).build())
            .build();
        long start = System.currentTimeMillis();
        // TODO: handle not exist object, should we regard it as deleted or ignore it.
        return this.s3.deleteObjects(request).thenApply(resp -> {
            LOGGER.info("[ControllerS3Operator]: Delete objects: {}, cost: {}", Arrays.toString(resp.deleted().toArray()), System.currentTimeMillis() - start);
            return resp.deleted().stream().map(DeletedObject::key).collect(Collectors.toList());
        }).exceptionally(ex -> {
            LOGGER.error("[ControllerS3Operator]:Delete objects: {} failed", Arrays.toString(objectKeys.toArray()), ex);
            return Collections.emptyList();
        });
    }
}
