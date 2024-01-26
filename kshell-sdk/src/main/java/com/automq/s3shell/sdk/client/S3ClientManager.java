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
package com.automq.s3shell.sdk.client;

import com.automq.s3shell.sdk.auth.CredentialsProviderHolder;
import com.automq.s3shell.sdk.model.S3Url;
import java.net.URI;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientManager {
    private S3Client s3Client;
    private String clusterId;
    private String bucket;

    public void init(String s3UrlString) {
        S3Url s3Url = S3Url.parse(s3UrlString);
        String bucket = s3Url.getS3OpsBucket();
        if (bucket == null) {
            throw new IllegalArgumentException("s3-ops-bucket not found");
        }
        this.bucket = bucket;
        String region = s3Url.getS3Region();
        if (region == null) {
            throw new IllegalArgumentException("s3-region not found");
        }
        String clusterId = s3Url.getClusterId();
        if (clusterId == null) {
            throw new IllegalArgumentException("cluster-id not found");
        }
        this.clusterId = clusterId;
        URI endPointUri = URI.create(s3Url.getEndpointProtocol().getName() + "://" + s3Url.getS3Endpoint());
        s3Client = S3Client.builder()
            .credentialsProvider(CredentialsProviderHolder.getAwsCredentialsProvider())
            .endpointOverride(endPointUri)
            .region(Region.of(region))
            .build();
    }

    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getBucket() {
        return bucket;
    }
}
