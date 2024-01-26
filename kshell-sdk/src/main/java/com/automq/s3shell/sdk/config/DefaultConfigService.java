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
package com.automq.s3shell.sdk.config;

import com.automq.s3shell.sdk.client.S3ClientManager;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class DefaultConfigService implements ConfigService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfigService.class);
    private static final String S3_CONFIG_KEY_TEMPLATE = "automq/config/%s/%s.properties";

    private S3ClientManager s3ClientManager;
    private String instanceId;

    @Override
    public void init(String instanceId, S3ClientManager s3ClientManager) {
        this.instanceId = instanceId;
        this.s3ClientManager = s3ClientManager;
    }

    @Override
    public Properties load() {
        S3Client s3Client = s3ClientManager.getS3Client();
        String key = getConfigKey(instanceId);
        String bucket = s3ClientManager.getBucket();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        ResponseInputStream<GetObjectResponse> object = null;
        try {
            object = s3Client.getObject(getObjectRequest);
        } catch (NoSuchKeyException e) {
            LOGGER.debug("Config not found, bucket: {}, key: {}", bucket, key);
        }
        if (object != null) {
            try {
                Properties properties = new Properties();
                properties.load(object);
                return properties;
            } catch (IOException e) {
                LOGGER.error(String.format("Read config data failed, bucket: %s, key: %s", bucket, key), e);
            }
        }
        return null;
    }

    @Override
    public void upload(Properties properties) throws IOException {
        S3Client s3Client = s3ClientManager.getS3Client();
        String key = getConfigKey(instanceId);
        String bucket = s3ClientManager.getBucket();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        StringWriter stringWriter = new StringWriter();
        properties.store(stringWriter, "");
        String content = stringWriter.toString();
        s3Client.putObject(putObjectRequest, RequestBody.fromString(content));
    }

    private String getConfigKey(String instanceId) {
        return String.format(S3_CONFIG_KEY_TEMPLATE, s3ClientManager.getClusterId(), instanceId);
    }
}
