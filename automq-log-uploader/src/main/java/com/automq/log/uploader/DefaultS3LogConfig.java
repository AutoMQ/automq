/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.log.uploader;

import com.automq.log.uploader.selector.LogLeaderNodeSelector;
import com.automq.log.uploader.selector.runtime.RuntimeLeaderSelectorProvider;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_CLUSTER_ID;
import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_ENABLE;
import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_NODE_ID;
import static com.automq.log.uploader.LogConfigConstants.LOG_PROPERTIES_FILE;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ACCESS_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_BUCKET_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_CLUSTER_ID_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ENABLE_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ENDPOINT_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_NODE_ID_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_REGION_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_SECRET_KEY;

public class DefaultS3LogConfig implements S3LogConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3LogConfig.class);

    private final Properties props;
    private ObjectStorage objectStorage;
    private LogLeaderNodeSelector leaderNodeSelector;

    public DefaultS3LogConfig() {
        this(null);
    }

    public DefaultS3LogConfig(Properties overrideProps) {
        this.props = new Properties();
        if (overrideProps != null) {
            this.props.putAll(overrideProps);
        }
        if (overrideProps == null) {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(LOG_PROPERTIES_FILE)) {
                if (input != null) {
                    props.load(input);
                    LOGGER.info("Loaded log configuration from {}", LOG_PROPERTIES_FILE);
                } else {
                    LOGGER.warn("Could not find {}, using default log configurations.", LOG_PROPERTIES_FILE);
                }
            } catch (IOException ex) {
                LOGGER.error("Failed to load log configuration from {}.", LOG_PROPERTIES_FILE, ex);
            }
        }
        initializeNodeSelector();
    }

    @Override
    public boolean isEnabled() {
        return Boolean.parseBoolean(props.getProperty(LOG_S3_ENABLE_KEY, String.valueOf(DEFAULT_LOG_S3_ENABLE)));
    }

    @Override
    public String clusterId() {
        return props.getProperty(LOG_S3_CLUSTER_ID_KEY, DEFAULT_LOG_S3_CLUSTER_ID);
    }

    @Override
    public int nodeId() {
        return Integer.parseInt(props.getProperty(LOG_S3_NODE_ID_KEY, String.valueOf(DEFAULT_LOG_S3_NODE_ID)));
    }

    @Override
    public synchronized ObjectStorage objectStorage() {
        if (this.objectStorage != null) {
            return this.objectStorage;
        }
        String bucket = props.getProperty(LOG_S3_BUCKET_KEY);
        if (StringUtils.isBlank(bucket)) {
            LOGGER.error("Mandatory log config '{}' is not set.", LOG_S3_BUCKET_KEY);
            return null;
        }

        String normalizedBucket = bucket.trim();
        if (!normalizedBucket.contains("@")) {
            String region = props.getProperty(LOG_S3_REGION_KEY);
            if (StringUtils.isBlank(region)) {
                LOGGER.error("'{}' must be provided when '{}' is not a full AutoMQ bucket URI.",
                    LOG_S3_REGION_KEY, LOG_S3_BUCKET_KEY);
                return null;
            }
            String endpoint = props.getProperty(LOG_S3_ENDPOINT_KEY);
            String accessKey = props.getProperty(LOG_S3_ACCESS_KEY);
            String secretKey = props.getProperty(LOG_S3_SECRET_KEY);

            StringBuilder builder = new StringBuilder("0@s3://").append(normalizedBucket)
                .append("?region=").append(region.trim());
            if (StringUtils.isNotBlank(endpoint)) {
                builder.append("&endpoint=").append(endpoint.trim());
            }
            if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
                builder.append("&authType=static")
                    .append("&accessKey=").append(accessKey.trim())
                    .append("&secretKey=").append(secretKey.trim());
            }
            normalizedBucket = builder.toString();
        }

        BucketURI logBucket = BucketURI.parse(normalizedBucket);
        this.objectStorage = ObjectStorageFactory.instance().builder(logBucket).threadPrefix("s3-log-uploader").build();
        return this.objectStorage;
    }

    @Override
    public LogLeaderNodeSelector leaderSelector() {
        if (leaderNodeSelector == null) {
            initializeNodeSelector();
        }
        return leaderNodeSelector;
    }

    private void initializeNodeSelector() {
        try {
            this.leaderNodeSelector = new RuntimeLeaderSelectorProvider().createSelector();
        } catch (Exception e) {
            LOGGER.error("Failed to create log uploader selector of type", e);
            this.leaderNodeSelector = LogLeaderNodeSelector.staticSelector(false);
        }
    }
}
