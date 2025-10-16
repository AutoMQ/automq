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

import com.automq.log.uploader.selector.LogUploaderNodeSelector;
import com.automq.log.uploader.selector.LogUploaderNodeSelectorFactory;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_ACTIVE_CONTROLLER;
import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_CLUSTER_ID;
import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_ENABLE;
import static com.automq.log.uploader.LogConfigConstants.DEFAULT_LOG_S3_NODE_ID;
import static com.automq.log.uploader.LogConfigConstants.LOG_PROPERTIES_FILE;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ACCESS_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ACTIVE_CONTROLLER_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_BUCKET_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_CLUSTER_ID_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ENABLE_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_ENDPOINT_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_NODE_ID_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_PRIMARY_NODE_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_REGION_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_SECRET_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_SELECTOR_PREFIX;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_SELECTOR_PRIMARY_NODE_ID_KEY;
import static com.automq.log.uploader.LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY;

public class DefaultS3LogConfig implements S3LogConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3LogConfig.class);

    private final Properties props;
    private ObjectStorage objectStorage;
    private LogUploaderNodeSelector nodeSelector;

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
    public LogUploaderNodeSelector nodeSelector() {
        if (nodeSelector == null) {
            initializeNodeSelector();
        }
        return nodeSelector;
    }

    private void initializeNodeSelector() {
        String selectorType = props.getProperty(LOG_S3_SELECTOR_TYPE_KEY, "static");
        Map<String, String> selectorConfig = new HashMap<>();
        Map<String, String> rawConfig = getPropertiesWithPrefix(LOG_S3_SELECTOR_PREFIX);
        String normalizedType = selectorType == null ? "" : selectorType.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, String> entry : rawConfig.entrySet()) {
            String key = entry.getKey();
            if (normalizedType.length() > 0 && key.toLowerCase(Locale.ROOT).startsWith(normalizedType + ".")) {
                key = key.substring(normalizedType.length() + 1);
            }
            if ("type".equalsIgnoreCase(key) || key.isEmpty()) {
                continue;
            }
            selectorConfig.putIfAbsent(key, entry.getValue());
        }

        selectorConfig.putIfAbsent("isPrimaryUploader",
            props.getProperty(LOG_S3_PRIMARY_NODE_KEY,
                props.getProperty(LOG_S3_ACTIVE_CONTROLLER_KEY, String.valueOf(DEFAULT_LOG_S3_ACTIVE_CONTROLLER))));

        String primaryNodeId = props.getProperty(LOG_S3_SELECTOR_PRIMARY_NODE_ID_KEY);
        if (StringUtils.isNotBlank(primaryNodeId)) {
            selectorConfig.putIfAbsent("primaryNodeId", primaryNodeId.trim());
        }

        try {
            this.nodeSelector = LogUploaderNodeSelectorFactory.createSelector(selectorType, clusterId(), nodeId(), selectorConfig);
        } catch (Exception e) {
            LOGGER.error("Failed to create log uploader selector of type {}", selectorType, e);
            this.nodeSelector = LogUploaderNodeSelector.staticSelector(false);
        }
    }

    private Map<String, String> getPropertiesWithPrefix(String prefix) {
        Map<String, String> result = new HashMap<>();
        if (prefix == null || prefix.isEmpty()) {
            return result;
        }
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String trimmed = key.substring(prefix.length());
                if (!trimmed.isEmpty()) {
                    result.put(trimmed, props.getProperty(key));
                }
            }
        }
        return result;
    }
}
