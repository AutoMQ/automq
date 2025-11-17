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

package org.apache.kafka.connect.automq.log;

import com.automq.log.uploader.S3LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides S3 log uploader configuration for Kafka Connect workers.
 */
public class ConnectS3LogConfigProvider {
    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConnectS3LogConfigProvider.class);
    }
    private static final AtomicReference<Properties> CONFIG = new AtomicReference<>();
    private static final long WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final CountDownLatch INIT = new CountDownLatch(1);

    public static void initialize(Properties workerProps) {
        try {
            if (workerProps == null) {
                CONFIG.set(null);
                return;
            }
            Properties copy = new Properties();
            for (Map.Entry<Object, Object> entry : workerProps.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    copy.put(entry.getKey(), entry.getValue());
                }
            }
            CONFIG.set(copy);
        } finally {
            INIT.countDown();
        }
        getLogger().info("Initializing ConnectS3LogConfigProvider");
    }
    
    public S3LogConfig get() {
        
        try {
            if (!INIT.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                getLogger().warn("S3 log uploader config not initialized within timeout; uploader disabled.");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            getLogger().warn("Interrupted while waiting for S3 log uploader config; uploader disabled.");
            return null;
        }

        Properties source = CONFIG.get();
        if (source == null) {
            getLogger().warn("S3 log upload configuration was not provided; uploader disabled.");
            return null;
        }
        
        String bucketURI = source.getProperty(LogConfigConstants.LOG_S3_BUCKET_KEY);
        String clusterId = source.getProperty(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY);
        String nodeIdStr = resolveNodeId(source);
        boolean enable = Boolean.parseBoolean(source.getProperty(LogConfigConstants.LOG_S3_ENABLE_KEY, "false"));
        return new ConnectS3LogConfig(enable, clusterId, Integer.parseInt(nodeIdStr), bucketURI);
    }

    private String resolveNodeId(Properties workerProps) {
        String fromConfig = workerProps.getProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY);
        if (!isBlank(fromConfig)) {
            return fromConfig.trim();
        }
        String env = System.getenv("CONNECT_NODE_ID");
        if (!isBlank(env)) {
            return env.trim();
        }
        String host = workerProps.getProperty("automq.log.s3.node.hostname");
        if (isBlank(host)) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = System.getenv().getOrDefault("HOSTNAME", "0");
            }
        }
        return Integer.toString(host.hashCode() & Integer.MAX_VALUE);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
