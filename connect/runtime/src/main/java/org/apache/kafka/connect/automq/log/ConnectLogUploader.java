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

import com.automq.log.S3RollingFileAppender;
import com.automq.log.uploader.S3LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Initializes the AutoMQ S3 log uploader for Kafka Connect.
 */
public final class ConnectLogUploader {
    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConnectLogUploader.class);
    }    
    
    private ConnectLogUploader() {
    }

    public static void initialize(Map<String, String> workerProps) {
        Properties props = new Properties();
        if (workerProps != null) {
            workerProps.forEach((k, v) -> {
                if (k != null && v != null) {
                    props.put(k, v);
                }
            });
        }
        ConnectS3LogConfigProvider.initialize(props);
        S3LogConfig s3LogConfig = new ConnectS3LogConfigProvider().get();
        S3RollingFileAppender.setup(s3LogConfig);
        getLogger().info("Initialized Connect S3 log uploader context");
    }
}
