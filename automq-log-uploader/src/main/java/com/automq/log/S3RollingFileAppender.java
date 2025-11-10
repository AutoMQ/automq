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

package com.automq.log;

import com.automq.log.uploader.LogRecorder;
import com.automq.log.uploader.LogUploader;
import com.automq.log.uploader.S3LogConfig;
import com.automq.log.uploader.S3LogConfigProvider;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RollingFileAppender extends RollingFileAppender {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3RollingFileAppender.class);
    private static final Object INIT_LOCK = new Object();

    private static volatile LogUploader logUploaderInstance;
    private static volatile S3LogConfigProvider configProvider;
    private static volatile S3LogConfig s3LogConfig;
    
    public S3RollingFileAppender() {
        super();
    }

    /**
     * Programmatically sets the configuration provider to be used by all {@link S3RollingFileAppender} instances.
     */
    public static void setConfigProvider(S3LogConfigProvider provider) {
        synchronized (INIT_LOCK) {
            configProvider = provider;
        }
        triggerInitialization();
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        initializeUploader();
    }
    
    public static void setS3Config(S3LogConfig config) {
        s3LogConfig = config;
        triggerInitialization();
    }

    private void initializeUploader() {
        if (logUploaderInstance != null) {
            return;
        }
        synchronized (INIT_LOCK) {
            if (logUploaderInstance != null) {
                return;
            }
            try {
                if (s3LogConfig == null) {
                    LOGGER.info("No s3LogConfig available; S3 log upload remains disabled.");
                    return;
                }
                if (!s3LogConfig.isEnabled() || s3LogConfig.objectStorage() == null) {
                    LOGGER.info("S3 log upload is disabled by configuration.");
                    return;
                }

                LogUploader uploader = new LogUploader();
                uploader.start(s3LogConfig);
                logUploaderInstance = uploader;

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        uploader.close();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.warn("Failed to close LogUploader gracefully", e);
                    }
                }));
                LOGGER.info("S3RollingFileAppender initialized successfully using s3LogConfig {}.", s3LogConfig.getClass().getName());
            } catch (Exception e) {
                LOGGER.error("Failed to initialize S3RollingFileAppender", e);
            }
        }
    }

    public static void triggerInitialization() {
        S3LogConfigProvider provider;
        synchronized (INIT_LOCK) {
            if (logUploaderInstance != null) {
                return;
            }
            provider = configProvider;
        }
        if (provider == null) {
            return;
        }
        new S3RollingFileAppender().initializeUploader();
    }

    @Override
    protected void subAppend(LoggingEvent event) {
        super.subAppend(event);
        if (!closed && logUploaderInstance != null) {
            LogRecorder.LogEvent logEvent = new LogRecorder.LogEvent(
                event.getTimeStamp(),
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getRenderedMessage(),
                event.getThrowableStrRep());

            try {
                logEvent.validate();
                logUploaderInstance.append(logEvent);
            } catch (IllegalArgumentException e) {
                errorHandler.error("Failed to validate and append log event", e, 0);
            }
        }
    }
}
