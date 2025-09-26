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

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RollingFileAppender extends RollingFileAppender {
    public static final String CONFIG_PROVIDER_PROPERTY = "automq.log.s3.config.provider";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3RollingFileAppender.class);
    private static final Object INIT_LOCK = new Object();

    private static volatile LogUploader logUploaderInstance;
    private static volatile S3LogConfigProvider configProvider;
    private static volatile boolean initializationPending;

    private String configProviderClass;

    public S3RollingFileAppender() {
        super();
    }

    /**
     * Allows programmatic override of the LogUploader instance.
     * Useful for testing or complex dependency injection scenarios.
     *
     * @param uploader The LogUploader instance to use.
     */
    public static void setLogUploader(LogUploader uploader) {
        synchronized (INIT_LOCK) {
            logUploaderInstance = uploader;
        }
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

    /**
     * Setter used by Log4j property configuration to specify a custom {@link S3LogConfigProvider} implementation.
     */
    public void setConfigProviderClass(String configProviderClass) {
        this.configProviderClass = configProviderClass;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        initializeUploader();
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
                S3LogConfigProvider provider = resolveProvider();
                if (provider == null) {
                    LOGGER.info("No S3LogConfigProvider available; S3 log upload remains disabled.");
                    initializationPending = true;
                    return;
                }
                S3LogConfig config = provider.get();
                if (config == null || !config.isEnabled() || config.objectStorage() == null) {
                    LOGGER.info("S3 log upload is disabled by configuration.");
                    initializationPending = config == null;
                    return;
                }

                LogUploader uploader = new LogUploader();
                uploader.start(config);
                logUploaderInstance = uploader;
                initializationPending = false;

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        uploader.close();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.warn("Failed to close LogUploader gracefully", e);
                    }
                }));
                LOGGER.info("S3RollingFileAppender initialized successfully using provider {}.",
                    provider.getClass().getName());
            } catch (Exception e) {
                LOGGER.error("Failed to initialize S3RollingFileAppender", e);
                initializationPending = true;
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
            initializationPending = true;
            return;
        }
        new S3RollingFileAppender().initializeUploader();
    }

    private S3LogConfigProvider resolveProvider() {
        S3LogConfigProvider provider = configProvider;
        if (provider != null) {
            return provider;
        }

        synchronized (INIT_LOCK) {
            if (configProvider != null) {
                return configProvider;
            }

            String providerClassName = configProviderClass;
            if (StringUtils.isBlank(providerClassName)) {
                providerClassName = System.getProperty(CONFIG_PROVIDER_PROPERTY);
            }

            if (StringUtils.isNotBlank(providerClassName)) {
                provider = instantiateProvider(providerClassName.trim());
                if (provider == null) {
                    LOGGER.warn("Falling back to default configuration provider because {} could not be instantiated.",
                        providerClassName);
                }
            }

            if (provider == null) {
                provider = new PropertiesS3LogConfigProvider();
            }

            configProvider = provider;
            return provider;
        }
    }

    private S3LogConfigProvider instantiateProvider(String providerClassName) {
        try {
            Class<?> clazz = Class.forName(providerClassName);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            if (!(instance instanceof S3LogConfigProvider)) {
                LOGGER.error("Class {} does not implement S3LogConfigProvider.", providerClassName);
                return null;
            }
            return (S3LogConfigProvider) instance;
        } catch (Exception e) {
            LOGGER.error("Failed to instantiate S3LogConfigProvider {}", providerClassName, e);
            return null;
        }
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
