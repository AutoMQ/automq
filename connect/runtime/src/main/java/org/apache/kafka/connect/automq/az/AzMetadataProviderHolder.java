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

package org.apache.kafka.connect.automq.az;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;

public final class AzMetadataProviderHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzMetadataProviderHolder.class);
    private static final AzMetadataProvider DEFAULT_PROVIDER = new AzMetadataProvider() { };

    private static volatile AzMetadataProvider provider = DEFAULT_PROVIDER;

    private AzMetadataProviderHolder() {
    }

    public static void initialize(Map<String, String> workerProps) {
        AzMetadataProvider selected = DEFAULT_PROVIDER;
        try {
            ServiceLoader<AzMetadataProvider> loader = ServiceLoader.load(AzMetadataProvider.class);
            for (AzMetadataProvider candidate : loader) {
                try {
                    candidate.configure(workerProps);
                    selected = candidate;
                    LOGGER.info("Loaded AZ metadata provider: {}", candidate.getClass().getName());
                    break;
                } catch (Exception e) {
                    LOGGER.warn("Failed to initialize AZ metadata provider: {}", candidate.getClass().getName(), e);
                }
            }
        } catch (Throwable t) {
            LOGGER.warn("Failed to load AZ metadata providers", t);
        }
        provider = selected;
    }

    public static AzMetadataProvider provider() {
        return provider;
    }

    public static void setProviderForTest(AzMetadataProvider newProvider) {
        provider = newProvider != null ? newProvider : DEFAULT_PROVIDER;
    }
}
