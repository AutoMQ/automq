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

package com.automq.log.uploader.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory that resolves node selectors from configuration.
 */
public final class LogLeaderNodeSelectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogLeaderNodeSelectorFactory.class);
    private static final Map<String, LogLeaderNodeSelectorProvider> PROVIDERS = new HashMap<>();

    static {
        ServiceLoader<LogLeaderNodeSelectorProvider> loader = ServiceLoader.load(LogLeaderNodeSelectorProvider.class);
        for (LogLeaderNodeSelectorProvider provider : loader) {
            String type = provider.getType();
            if (type != null) {
                PROVIDERS.put(type.toLowerCase(Locale.ROOT), provider);
                LOGGER.info("Loaded LeaderNodeSelectorProvider for type {}", type);
            }
        }
    }

    private LogLeaderNodeSelectorFactory() {
    }

    public static LogLeaderNodeSelector createSelector(String typeString,
                                                       String clusterId,
                                                       int nodeId,
                                                       Map<String, String> config) {
        LogLeaderNodeSelectorType type = LogLeaderNodeSelectorType.fromString(typeString);
        switch (type) {
            case CUSTOM:
                LogLeaderNodeSelectorProvider provider = PROVIDERS.get(typeString.toLowerCase(Locale.ROOT));
                if (provider != null) {
                    try {
                        return provider.createSelector(clusterId, nodeId, config);
                    } catch (Exception e) {
                        LOGGER.error("Failed to create selector of type {}", typeString, e);
                    }
                }
                LOGGER.warn("Unsupported log uploader selector type {}, falling back to supplier with static false", typeString);
                return () -> false;
            default:
                return () -> false;
        }
    }

    public static boolean isSupported(String typeString) {
        if (typeString == null) {
            return true;
        }
        LogLeaderNodeSelectorType type = LogLeaderNodeSelectorType.fromString(typeString);
        if (type != LogLeaderNodeSelectorType.CUSTOM) {
            return true;
        }
        return PROVIDERS.containsKey(typeString.toLowerCase(Locale.ROOT));
    }
}
