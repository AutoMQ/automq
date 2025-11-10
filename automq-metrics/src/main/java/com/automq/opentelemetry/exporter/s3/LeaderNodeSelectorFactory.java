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

package com.automq.opentelemetry.exporter.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Factory for loading LeaderNodeSelector implementations via SPI.
 * This enables third parties to contribute their own node selection implementations.
 */
public class LeaderNodeSelectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNodeSelectorFactory.class);
    
    private static final Map<String, LeaderNodeSelectorProvider> PROVIDERS = new HashMap<>();
    
    static {
        // Load providers using SPI
        ServiceLoader<LeaderNodeSelectorProvider> serviceLoader = ServiceLoader.load(LeaderNodeSelectorProvider.class);
        for (LeaderNodeSelectorProvider provider : serviceLoader) {
            String type = provider.getType();
            LOGGER.info("Loaded LeaderNodeSelectorProvider for type: {}", type);
            PROVIDERS.put(type.toLowerCase(Locale.ROOT), provider);
        }
    }
    
    private LeaderNodeSelectorFactory() {
        // Utility class, no instances
    }
    
    /**
     * Creates a node selector based on the specified type and configuration.
     *
     * @param typeString The selector type (can be a built-in type or custom type from SPI)
     * @param clusterId The cluster ID
     * @param nodeId The node ID
     * @param config Additional configuration parameters
     * @return A LeaderNodeSelector instance or null if type is not supported
     */
    public static LeaderNodeSelector createSelector(String typeString, String clusterId, int nodeId, Map<String, String> config) {
        LeaderNodeSelectorType type = LeaderNodeSelectorType.fromString(typeString);
        
        // Handle built-in selectors based on the enum type
        switch (type) {
            case CUSTOM:
                // For custom types, try to find an SPI provider
                LeaderNodeSelectorProvider provider = PROVIDERS.get(typeString.toLowerCase(Locale.ROOT));
                if (provider != null) {
                    try {
                        return provider.createSelector(clusterId, nodeId, config);
                    } catch (Exception e) {
                        LOGGER.error("Failed to create LeaderNodeSelector of type {} using provider {}", 
                            typeString, provider.getClass().getName(), e);
                    }
                }
                
                LOGGER.warn("Unsupported LeaderNodeSelector type: {}. Using supplier selector with static false value", typeString);
                return () -> false;
        }
        
        // Should never reach here because all enum values are covered
        return () -> false;
    }
    
    /**
     * Returns true if the specified selector type is supported.
     *
     * @param typeString The selector type to check
     * @return True if the type is supported, false otherwise
     */
    public static boolean isSupported(String typeString) {
        if (typeString == null) {
            return false;
        }
        
        // First check built-in types using the enum
        LeaderNodeSelectorType type = LeaderNodeSelectorType.fromString(typeString);
        if (type != LeaderNodeSelectorType.CUSTOM) {
            return true;
        }
        
        // Then check custom SPI providers
        return PROVIDERS.containsKey(typeString.toLowerCase(Locale.ROOT));
    }
    
    /**
     * Gets all supported selector types (built-in and from SPI).
     *
     * @return Array of supported selector types
     */
    public static String[] getSupportedTypes() {
        // Get built-in types from the enum
        String[] builtInTypes = Stream.of(LeaderNodeSelectorType.values())
            .filter(t -> t != LeaderNodeSelectorType.CUSTOM)
            .map(LeaderNodeSelectorType::getType)
            .toArray(String[]::new);
            
        String[] customTypes = PROVIDERS.keySet().toArray(new String[0]);
        
        String[] allTypes = new String[builtInTypes.length + customTypes.length];
        System.arraycopy(builtInTypes, 0, allTypes, 0, builtInTypes.length);
        System.arraycopy(customTypes, 0, allTypes, builtInTypes.length, customTypes.length);
        
        return allTypes;
    }
}
