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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Factory for loading UploaderNodeSelector implementations via SPI.
 * This enables third parties to contribute their own node selection implementations.
 */
public class UploaderNodeSelectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploaderNodeSelectorFactory.class);
    
    private static final Map<String, UploaderNodeSelectorProvider> PROVIDERS = new HashMap<>();
    
    static {
        // Load providers using SPI
        ServiceLoader<UploaderNodeSelectorProvider> serviceLoader = ServiceLoader.load(UploaderNodeSelectorProvider.class);
        for (UploaderNodeSelectorProvider provider : serviceLoader) {
            String type = provider.getType();
            LOGGER.info("Loaded UploaderNodeSelectorProvider for type: {}", type);
            PROVIDERS.put(type.toLowerCase(), provider);
        }
    }
    
    private UploaderNodeSelectorFactory() {
        // Utility class, no instances
    }
    
    /**
     * Creates a node selector based on the specified type and configuration.
     *
     * @param typeString The selector type (can be a built-in type or custom type from SPI)
     * @param clusterId The cluster ID
     * @param nodeId The node ID
     * @param config Additional configuration parameters
     * @return A UploaderNodeSelector instance or null if type is not supported
     */
    public static UploaderNodeSelector createSelector(String typeString, String clusterId, int nodeId, Map<String, String> config) {
        UploaderNodeSelectorType type = UploaderNodeSelectorType.fromString(typeString);
        
        // Handle built-in selectors based on the enum type
        switch (type) {
            case STATIC:
                boolean isPrimaryUploader = Boolean.parseBoolean(config.getOrDefault("isPrimaryUploader", "false"));
                return UploaderNodeSelectors.staticSelector(isPrimaryUploader);
                
            case NODE_ID:
                int primaryNodeId = Integer.parseInt(config.getOrDefault("primaryNodeId", "0"));
                return UploaderNodeSelectors.nodeIdSelector(nodeId, primaryNodeId);
                
            case FILE:
                String leaderFile = config.getOrDefault("leaderFile", "/tmp/s3-metrics-leader");
                long timeoutMs = Long.parseLong(config.getOrDefault("leaderTimeoutMs", "60000"));
                return UploaderNodeSelectors.fileLeaderElectionSelector(leaderFile, nodeId, timeoutMs);
                
            case CUSTOM:
                // For custom types, try to find an SPI provider
                UploaderNodeSelectorProvider provider = PROVIDERS.get(typeString.toLowerCase());
                if (provider != null) {
                    try {
                        return provider.createSelector(clusterId, nodeId, config);
                    } catch (Exception e) {
                        LOGGER.error("Failed to create UploaderNodeSelector of type {} using provider {}", 
                            typeString, provider.getClass().getName(), e);
                    }
                }
                
                LOGGER.warn("Unsupported UploaderNodeSelector type: {}. Using static selector with isPrimaryUploader=false", typeString);
                return UploaderNodeSelectors.staticSelector(false);
        }
        
        // Should never reach here because all enum values are covered
        return UploaderNodeSelectors.staticSelector(false);
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
        UploaderNodeSelectorType type = UploaderNodeSelectorType.fromString(typeString);
        if (type != UploaderNodeSelectorType.CUSTOM) {
            return true;
        }
        
        // Then check custom SPI providers
        return PROVIDERS.containsKey(typeString.toLowerCase());
    }
    
    /**
     * Gets all supported selector types (built-in and from SPI).
     *
     * @return Array of supported selector types
     */
    public static String[] getSupportedTypes() {
        // Get built-in types from the enum
        String[] builtInTypes = Stream.of(UploaderNodeSelectorType.values())
            .filter(t -> t != UploaderNodeSelectorType.CUSTOM)
            .map(UploaderNodeSelectorType::getType)
            .toArray(String[]::new);
            
        String[] customTypes = PROVIDERS.keySet().toArray(new String[0]);
        
        String[] allTypes = new String[builtInTypes.length + customTypes.length];
        System.arraycopy(builtInTypes, 0, allTypes, 0, builtInTypes.length);
        System.arraycopy(customTypes, 0, allTypes, builtInTypes.length, customTypes.length);
        
        return allTypes;
    }
}
