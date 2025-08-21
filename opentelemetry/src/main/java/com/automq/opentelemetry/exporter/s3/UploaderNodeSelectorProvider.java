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

import java.util.Map;

/**
 * SPI interface for providing custom UploaderNodeSelector implementations.
 * Third-party libraries can implement this interface and register their implementations
 * using Java's ServiceLoader mechanism.
 */
public interface UploaderNodeSelectorProvider {
    
    /**
     * Returns the type identifier for this selector provider.
     * This is the string that should be used in configuration to select this provider.
     * 
     * @return A unique type identifier for this selector implementation
     */
    String getType();
    
    /**
     * Creates a new UploaderNodeSelector instance based on the provided configuration.
     * 
     * @param clusterId The cluster ID
     * @param nodeId The node ID of the current node
     * @param config Additional configuration parameters
     * @return A new UploaderNodeSelector instance
     * @throws Exception If the selector cannot be created
     */
    UploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) throws Exception;
}
