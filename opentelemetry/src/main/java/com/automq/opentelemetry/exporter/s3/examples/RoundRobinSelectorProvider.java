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

package com.automq.opentelemetry.exporter.s3.examples;

import com.automq.opentelemetry.exporter.s3.UploaderNodeSelector;
import com.automq.opentelemetry.exporter.s3.UploaderNodeSelectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example implementation of UploaderNodeSelectorProvider using a simple round-robin approach
 * for demonstration purposes. In a real environment, this would be in a separate module.
 */
public class RoundRobinSelectorProvider implements UploaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinSelectorProvider.class);
    
    private static final AtomicInteger CURRENT_PRIMARY = new AtomicInteger(0);
    // 1 minute
    private static final int DEFAULT_ROTATION_INTERVAL_MS = 60000;
    
    @Override
    public String getType() {
        return "roundrobin";
    }
    
    @Override
    public UploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) {
        int rotationIntervalMs = DEFAULT_ROTATION_INTERVAL_MS;
        if (config.containsKey("rotationIntervalMs")) {
            try {
                rotationIntervalMs = Integer.parseInt(config.get("rotationIntervalMs"));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid rotationIntervalMs value: {}, using default", config.get("rotationIntervalMs"));
            }
        }
        
        int totalNodes = 1;
        if (config.containsKey("totalNodes")) {
            try {
                totalNodes = Integer.parseInt(config.get("totalNodes"));
                if (totalNodes < 1) {
                    LOGGER.warn("Invalid totalNodes value: {}, using 1", totalNodes);
                    totalNodes = 1;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid totalNodes value: {}, using 1", config.get("totalNodes"));
            }
        }
        
        LOGGER.info("Creating round-robin selector for node {} in cluster {} with {} total nodes and rotation interval {}ms",
            nodeId, clusterId, totalNodes, rotationIntervalMs);
        
        return new RoundRobinSelector(nodeId, totalNodes, rotationIntervalMs);
    }
    
    /**
     * A selector that rotates the primary uploader role among nodes.
     */
    private static class RoundRobinSelector implements UploaderNodeSelector {
        private final int nodeId;
        private final int totalNodes;
        private final long rotationIntervalMs;
        private final long startTimeMs;
        
        RoundRobinSelector(int nodeId, int totalNodes, long rotationIntervalMs) {
            this.nodeId = nodeId;
            this.totalNodes = totalNodes;
            this.rotationIntervalMs = rotationIntervalMs;
            this.startTimeMs = System.currentTimeMillis();
        }
        
        @Override
        public boolean isPrimaryUploader() {
            if (totalNodes <= 1) {
                return true; // If only one node, it's always primary
            }
            
            // Calculate the current primary node based on time
            long elapsedMs = System.currentTimeMillis() - startTimeMs;
            int rotations = (int)(elapsedMs / rotationIntervalMs);
            int currentPrimary = rotations % totalNodes;
            
            return nodeId == currentPrimary;
        }
    }
}
