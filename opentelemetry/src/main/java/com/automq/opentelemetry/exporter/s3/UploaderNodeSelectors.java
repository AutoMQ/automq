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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class provides various implementations of the UploaderNodeSelector interface.
 */
public class UploaderNodeSelectors {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploaderNodeSelectors.class);

    private UploaderNodeSelectors() {
        // Utility class
    }

    /**
     * Creates a selector that uses a static boolean value.
     *
     * @param isPrimaryUploader whether this node should be the primary uploader
     * @return a selector that always returns the provided value
     */
    public static UploaderNodeSelector staticSelector(boolean isPrimaryUploader) {
        return () -> isPrimaryUploader;
    }

    /**
     * Creates a selector that uses a supplier to dynamically determine if this node is the primary uploader.
     *
     * @param supplier a function that determines if this node is the primary uploader
     * @return a selector that delegates to the supplier
     */
    public static UploaderNodeSelector supplierSelector(Supplier<Boolean> supplier) {
        return supplier::get;
    }

    /**
     * Creates a selector that checks if the current node's ID matches a specific node ID.
     * If it matches, this node will be considered the primary uploader.
     *
     * @param currentNodeId the ID of the current node
     * @param primaryNodeId the ID of the node that should be the primary uploader
     * @return a selector based on node ID matching
     */
    public static UploaderNodeSelector nodeIdSelector(int currentNodeId, int primaryNodeId) {
        return () -> currentNodeId == primaryNodeId;
    }

    /**
     * Creates a selector that uses a leader election file for multiple nodes.
     * The node that successfully creates or updates the leader file becomes the primary uploader.
     * This implementation periodically attempts to claim leadership.
     *
     * @param leaderFilePath the path to the leader election file
     * @param nodeId the ID of the current node
     * @param leaderTimeoutMs the maximum time in milliseconds before leadership can be claimed by another node
     * @return a selector based on file-based leader election
     */
    public static UploaderNodeSelector fileLeaderElectionSelector(String leaderFilePath, int nodeId, long leaderTimeoutMs) {
        Path path = Paths.get(leaderFilePath);
        
        // Create an atomic reference to track leadership status
        AtomicBoolean isLeader = new AtomicBoolean(false);
        
        // Start a background thread to periodically attempt to claim leadership
        Thread leaderElectionThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    boolean claimed = attemptToClaimLeadership(path, nodeId, leaderTimeoutMs);
                    isLeader.set(claimed);
                    
                    // Sleep for half the timeout period
                    Thread.sleep(leaderTimeoutMs / 2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error in leader election", e);
                    isLeader.set(false);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        
        leaderElectionThread.setDaemon(true);
        leaderElectionThread.setName("s3-metrics-leader-election");
        leaderElectionThread.start();
        
        // Return a selector that checks the current leadership status
        return isLeader::get;
    }
    
    private static boolean attemptToClaimLeadership(Path leaderFilePath, int nodeId, long leaderTimeoutMs) throws IOException {
        try {
            // Try to create directory if it doesn't exist
            Files.createDirectories(leaderFilePath.getParent());
            
            // Check if file exists
            if (Files.exists(leaderFilePath)) {
                // Read the current leader info
                List<String> lines = Files.readAllLines(leaderFilePath);
                if (!lines.isEmpty()) {
                    String[] parts = lines.get(0).split(":");
                    if (parts.length == 2) {
                        int currentLeaderNodeId = Integer.parseInt(parts[0]);
                        long timestamp = Long.parseLong(parts[1]);
                        
                        // Check if the current leader has timed out
                        if (System.currentTimeMillis() - timestamp <= leaderTimeoutMs) {
                            // Leader is still active
                            return currentLeaderNodeId == nodeId;
                        }
                    }
                }
            }
            
            // No leader or leader timed out, try to claim leadership
            String content = nodeId + ":" + System.currentTimeMillis();
            Files.write(leaderFilePath, content.getBytes());
            
            // Verify leadership was claimed by this node
            List<String> lines = Files.readAllLines(leaderFilePath);
            if (!lines.isEmpty()) {
                String[] parts = lines.get(0).split(":");
                if (parts.length == 2) {
                    int currentLeaderNodeId = Integer.parseInt(parts[0]);
                    return currentLeaderNodeId == nodeId;
                }
            }
            
            return false;
        } catch (IOException e) {
            LOGGER.warn("Failed to claim leadership", e);
            return false;
        }
    }
}
