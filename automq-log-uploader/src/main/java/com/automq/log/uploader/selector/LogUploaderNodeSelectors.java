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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Utility methods providing built-in selector implementations.
 */
public final class LogUploaderNodeSelectors {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUploaderNodeSelectors.class);

    private LogUploaderNodeSelectors() {
    }

    public static LogUploaderNodeSelector staticSelector(boolean isPrimary) {
        return LogUploaderNodeSelector.staticSelector(isPrimary);
    }

    public static LogUploaderNodeSelector nodeIdSelector(int currentNodeId, int primaryNodeId) {
        return () -> currentNodeId == primaryNodeId;
    }

    public static LogUploaderNodeSelector supplierSelector(Supplier<Boolean> supplier) {
        Objects.requireNonNull(supplier, "supplier");
        return () -> Boolean.TRUE.equals(supplier.get());
    }

    public static LogUploaderNodeSelector fileLeaderElectionSelector(String leaderFilePath,
                                                                     int nodeId,
                                                                     long leaderTimeoutMs) {
        Path path = Paths.get(leaderFilePath);
        AtomicBoolean isLeader = new AtomicBoolean(false);

        Thread leaderThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    boolean claimed = attemptToClaimLeadership(path, nodeId, leaderTimeoutMs);
                    isLeader.set(claimed);
                    Thread.sleep(Math.max(leaderTimeoutMs / 2, 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOGGER.warn("File leader election failed", e);
                    isLeader.set(false);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }, "log-uploader-file-selector");
        leaderThread.setDaemon(true);
        leaderThread.start();

        return isLeader::get;
    }

    private static boolean attemptToClaimLeadership(Path leaderFilePath, int nodeId, long leaderTimeoutMs) throws IOException {
        Path parentDir = leaderFilePath.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }
        if (Files.exists(leaderFilePath)) {
            List<String> lines = Files.readAllLines(leaderFilePath, StandardCharsets.UTF_8);
            if (!lines.isEmpty()) {
                String[] parts = lines.get(0).split(":");
                if (parts.length == 2) {
                    int currentLeader = Integer.parseInt(parts[0]);
                    long ts = Long.parseLong(parts[1]);
                    if (System.currentTimeMillis() - ts <= leaderTimeoutMs) {
                        return currentLeader == nodeId;
                    }
                }
            }
        }
        String content = nodeId + ":" + System.currentTimeMillis();
        Files.write(leaderFilePath, content.getBytes(StandardCharsets.UTF_8));
        List<String> lines = Files.readAllLines(leaderFilePath, StandardCharsets.UTF_8);
        if (!lines.isEmpty()) {
            String[] parts = lines.get(0).split(":");
            return parts.length == 2 && Integer.parseInt(parts[0]) == nodeId;
        }
        return false;
    }
}
