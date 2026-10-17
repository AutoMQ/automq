/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.runtime;

import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-wide data-path monitor hook.
 *
 * <p>OSS data-path code records generic runtime signals through this static facade. The default
 * monitor is a no-op, and downstream distributions may register a monitor during broker lifecycle
 * startup to collect or react to these signals.</p>
 */
public final class DataPathMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPathMonitor.class);
    private static final OperationHandle NOOP_HANDLE = () -> { };
    private static final Monitor NOOP = new Monitor() {
    };
    private static final AtomicReference<Monitor> MONITOR = new AtomicReference<>(NOOP);

    private DataPathMonitor() {
    }

    public static void register(Monitor monitor) {
        MONITOR.set(monitor == null ? NOOP : monitor);
    }

    public static void unregister(Monitor monitor) {
        MONITOR.compareAndSet(monitor, NOOP);
    }

    public static OperationHandle recordPartitionCloseStarted(TopicPartition topicPartition) {
        try {
            return safeHandle(MONITOR.get().recordPartitionCloseStarted(topicPartition));
        } catch (Throwable e) {
            LOGGER.warn("Failed to record partition close started for {}", topicPartition, e);
            return NOOP_HANDLE;
        }
    }

    public static void recordAppendPending(CompletableFuture<?> appendFuture, long startNanos) {
        try {
            MONITOR.get().recordAppendPending(appendFuture, startNanos);
        } catch (Throwable e) {
            LOGGER.warn("Failed to record append pending", e);
        }
    }

    public static void recordLogWriteFailed(TopicPartition topicPartition) {
        try {
            MONITOR.get().recordLogWriteFailed(topicPartition);
        } catch (Throwable e) {
            LOGGER.warn("Failed to record log write failure for {}", topicPartition, e);
        }
    }

    private static OperationHandle safeHandle(OperationHandle handle) {
        OperationHandle safeHandle = handle == null ? NOOP_HANDLE : handle;
        return () -> {
            try {
                safeHandle.close();
            } catch (Throwable e) {
                LOGGER.warn("Failed to close data-path monitor operation handle", e);
            }
        };
    }

    public interface Monitor {
        default OperationHandle recordPartitionCloseStarted(TopicPartition topicPartition) {
            return () -> { };
        }

        default void recordAppendPending(CompletableFuture<?> appendFuture, long startNanos) {
        }

        default void recordLogWriteFailed(TopicPartition topicPartition) {
        }
    }

    public interface OperationHandle extends AutoCloseable {
        @Override
        void close();
    }
}
