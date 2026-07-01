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

package org.apache.kafka.tools.automq.cost;

import java.util.Collections;
import java.util.Map;

/**
 * Holds the storage and metadata facts collected from a live AutoMQ cluster.
 * <p>
 * Instances are created by {@code S3CostEstimatorCommand} after querying the
 * Admin API, and are then handed to {@link CostCalculator} for cost computation.
 * <p>
 * Note: {@code totalLogBytes} is derived from {@code describeLogDirs()} which
 * reflects data currently held in local WAL directories. In a fully diskless
 * AutoMQ deployment the bulk of data lives in S3, so this value is used as an
 * approximation. The printed report includes a disclaimer about this limitation.
 */
public final class ClusterStorageInfo {

    /** Cluster identifier returned by the Admin API. */
    public final String clusterId;

    /** Number of brokers currently registered in the cluster. */
    public final int brokerCount;

    /** Total number of user-visible topics (internal topics excluded). */
    public final int topicCount;

    /** Total number of partitions across all user-visible topics. */
    public final int partitionCount;

    /**
     * Sum of replica log sizes in bytes reported by {@code describeLogDirs()}.
     * This is an approximation of the logical data volume.
     */
    public final long totalLogBytes;

    /**
     * Per-topic sum of replica log sizes in bytes.
     * Key: topic name. Value: total bytes across all partitions of that topic.
     * Empty when per-topic breakdown was not requested.
     */
    public final Map<String, Long> topicBytes;

    private ClusterStorageInfo(Builder builder) {
        this.clusterId = builder.clusterId;
        this.brokerCount = builder.brokerCount;
        this.topicCount = builder.topicCount;
        this.partitionCount = builder.partitionCount;
        this.totalLogBytes = builder.totalLogBytes;
        this.topicBytes = Collections.unmodifiableMap(builder.topicBytes);
    }

    /** Returns a new builder for {@link ClusterStorageInfo}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link ClusterStorageInfo}. */
    public static final class Builder {
        private String clusterId = "unknown";
        private int brokerCount;
        private int topicCount;
        private int partitionCount;
        private long totalLogBytes;
        private Map<String, Long> topicBytes = Collections.emptyMap();

        private Builder() {
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder brokerCount(int brokerCount) {
            this.brokerCount = brokerCount;
            return this;
        }

        public Builder topicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder totalLogBytes(long totalLogBytes) {
            this.totalLogBytes = totalLogBytes;
            return this;
        }

        public Builder topicBytes(Map<String, Long> topicBytes) {
            this.topicBytes = topicBytes;
            return this;
        }

        public ClusterStorageInfo build() {
            return new ClusterStorageInfo(this);
        }
    }
}
