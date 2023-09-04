/*
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

package kafka.autobalancer.model;

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import kafka.autobalancer.metricsreporter.metric.RawMetricType;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicPartitionReplicaUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionReplicaUpdater.class);
    private final Lock lock = new ReentrantLock();
    private final TopicPartitionReplica replica;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.replica = new TopicPartitionReplica(tp);
    }

    public static class TopicPartitionReplica {
        private final TopicPartition tp;
        private final double[] replicaLoad = new double[Resource.cachedValues().size()];
        private final Set<Resource> resources = new HashSet<>();
        private long timestamp;

        public TopicPartitionReplica(TopicPartition tp) {
            this.tp = tp;
        }

        public TopicPartitionReplica(TopicPartitionReplica other) {
            this.tp = new TopicPartition(other.tp.topic(), other.tp.partition());
            System.arraycopy(other.replicaLoad, 0, this.replicaLoad, 0, other.replicaLoad.length);
            this.resources.addAll(other.resources);
            this.timestamp = other.timestamp;
        }

        public Set<Resource> getResources() {
            return this.resources;
        }

        public void setLoad(Resource resource, double value) {
            this.resources.add(resource);
            this.replicaLoad[resource.id()] = value;
        }

        public double load(Resource resource) {
            if (!this.resources.contains(resource)) {
                return 0.0;
            }
            return this.replicaLoad[resource.id()];
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public TopicPartition getTopicPartition() {
            return tp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicPartitionReplica replica = (TopicPartitionReplica) o;
            return tp.equals(replica.tp);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tp);
        }

        @Override
        public String toString() {
            return "TopicPartitionReplica{" +
                    "tp=" + tp +
                    ", replicaLoad=" + Arrays.toString(replicaLoad) +
                    '}';
        }
    }

    public boolean update(AutoBalancerMetrics metrics) {
        if (metrics.metricClassId() != AutoBalancerMetrics.MetricClassId.PARTITION_METRIC) {
            LOGGER.error("Mismatched metrics type {} for broker", metrics.metricClassId());
            return false;
        }

        if (!MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics)) {
            LOGGER.error("Topic partition metrics sanity check failed, metrics is incomplete {}", metrics);
            return false;
        }

        lock.lock();
        try {
            if (metrics.time() < this.replica.getTimestamp()) {
                LOGGER.warn("Outdated metrics at time {}, last updated time {}", metrics.time(), this.replica.getTimestamp());
                return false;
            }
            for (Map.Entry<RawMetricType, Double> entry : metrics.getMetricTypeValueMap().entrySet()) {
                switch (entry.getKey()) {
                    case TOPIC_PARTITION_BYTES_IN:
                        this.replica.setLoad(Resource.NW_IN, entry.getValue());
                        break;
                    case TOPIC_PARTITION_BYTES_OUT:
                        this.replica.setLoad(Resource.NW_OUT, entry.getValue());
                        break;
                    case PARTITION_SIZE:
                        // simply update the timestamp
                        break;
                    default:
                        LOGGER.error("Unsupported broker metrics type {}", entry.getKey());
                }
            }
            this.replica.setTimestamp(metrics.time());
        } finally {
            lock.unlock();
        }
        LOGGER.debug("Successfully updated on {} at time {}", this.replica.getTopicPartition(), this.replica.getTimestamp());
        return true;
    }

    public TopicPartitionReplica get() {
        TopicPartitionReplica replica;
        lock.lock();
        try {
            replica = new TopicPartitionReplica(this.replica);
        } finally {
            lock.unlock();
        }
        return replica;
    }

    public TopicPartitionReplica get(long timeSince) {
        TopicPartitionReplica replica;
        lock.lock();
        try {
            if (this.replica.timestamp < timeSince) {
                LOGGER.warn("Topic partition {} metrics is out of sync, expected earliest time: {}, actual: {}",
                        this.replica.getTopicPartition(), timeSince, this.replica.timestamp);
                return null;
            }
            replica = new TopicPartitionReplica(this.replica);
        } finally {
            lock.unlock();
        }
        return replica;
    }

    public TopicPartition topicPartition() {
        return this.replica.getTopicPartition();
    }
}
