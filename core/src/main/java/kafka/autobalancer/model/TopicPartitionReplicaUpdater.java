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

import kafka.autobalancer.common.RawMetricType;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicPartitionReplicaUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionReplicaUpdater.class);
    private static final List<RawMetricType> MANDATORY_METRICS = RawMetricType.partitionMetricTypes();
    private final Lock lock = new ReentrantLock();
    private final TopicPartitionReplica replica;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.replica = new TopicPartitionReplica(tp);
    }

    public boolean update(Map<RawMetricType, Double> metricsMap, long time) {
        if (!metricsMap.keySet().containsAll(MANDATORY_METRICS)) {
            LOGGER.error("Topic partition {} metrics sanity check failed, metrics is incomplete {}", replica.getTopicPartition(), metricsMap.keySet());
            return false;
        }

        lock.lock();
        try {
            if (time < this.replica.getTimestamp()) {
                LOGGER.warn("Outdated topic partition {} metrics at time {}, last updated time {}", replica.getTopicPartition(), time, this.replica.getTimestamp());
                return false;
            }
            this.replica.update(metricsMap, time);
        } finally {
            lock.unlock();
        }
        LOGGER.debug("Successfully updated on {} at time {}", this.replica.getTopicPartition(), this.replica.getTimestamp());
        return true;
    }

    public TopicPartitionReplica get() {
        return get(-1);
    }

    public TopicPartitionReplica get(long timeSince) {
        TopicPartitionReplica replica;
        lock.lock();
        try {
            if (this.replica.getTimestamp() < timeSince) {
                LOGGER.debug("Topic partition {} metrics is out of sync, expected earliest time: {}, actual: {}",
                        this.replica.getTopicPartition(), timeSince, this.replica.getTimestamp());
                return null;
            }
            replica = new TopicPartitionReplica(this.replica);
        } finally {
            lock.unlock();
        }
        replica.deriveLoads();
        return replica;
    }

    public TopicPartition topicPartition() {
        return this.replica.getTopicPartition();
    }
}
