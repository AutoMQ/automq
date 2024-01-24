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
import kafka.autobalancer.common.types.RawMetricTypes;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

public class TopicPartitionReplicaUpdater extends AbstractInstanceUpdater {
    private final TopicPartitionReplica replica;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.replica = new TopicPartitionReplica(tp);
    }

    public TopicPartition topicPartition() {
        return this.replica.getTopicPartition();
    }

    @Override
    protected boolean validateMetrics(Map<Byte, Double> metricsMap) {
        return metricsMap.keySet().containsAll(RawMetricTypes.partitionMetrics());
    }

    @Override
    protected AbstractInstance instance() {
        return replica;
    }

    @Override
    protected boolean isValidInstance() {
        return true;
    }

    public static class TopicPartitionReplica extends AbstractInstance {
        private final TopicPartition tp;

        public TopicPartitionReplica(TopicPartition tp) {
            this.tp = tp;
        }

        public TopicPartitionReplica(TopicPartitionReplica other) {
            super(other);
            this.tp = new TopicPartition(other.tp.topic(), other.tp.partition());
        }

        @Override
        public void processMetrics() {
            for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
                if (!RawMetricTypes.partitionMetrics().contains(entry.getKey())) {
                    continue;
                }
                switch (entry.getKey()) {
                    case RawMetricTypes.TOPIC_PARTITION_BYTES_IN:
                        this.setLoad(Resource.NW_IN, entry.getValue());
                        break;
                    case RawMetricTypes.TOPIC_PARTITION_BYTES_OUT:
                        this.setLoad(Resource.NW_OUT, entry.getValue());
                        break;
                    default:
                        break;
                }
            }
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

        public String shortString() {
            return "TopicPartitionReplica{" +
                    "tp=" + tp +
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public AbstractInstance copy() {
            return new TopicPartitionReplica(this);
        }

        @Override
        protected String name() {
            return tp.toString();
        }

        @Override
        public String toString() {
            return "{TopicPartition=" + tp +
                    ", " + super.toString() +
                    "}";
        }
    }
}
