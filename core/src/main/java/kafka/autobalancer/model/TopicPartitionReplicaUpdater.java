/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
