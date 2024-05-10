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

import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.model.samples.AbstractTimeWindowSamples;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

public class TopicPartitionReplicaUpdater extends AbstractInstanceUpdater {
    private final TopicPartition tp;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.tp = tp;
    }

    public TopicPartition topicPartition() {
        return this.tp;
    }

    @Override
    protected boolean validateMetrics(Map<Byte, Double> metricsMap) {
        return metricsMap.keySet().containsAll(RawMetricTypes.PARTITION_METRICS);
    }

    @Override
    protected String name() {
        return tp.toString();
    }

    @Override
    protected boolean isValidInstance() {
        return true;
    }

    @Override
    protected AbstractInstance createInstance() {
        TopicPartitionReplica replica = new TopicPartitionReplica(tp, timestamp);
        processRawMetrics(replica);
        return replica;
    }

    protected void processRawMetrics(TopicPartitionReplica replica) {
        for (Map.Entry<Byte, AbstractTimeWindowSamples> entry : metricSampleMap.entrySet()) {
            byte metricType = entry.getKey();
            AbstractTimeWindowSamples samples = entry.getValue();
            if (!RawMetricTypes.PARTITION_METRICS.contains(metricType)) {
                continue;
            }
            switch (metricType) {
                case RawMetricTypes.PARTITION_BYTES_IN:
                    replica.setLoad(Resource.NW_IN, samples.ofLoad());
                    break;
                case RawMetricTypes.PARTITION_BYTES_OUT:
                    replica.setLoad(Resource.NW_OUT, samples.ofLoad());
                    break;
                default:
                    break;
            }
        }
    }

    public static class TopicPartitionReplica extends AbstractInstance {
        private final TopicPartition tp;

        public TopicPartitionReplica(TopicPartition tp, long timestamp) {
            super(timestamp);
            this.tp = tp;
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
            TopicPartitionReplica replica = new TopicPartitionReplica(tp, timestamp);
            replica.copyLoads(this);
            return replica;
        }

        @Override
        public String toString() {
            return "{TopicPartition=" + tp +
                    ", " + super.toString() +
                    "}";
        }
    }
}
