/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model;

import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.samples.Samples;
import kafka.autobalancer.model.samples.SingleValueSamples;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicPartitionReplicaUpdater extends AbstractInstanceUpdater {
    private final TopicPartition tp;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.tp = tp;
    }

    public TopicPartition topicPartition() {
        return this.tp;
    }

    @Override
    protected String name() {
        return tp.toString();
    }

    @Override
    protected AbstractInstance createInstance(boolean metricsOutOfDate) {
        TopicPartitionReplica replica = new TopicPartitionReplica(tp, lastUpdateTimestamp, metricVersion, metricsOutOfDate);
        processRawMetrics(replica);
        return replica;
    }

    @Override
    protected Set<Byte> requiredMetrics() {
        return metricVersion.requiredPartitionMetrics();
    }

    public void setMetricVersion(MetricVersion metricVersion) {
        this.metricVersion = metricVersion;
    }

    protected void processRawMetrics(TopicPartitionReplica replica) {
        for (Map.Entry<Byte, Samples> entry : metricSampleMap.entrySet()) {
            byte metricType = entry.getKey();
            Samples samples = entry.getValue();
            switch (metricType) {
                case RawMetricTypes.PARTITION_BYTES_IN:
                    replica.setLoad(Resource.NW_IN, samples.value(), samples.isTrusted());
                    break;
                case RawMetricTypes.PARTITION_BYTES_OUT:
                    replica.setLoad(Resource.NW_OUT, samples.value(), samples.isTrusted());
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    protected boolean processMetric(byte metricType, double value) {
        return true;
    }

    @Override
    protected Samples createSample(byte metricType) {
        return new SingleValueSamples();
    }

    public static class TopicPartitionReplica extends AbstractInstance {
        private final TopicPartition tp;

        public TopicPartitionReplica(TopicPartition tp, long timestamp, MetricVersion metricVersion, boolean metricsOutOfDate) {
            super(timestamp, metricVersion, metricsOutOfDate);
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
                    ", outOfDate=" + metricsOutOfDate +
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public AbstractInstance copy() {
            TopicPartitionReplica replica = new TopicPartitionReplica(tp, timestamp, metricVersion, metricsOutOfDate);
            replica.copyLoads(this);
            return replica;
        }

        @Override
        public String toString() {
            return "TopicPartitionReplica{" +
                    "tp=" + tp +
                    ", outOfDate=" + metricsOutOfDate +
                    ", " + super.toString() +
                    "}";
        }
    }
}
