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
