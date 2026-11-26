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
import java.util.concurrent.TimeUnit;

public class TopicPartitionReplicaUpdater extends AbstractInstanceUpdater {
    // Yammer partition rate meters tick every five seconds, even if reporting is configured faster.
    private static final long RECOVERY_LOAD_MIN_REPORT_SPAN_MS = TimeUnit.SECONDS.toMillis(5);
    private final TopicPartition tp;
    // Guarded by the updater lock and discarded with the replica. A newly opened rate meter can report
    // zero before its first tick; complete reports spanning a tick are required for optional balancing only.
    private long leaderChangeTimeMs = -1L;
    private int recoveryLoadReports;
    private long firstRecoveryLoadReportMs = -1L;
    private long lastRecoveryLoadReportMs = -1L;
    private int updatedLoadMetrics;

    public TopicPartitionReplicaUpdater(TopicPartition tp) {
        this.tp = tp;
    }

    public TopicPartition topicPartition() {
        return this.tp;
    }

    void markLeaderChange(long timestampMs) {
        lock.lock();
        try {
            leaderChangeTimeMs = timestampMs;
            recoveryLoadReports = 0;
            firstRecoveryLoadReportMs = -1L;
            lastRecoveryLoadReportMs = -1L;
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected String name() {
        return tp.toString();
    }

    @Override
    protected AbstractInstance createInstance(boolean metricsOutOfDate) {
        TopicPartitionReplica replica = new TopicPartitionReplica(tp, lastUpdateTimestamp, metricVersion,
            metricsOutOfDate, leaderChangeTimeMs, leaderChangeTimeMs < 0 || recoveryLoadReports >= 2
                && lastRecoveryLoadReportMs - firstRecoveryLoadReportMs >= RECOVERY_LOAD_MIN_REPORT_SPAN_MS);
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
    protected void update0(Iterable<Map.Entry<Byte, Double>> metricsMap, long timestamp) {
        updatedLoadMetrics = 0;
        super.update0(metricsMap, timestamp);
        // Use the existing report/controller wall-clock freshness domain. Old, duplicate and partial
        // reports cannot qualify a reopened partition. Do not change raw recovery-placement eligibility.
        if (leaderChangeTimeMs >= 0 && timestamp >= leaderChangeTimeMs
            && timestamp > lastRecoveryLoadReportMs && updatedLoadMetrics == 7) {
            if (recoveryLoadReports == 0) {
                firstRecoveryLoadReportMs = timestamp;
            }
            recoveryLoadReports = Math.min(2, recoveryLoadReports + 1);
            lastRecoveryLoadReportMs = timestamp;
        }
    }

    @Override
    protected boolean processMetric(byte metricType, double value) {
        if (metricType == RawMetricTypes.PARTITION_BYTES_IN) {
            updatedLoadMetrics |= 1;
        } else if (metricType == RawMetricTypes.PARTITION_BYTES_OUT) {
            updatedLoadMetrics |= 2;
        } else if (metricType == RawMetricTypes.PARTITION_SIZE) {
            updatedLoadMetrics |= 4;
        }
        return true;
    }

    @Override
    protected Samples createSample(byte metricType) {
        return new SingleValueSamples();
    }

    public static class TopicPartitionReplica extends AbstractInstance {
        private final TopicPartition tp;
        private final long leaderChangeTimeMs;
        private final boolean recoveryLoadReady;

        public TopicPartitionReplica(TopicPartition tp, long timestamp, MetricVersion metricVersion, boolean metricsOutOfDate) {
            this(tp, timestamp, metricVersion, metricsOutOfDate, -1L, true);
        }

        private TopicPartitionReplica(TopicPartition tp, long timestamp, MetricVersion metricVersion,
            boolean metricsOutOfDate, long leaderChangeTimeMs, boolean recoveryLoadReady) {
            super(timestamp, metricVersion, metricsOutOfDate);
            this.tp = tp;
            this.leaderChangeTimeMs = leaderChangeTimeMs;
            this.recoveryLoadReady = recoveryLoadReady;
        }

        public TopicPartition getTopicPartition() {
            return tp;
        }

        /**
         * Returns whether distinct post-handoff reports span a rate-meter tick for optional balancing.
         * Raw metric freshness and mandatory recovery placement are intentionally unaffected.
         */
        public boolean isRecoveryLoadReady() {
            return recoveryLoadReady;
        }

        /** Returns controller-local handoff observation time, or -1 for initial metadata; not durable health state. */
        public long leaderChangeTimeMs() {
            return leaderChangeTimeMs;
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
            TopicPartitionReplica replica = new TopicPartitionReplica(tp, timestamp, metricVersion, metricsOutOfDate,
                leaderChangeTimeMs, recoveryLoadReady);
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
