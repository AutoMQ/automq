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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ShareFetchMetricsManagerTest {
    private static final double EPSILON = 0.0001;
    private final Time time = new MockTime(1, 0, 0);
    private ShareFetchMetricsManager shareFetchMetricsManager;
    private ShareFetchMetricsRegistry shareFetchMetricsRegistry;
    private Metrics metrics;

    @BeforeEach
    public void setup() {
        metrics = new Metrics(time);
        shareFetchMetricsRegistry = new ShareFetchMetricsRegistry(CONSUMER_SHARE_METRIC_GROUP_PREFIX);
        shareFetchMetricsManager = new ShareFetchMetricsManager(metrics, shareFetchMetricsRegistry);
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }

        shareFetchMetricsManager = null;
    }

    @Test
    public void testLatency() {
        shareFetchMetricsManager.recordLatency(101);
        time.sleep(metrics.config().timeWindowMs() + 1);
        shareFetchMetricsManager.recordLatency(155);

        assertEquals(155, (double) getMetric(shareFetchMetricsRegistry.fetchLatencyMax).metricValue(), EPSILON);
        assertEquals(128, (double) getMetric(shareFetchMetricsRegistry.fetchLatencyAvg).metricValue(), EPSILON);
    }

    @Test
    public void testBytesFetched() {
        shareFetchMetricsManager.recordBytesFetched(2);
        time.sleep(metrics.config().timeWindowMs() + 1);
        shareFetchMetricsManager.recordBytesFetched(10);

        assertEquals(10, (double) getMetric(shareFetchMetricsRegistry.fetchSizeMax).metricValue());
        assertEquals(6, (double) getMetric(shareFetchMetricsRegistry.fetchSizeAvg).metricValue(), EPSILON);
    }

    @Test
    public void testRecordsFetched() {
        shareFetchMetricsManager.recordRecordsFetched(7);
        time.sleep(metrics.config().timeWindowMs() + 1);
        shareFetchMetricsManager.recordRecordsFetched(9);

        assertEquals(9, (double) getMetric(shareFetchMetricsRegistry.recordsPerRequestMax).metricValue());
        assertEquals(8, (double) getMetric(shareFetchMetricsRegistry.recordsPerRequestAvg).metricValue(), EPSILON);
    }

    private KafkaMetric getMetric(MetricNameTemplate name) {
        return metrics.metric(metrics.metricInstance(name));
    }
}