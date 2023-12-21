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
package org.apache.kafka.coordinator.group.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.METRICS_GROUP;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_COMMITS_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_EXPIRED_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertGaugeValue;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertMetricsForTypeEqual;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.metricName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupCoordinatorMetricsTest {

    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();

        HashSet<org.apache.kafka.common.MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
            metrics.metricName("offset-commit-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-commit-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-expiration-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-expiration-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-deletion-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("offset-deletion-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-completed-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-completed-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("consumer-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("consumer-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("protocol", "classic")),
            metrics.metricName(
                "group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("protocol", "consumer")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("state", "empty")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("state", "assigning")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("state", "reconciling")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("state", "stable")),
            metrics.metricName(
                "consumer-group-count",
                GroupCoordinatorMetrics.METRICS_GROUP,
                Collections.singletonMap("state", "dead"))
        ));

        try {
            try (GroupCoordinatorMetrics ignored = new GroupCoordinatorMetrics(registry, metrics)) {
                HashSet<String> expectedRegistry = new HashSet<>(Arrays.asList(
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumOffsets",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroups",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsPreparingRebalance",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsCompletingRebalance",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsStable",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsDead",
                    "kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsEmpty"
                ));

                assertMetricsForTypeEqual(registry, "kafka.coordinator.group", expectedRegistry);
                expectedMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName)));
            }
            assertMetricsForTypeEqual(registry, "kafka.coordinator.group", Collections.emptySet());
            expectedMetrics.forEach(metricName -> assertFalse(metrics.metrics().containsKey(metricName)));
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void aggregateShards() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        SnapshotRegistry snapshotRegistry0 = new SnapshotRegistry(new LogContext());
        SnapshotRegistry snapshotRegistry1 = new SnapshotRegistry(new LogContext());
        TopicPartition tp0 = new TopicPartition("__consumer_offsets", 0);
        TopicPartition tp1 = new TopicPartition("__consumer_offsets", 1);
        GroupCoordinatorMetricsShard shard0 = coordinatorMetrics.newMetricsShard(snapshotRegistry0, tp0);
        GroupCoordinatorMetricsShard shard1 = coordinatorMetrics.newMetricsShard(snapshotRegistry1, tp1);
        coordinatorMetrics.activateMetricsShard(shard0);
        coordinatorMetrics.activateMetricsShard(shard1);

        IntStream.range(0, 5).forEach(__ -> shard0.incrementNumClassicGroups(ClassicGroupState.PREPARING_REBALANCE));
        IntStream.range(0, 1).forEach(__ -> shard0.decrementNumClassicGroups(ClassicGroupState.COMPLETING_REBALANCE));
        IntStream.range(0, 5).forEach(__ -> shard1.incrementNumClassicGroups(ClassicGroupState.STABLE));
        IntStream.range(0, 4).forEach(__ -> shard1.incrementNumClassicGroups(ClassicGroupState.DEAD));
        IntStream.range(0, 4).forEach(__ -> shard1.decrementNumClassicGroups(ClassicGroupState.EMPTY));

        IntStream.range(0, 5).forEach(__ -> shard0.incrementNumConsumerGroups(ConsumerGroupState.ASSIGNING));
        IntStream.range(0, 5).forEach(__ -> shard1.incrementNumConsumerGroups(ConsumerGroupState.RECONCILING));
        IntStream.range(0, 3).forEach(__ -> shard1.decrementNumConsumerGroups(ConsumerGroupState.DEAD));

        IntStream.range(0, 6).forEach(__ -> shard0.incrementNumOffsets());
        IntStream.range(0, 2).forEach(__ -> shard1.incrementNumOffsets());
        IntStream.range(0, 1).forEach(__ -> shard1.decrementNumOffsets());

        assertEquals(4, shard0.numClassicGroups());
        assertEquals(5, shard1.numClassicGroups());
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroups"), 9);
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Collections.singletonMap("protocol", "classic")),
            9
        );

        snapshotRegistry0.getOrCreateSnapshot(1000);
        snapshotRegistry1.getOrCreateSnapshot(1500);
        shard0.commitUpTo(1000);
        shard1.commitUpTo(1500);

        assertEquals(5, shard0.numConsumerGroups());
        assertEquals(2, shard1.numConsumerGroups());
        assertEquals(6, shard0.numOffsets());
        assertEquals(1, shard1.numOffsets());
        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", METRICS_GROUP, Collections.singletonMap("protocol", "consumer")),
            7
        );
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumOffsets"), 7);
    }

    @Test
    public void testGlobalSensors() {
        MetricsRegistry registry = new MetricsRegistry();
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(
            new SnapshotRegistry(new LogContext()), new TopicPartition("__consumer_offsets", 0)
        );

        shard.record(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, 10);
        assertMetricValue(metrics, metrics.metricName("group-completed-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP), 1.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("group-completed-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP), 10);

        shard.record(OFFSET_COMMITS_SENSOR_NAME, 20);
        assertMetricValue(metrics, metrics.metricName("offset-commit-rate", GroupCoordinatorMetrics.METRICS_GROUP), 2.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("offset-commit-count", GroupCoordinatorMetrics.METRICS_GROUP), 20);

        shard.record(OFFSET_EXPIRED_SENSOR_NAME, 30);
        assertMetricValue(metrics, metrics.metricName("offset-expiration-rate", GroupCoordinatorMetrics.METRICS_GROUP), 1.0);
        assertMetricValue(metrics, metrics.metricName("offset-expiration-count", GroupCoordinatorMetrics.METRICS_GROUP), 30);

        shard.record(CLASSIC_GROUP_REBALANCES_SENSOR_NAME, 40);
        assertMetricValue(metrics, metrics.metricName("group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP), 4.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP), 40);

        shard.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, 50);
        assertMetricValue(metrics, metrics.metricName("consumer-group-rebalance-rate", GroupCoordinatorMetrics.METRICS_GROUP), 5.0 / 3.0);
        assertMetricValue(metrics, metrics.metricName("consumer-group-rebalance-count", GroupCoordinatorMetrics.METRICS_GROUP), 50);
    }

    private void assertMetricValue(Metrics metrics, MetricName metricName, double val) {
        assertEquals(val, metrics.metric(metricName).metricValue());
    }
}
