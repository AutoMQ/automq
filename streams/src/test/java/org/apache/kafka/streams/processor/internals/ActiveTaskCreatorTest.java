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
package org.apache.kafka.streams.processor.internals;

import java.io.File;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockClientSupplier;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

@RunWith(EasyMockRunner.class)
public class ActiveTaskCreatorTest {

    @Mock(type = MockType.NICE)
    private InternalTopologyBuilder builder;
    @Mock(type = MockType.NICE)
    private StreamsConfig config;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> consumer;
    @Mock(type = MockType.NICE)
    private Admin adminClient;

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "clientId", StreamsConfig.METRICS_LATEST);

    private ActiveTaskCreator activeTaskCreator;

    @Test
    public void shouldConstructProducerMetricsWithoutEOS() {
        expect(config.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("appId");
        expect(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.AT_LEAST_ONCE);
        expect(config.getProducerConfigs(anyString())).andReturn(Collections.emptyMap());
        replay(config);

        activeTaskCreator = new ActiveTaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changeLogReader,
            new ThreadCache(new LogContext(), 0L, streamsMetrics),
            new MockTime(),
            mockClientSupplier,
            "threadId",
            new LogContext().logger(ActiveTaskCreator.class)
        );

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());

        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName, testMetric);
        final Map<MetricName, Metric> producerMetrics = activeTaskCreator.producerMetrics();
        assertEquals(testMetricName, producerMetrics.get(testMetricName).metricName());
    }

    @Test
    public void shouldConstructProducerMetricsWithEOS() {
        final TaskId taskId = new TaskId(0, 0);
        final ProcessorTopology topology = mock(ProcessorTopology.class);

        expect(config.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("appId");
        expect(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.EXACTLY_ONCE);
        expect(config.getLong(anyString())).andReturn(0L);
        expect(config.getInt(anyString())).andReturn(0);
        expect(config.getProducerConfigs(anyString())).andReturn(new HashMap<>());
        expect(builder.buildSubtopology(taskId.topicGroupId)).andReturn(topology);
        expect(stateDirectory.directoryForTask(taskId)).andReturn(new File(taskId.toString()));
        expect(topology.storeToChangelogTopic()).andReturn(Collections.emptyMap());
        expect(topology.source("topic")).andReturn(mock(SourceNode.class));
        expect(topology.globalStateStores()).andReturn(Collections.emptyList());
        replay(config, builder, stateDirectory, topology);

        mockClientSupplier.setApplicationIdForProducer("appId");

        activeTaskCreator = new ActiveTaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changeLogReader,
            new ThreadCache(new LogContext(), 0L, streamsMetrics),
            new MockTime(),
            mockClientSupplier,
            "threadId",
            new LogContext().logger(ActiveTaskCreator.class)
        );

        activeTaskCreator.createTasks(
            new MockConsumer<>(OffsetResetStrategy.NONE),
            mkMap(mkEntry(new TaskId(0, 0), Collections.singleton(new TopicPartition("topic", 0)))));

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());

        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName, testMetric);
        final Map<MetricName, Metric> producerMetrics = activeTaskCreator.producerMetrics();
        assertEquals(testMetricName, producerMetrics.get(testMetricName).metricName());
    }
}
