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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockClientSupplier;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThrows;

@RunWith(EasyMockRunner.class)
public class ActiveTaskCreatorTest {

    @Mock(type = MockType.NICE)
    private InternalTopologyBuilder builder;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "clientId", StreamsConfig.METRICS_LATEST);
    private final Map<String, Object> properties = mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    );
    final UUID uuid = UUID.randomUUID();

    private ActiveTaskCreator activeTaskCreator;



    // non-EOS test

    // functional test

    @Test
    public void shouldConstructProducerMetricsWithEosDisabled() {
        shouldConstructThreadProducerMetric();
    }

    @Test
    public void shouldConstructClientIdWithEosDisabled() {
        createTasks();

        final Set<String> clientIds = activeTaskCreator.producerClientIds();

        assertThat(clientIds, is(Collections.singleton("clientId-StreamThread-0-producer")));
    }

    @Test
    public void shouldCloseThreadProducerIfEosDisabled() {
        createTasks();

        activeTaskCreator.closeThreadProducerIfNeeded();

        assertThat(mockClientSupplier.producers.get(0).closed(), is(true));
    }

    @Test
    public void shouldNoOpCloseTaskProducerIfEosDisabled() {
        createTasks();

        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 1));

        assertThat(mockClientSupplier.producers.get(0).closed(), is(false));
    }

    // error handling

    @Test
    public void shouldFailOnStreamsProducerPerTaskIfEosDisabled() {
        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> activeTaskCreator.streamsProducerForTask(null)
        );

        assertThat(thrown.getMessage(), is("Producer per thread is used."));
    }

    @Test
    public void shouldFailOnGetThreadProducerIfEosDisabled() {
        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            activeTaskCreator::threadProducer
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnErrorCloseThreadProducerIfEosDisabled() {
        createTasks();
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            activeTaskCreator::closeThreadProducerIfNeeded
        );

        assertThat(thrown.getMessage(), is("Thread producer encounter error trying to close."));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }



    // eos-alpha test

    // functional test

    @Test
    public void shouldReturnStreamsProducerPerTaskIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        shouldReturnStreamsProducerPerTask();
    }

    @Test
    public void shouldConstructProducerMetricsWithEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        shouldConstructProducerMetricsPerTask();
    }

    @Test
    public void shouldConstructClientIdWithEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        final Set<String> clientIds = activeTaskCreator.producerClientIds();

        assertThat(clientIds, is(mkSet("clientId-StreamThread-0-0_0-producer", "clientId-StreamThread-0-0_1-producer")));
    }

    @Test
    public void shouldNoOpCloseThreadProducerIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        activeTaskCreator.closeThreadProducerIfNeeded();

        assertThat(mockClientSupplier.producers.get(0).closed(), is(false));
        assertThat(mockClientSupplier.producers.get(1).closed(), is(false));
    }

    @Test
    public void shouldCloseTaskProducersIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 1));
        // should no-op unknown task
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 2));

        assertThat(mockClientSupplier.producers.get(0).closed(), is(true));
        assertThat(mockClientSupplier.producers.get(1).closed(), is(true));

        // should not throw because producer should be removed
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
    }

    // error handling

    @Test
    public void shouldFailForUnknownTaskOnStreamsProducerPerTaskIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        {
            final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> activeTaskCreator.streamsProducerForTask(null)
            );

            assertThat(thrown.getMessage(), is("Unknown TaskId: null"));
        }
        {
            final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> activeTaskCreator.streamsProducerForTask(new TaskId(0, 2))
            );

            assertThat(thrown.getMessage(), is("Unknown TaskId: 0_2"));
        }
    }

    @Test
    public void shouldFailOnGetThreadProducerIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            activeTaskCreator::threadProducer
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnErrorCloseTaskProducerIfEosAlphaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0))
        );

        assertThat(thrown.getMessage(), is("[0_0] task producer encounter error trying to close."));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));

        // should not throw again because producer should be removed
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
    }



    // eos-beta test

    // functional test

    @Test
    public void shouldReturnThreadProducerIfEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final StreamsProducer threadProducer = activeTaskCreator.threadProducer();

        assertThat(mockClientSupplier.producers.size(), is(1));
        assertThat(threadProducer.kafkaProducer(), is(mockClientSupplier.producers.get(0)));
    }

    @Test
    public void shouldConstructProducerMetricsWithEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");

        shouldConstructThreadProducerMetric();
    }

    @Test
    public void shouldConstructClientIdWithEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        final Set<String> clientIds = activeTaskCreator.producerClientIds();

        assertThat(clientIds, is(Collections.singleton("clientId-StreamThread-0-producer")));
    }

    @Test
    public void shouldCloseThreadProducerIfEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        activeTaskCreator.closeThreadProducerIfNeeded();

        assertThat(mockClientSupplier.producers.get(0).closed(), is(true));
    }

    @Test
    public void shouldNoOpCloseTaskProducerIfEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 1));

        assertThat(mockClientSupplier.producers.get(0).closed(), is(false));
    }

    // error handling

    @Test
    public void shouldFailOnStreamsProducerPerTaskIfEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> activeTaskCreator.streamsProducerForTask(null)
        );

        assertThat(thrown.getMessage(), is("Producer per thread is used."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnErrorCloseThreadProducerIfEosBetaEnabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            activeTaskCreator::closeThreadProducerIfNeeded
        );

        assertThat(thrown.getMessage(), is("Thread producer encounter error trying to close."));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }

    private void shouldReturnStreamsProducerPerTask() {
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final StreamsProducer streamsProducer1 = activeTaskCreator.streamsProducerForTask(new TaskId(0, 0));
        final StreamsProducer streamsProducer2 = activeTaskCreator.streamsProducerForTask(new TaskId(0, 1));

        assertThat(streamsProducer1, not(is(streamsProducer2)));
    }

    private void shouldConstructProducerMetricsPerTask() {
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final MetricName testMetricName1 = new MetricName("test_metric_1", "", "", new HashMap<>());
        final Metric testMetric1 = new KafkaMetric(
            new Object(),
            testMetricName1,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName1, testMetric1);
        final MetricName testMetricName2 = new MetricName("test_metric_2", "", "", new HashMap<>());
        final Metric testMetric2 = new KafkaMetric(
            new Object(),
            testMetricName2,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName2, testMetric2);

        final Map<MetricName, Metric> producerMetrics = activeTaskCreator.producerMetrics();

        assertThat(producerMetrics, is(mkMap(mkEntry(testMetricName1, testMetric1), mkEntry(testMetricName2, testMetric2))));
    }

    private void shouldConstructThreadProducerMetric() {
        createTasks();

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName, testMetric);
        assertThat(mockClientSupplier.producers.size(), is(1));

        final Map<MetricName, Metric> producerMetrics = activeTaskCreator.producerMetrics();

        assertThat(producerMetrics.size(), is(1));
        assertThat(producerMetrics.get(testMetricName), is(testMetric));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createTasks() {
        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);

        final ProcessorTopology topology = mock(ProcessorTopology.class);
        final SourceNode sourceNode = mock(SourceNode.class);

        reset(builder, stateDirectory);
        expect(builder.buildSubtopology(0)).andReturn(topology).anyTimes();
        expect(stateDirectory.directoryForTask(task00)).andReturn(mock(File.class));
        expect(stateDirectory.checkpointFileFor(task00)).andReturn(mock(File.class));
        expect(stateDirectory.directoryForTask(task01)).andReturn(mock(File.class));
        expect(stateDirectory.checkpointFileFor(task01)).andReturn(mock(File.class));
        expect(topology.storeToChangelogTopic()).andReturn(Collections.emptyMap()).anyTimes();
        expect(topology.source("topic")).andReturn(sourceNode).anyTimes();
        expect(sourceNode.getTimestampExtractor()).andReturn(mock(TimestampExtractor.class)).anyTimes();
        expect(topology.globalStateStores()).andReturn(Collections.emptyList()).anyTimes();
        replay(builder, stateDirectory, topology, sourceNode);

        activeTaskCreator = new ActiveTaskCreator(
            builder,
            new StreamsConfig(properties),
            streamsMetrics,
            stateDirectory,
            changeLogReader,
            new ThreadCache(new LogContext(), 0L, streamsMetrics),
            new MockTime(),
            mockClientSupplier,
            "clientId-StreamThread-0",
            uuid,
            new LogContext().logger(ActiveTaskCreator.class)
        );

        assertThat(
            activeTaskCreator.createTasks(
                null,
                mkMap(
                    mkEntry(task00, Collections.singleton(new TopicPartition("topic", 0))),
                    mkEntry(task01, Collections.singleton(new TopicPartition("topic", 1)))
                )
            ).stream().map(Task::id).collect(Collectors.toSet()),
            equalTo(mkSet(task00, task01))
        );
    }
}
