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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG_0100_TO_24;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamTaskTest {

    private static final File BASE_DIR = TestUtils.tempDirectory();

    private final Serializer<Integer> intSerializer = Serdes.Integer().serializer();
    private final Serializer<byte[]> bytesSerializer = Serdes.ByteArray().serializer();
    private final Deserializer<Integer> intDeserializer = Serdes.Integer().deserializer();
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition partition1 = new TopicPartition(topic1, 1);
    private final TopicPartition partition2 = new TopicPartition(topic2, 1);
    private final Set<TopicPartition> partitions = mkSet(partition1, partition2);

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(new String[]{topic1}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(new String[]{topic2}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source3 = new MockSourceNode<Integer, Integer>(new String[]{topic2}, intDeserializer, intDeserializer) {
        @Override
        public void process(final Integer key, final Integer value) {
            throw new RuntimeException("KABOOM!");
        }

        @Override
        public void close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private final MockProcessorNode<Integer, Integer> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final String storeName = "store";
    private final StateStore stateStore = new MockKeyValueStore(storeName, false);
    private final TopicPartition changelogPartition = new TopicPartition("store-changelog", 0);
    private final Long offset = 543L;

    private final ProcessorTopology topology = withSources(
        asList(source1, source2, processorStreamTime, processorSystemTime),
        mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
    );

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private MockProducer<byte[], byte[]> producer;
    private final MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, Duration.ZERO, stateRestoreListener, new LogContext("stream-task-test ")) {
        @Override
        public Map<TopicPartition, Long> restoredOffsets() {
            return Collections.singletonMap(changelogPartition, offset);
        }
    };
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
    private final TaskId taskId00 = new TaskId(0, 0);
    private final MockTime time = new MockTime();
    private StateDirectory stateDirectory;
    private StreamTask task;
    private long punctuatedAt;

    private static final String APPLICATION_ID = "stream-task-test";
    private static final long DEFAULT_TIMESTAMP = 1000;

    private final Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    private static ProcessorTopology withRepartitionTopics(final List<ProcessorNode> processorNodes,
                                                           final Map<String, SourceNode> sourcesByTopic,
                                                           final Set<String> repartitionTopics) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     repartitionTopics);
    }

    private static ProcessorTopology withSources(final List<ProcessorNode> processorNodes,
                                                 final Map<String, SourceNode> sourcesByTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptySet());
    }

    // Exposed to make it easier to create StreamTask config from other tests.
    static StreamsConfig createConfig(final boolean enableEoS) {
        final String canonicalPath;
        try {
            canonicalPath = BASE_DIR.getCanonicalPath();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, canonicalPath),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE),
            mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "100")
        )));
    }

    @Before
    public void setup() {
        consumer.assign(asList(partition1, partition2));
        stateDirectory = new StateDirectory(createConfig(false), new MockTime(), true);
    }

    @After
    public void cleanup() throws IOException {
        try {
            if (task != null) {
                try {
                    task.close(true, false);
                } catch (final Exception e) {
                    // swallow
                }
            }
        } finally {
            Utils.delete(BASE_DIR);
        }
    }

    @Test
    public void shouldHandleInitTransactionsTimeoutExceptionOnCreation() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        try {
            new StreamTask(
                taskId00,
                partitions,
                topology,
                consumer,
                changelogReader,
                createConfig(true),
                streamsMetrics,
                stateDirectory,
                null,
                time,
                () -> producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                    @Override
                    public void initTransactions() {
                        throw new TimeoutException("test");
                    }
                },
                null
            );
            fail("Expected an exception");
        } catch (final StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            assertThat(
                expected.getMessage(),
                is("stream-thread [" + Thread.currentThread().getName() + "] task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            assertEquals(expected.getCause().getClass(), TimeoutException.class);
            assertThat(expected.getCause().getMessage(), is("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    @Test
    public void shouldHandleInitTransactionsTimeoutExceptionOnResume() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        final AtomicBoolean timeOut = new AtomicBoolean(false);

        final StreamTask testTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                @Override
                public void initTransactions() {
                    if (timeOut.get()) {
                        throw new TimeoutException("test");
                    } else {
                        super.initTransactions();
                    }
                }
            },
            null
        );
        testTask.initializeTopology();
        testTask.suspend();
        timeOut.set(true);
        try {
            testTask.resume();
            fail("Expected an exception");
        } catch (final StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            assertThat(
                expected.getMessage(),
                is("stream-thread [" + Thread.currentThread().getName() + "] task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            assertEquals(expected.getCause().getClass(), TimeoutException.class);
            assertThat(expected.getCause().getMessage(), is("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    private void assertTimeoutErrorLog(final LogCaptureAppender appender) {

        final String expectedErrorLogMessage =
            "stream-thread [" + Thread.currentThread().getName() + "] task [0_0] Timeout exception caught when initializing transactions for task 0_0. " +
                "This might happen if the broker is slow to respond, if the network " +
                "connection to the broker was interrupted, or if similar circumstances arise. " +
                "You can increase producer parameter `max.block.ms` to increase this timeout.";

        final List<String> expectedError =
            appender
                .getEvents()
                .stream()
                .filter(event -> event.getMessage().equals(expectedErrorLogMessage))
                .map(LogCaptureAppender.Event::getLevel)
                .collect(Collectors.toList());
        assertThat(expectedError, is(singletonList("ERROR")));
    }

    @Test
    public void testProcessOrder() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
    }

    @Test
    public void testMetricsWithBuiltInMetricsVersion0100To24() {
        testMetrics(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void testMetricsWithBuiltInMetricsVersionLatest() {
        testMetrics(StreamsConfig.METRICS_LATEST);
    }

    private void testMetrics(final String builtInMetricsVersion) {
        task = createStatelessTask(createConfig(false), builtInMetricsVersion);

        assertNotNull(getMetric(
            "commit",
            "%s-latency-avg",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "commit",
            "%s-latency-max",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "commit",
            "%s-rate",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "commit",
            "%s-total",
            task.id().toString(),
            builtInMetricsVersion
        ));

        assertNotNull(getMetric(
            "enforced-processing",
            "%s-rate",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "enforced-processing",
            "%s-total",
            task.id().toString(),
            builtInMetricsVersion
        ));

        assertNotNull(getMetric(
            "record-lateness",
            "%s-avg",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "record-lateness",
            "%s-max",
            task.id().toString(),
            builtInMetricsVersion
        ));

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            testMetricsForBuiltInMetricsVersion0100To24();
        } else {
            testMetricsForBuiltInMetricsVersionLatest();
        }

        final String threadId = Thread.currentThread().getName();
        final JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        final String threadIdTag =
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? THREAD_ID_TAG : THREAD_ID_TAG_0100_TO_24;
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=stream-task-metrics,%s=%s,task-id=%s",
            threadIdTag,
            threadId,
            task.id.toString()
        )));
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=stream-task-metrics,%s=%s,task-id=all",
                threadIdTag,
                threadId
            )));
        }
    }

    private void testMetricsForBuiltInMetricsVersionLatest() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        assertNull(getMetric("commit", "%s-latency-avg", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-latency-max", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-rate", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-total", "all", builtInMetricsVersion));

        assertNotNull(getMetric("process", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("process", "%s-latency-max", task.id().toString(), builtInMetricsVersion));

        assertNotNull(getMetric("punctuate", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-latency-max", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-rate", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-total", task.id().toString(), builtInMetricsVersion));
    }

    private void testMetricsForBuiltInMetricsVersion0100To24() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_0100_TO_24;
        assertNotNull(getMetric("commit", "%s-latency-avg", "all", builtInMetricsVersion));
        assertNotNull(getMetric("commit", "%s-latency-max", "all", builtInMetricsVersion));
        assertNotNull(getMetric("commit", "%s-rate", "all", builtInMetricsVersion));

        assertNull(getMetric("process", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("process", "%s-latency-max", task.id().toString(), builtInMetricsVersion));

        assertNull(getMetric("punctuate", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-latency-max", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-rate", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-total", task.id().toString(), builtInMetricsVersion));
    }

    private KafkaMetric getMetric(final String operation,
                                  final String nameFormat,
                                  final String taskId,
                                  final String builtInMetricsVersion) {
        final String descriptionIsNotVerified = "";
        return metrics.metrics().get(metrics.metricName(
            String.format(nameFormat, operation),
            "stream-task-metrics",
            descriptionIsNotVerified,
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry(
                    StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? THREAD_ID_TAG
                        : THREAD_ID_TAG_0100_TO_24,
                    Thread.currentThread().getName()
                )
            )
        ));
    }

    @Test
    public void testPauseResume() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45),
            getConsumerRecord(partition2, 55),
            getConsumerRecord(partition2, 65)
        ));

        assertTrue(task.process());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40),
            getConsumerRecord(partition1, 50)
        ));

        assertEquals(2, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertEquals(0, consumer.paused().size());
    }

    @Test
    public void shouldPunctuateOnceStreamTimeAfterGap() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 142),
            getConsumerRecord(partition1, 155),
            getConsumerRecord(partition1, 160)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 145),
            getConsumerRecord(partition2, 159),
            getConsumerRecord(partition2, 161)
        ));

        // st: -1
        assertFalse(task.maybePunctuateStreamTime()); // punctuate at 20

        // st: 20
        assertTrue(task.process());
        assertEquals(7, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 25
        assertTrue(task.process());
        assertEquals(6, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 142
        // punctuate at 142
        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 145
        // only one punctuation after 100ms gap
        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 155
        // punctuate at 155
        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 159
        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 160, aligned at 0
        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 161
        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(4, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    @Test
    public void shouldRespectPunctuateCancellationStreamTime() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        assertFalse(task.maybePunctuateStreamTime());

        // st is now 20
        assertTrue(task.process());

        assertTrue(task.maybePunctuateStreamTime());

        // st is now 25
        assertTrue(task.process());

        assertFalse(task.maybePunctuateStreamTime());

        // st is now 30
        assertTrue(task.process());

        processorStreamTime.mockProcessor.scheduleCancellable.cancel();

        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    @Test
    public void shouldRespectPunctuateCancellationSystemTime() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.scheduleCancellable.cancel();
        time.sleep(10);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @Test
    public void shouldRespectCommitNeeded() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        assertFalse(task.commitNeeded());

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        assertTrue(task.process());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());

        assertTrue(task.maybePunctuateStreamTime());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());

        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldRestorePartitionTimeAfterRestartWithEosDisabled() {
        createTaskWithProcessAndCommit(false);

        assertEquals(DEFAULT_TIMESTAMP, task.decodeTimestamp(consumer.committed(Collections.singleton(partition1)).get(partition1).metadata()));
        // reset times here by creating a new task
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        task.initializeMetadata();
        assertEquals(DEFAULT_TIMESTAMP, task.partitionTime(partition1));
        assertEquals(DEFAULT_TIMESTAMP, task.streamTime());
    }

    @Test
    public void shouldRestorePartitionTimeAfterRestartWithEosEnabled() {
        createTaskWithProcessAndCommit(true);

        moveCommittedOffsetsFromProducerToConsumer(DEFAULT_TIMESTAMP);

        // reset times here by creating a new task
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);

        task.initializeMetadata();
        assertEquals(DEFAULT_TIMESTAMP, task.partitionTime(partition1));
        assertEquals(DEFAULT_TIMESTAMP, task.streamTime());
    }

    private void createTaskWithProcessAndCommit(final boolean eosEnabled) {
        task = createStatelessTask(createConfig(eosEnabled), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, DEFAULT_TIMESTAMP)));

        task.process();
        task.commit();
    }

    @Test
    public void shouldEncodeAndDecodeMetadata() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        assertEquals(DEFAULT_TIMESTAMP, task.decodeTimestamp(task.encodeTimestamp(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void shouldReturnUnknownTimestampIfUnknownVersion() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        final byte[] emptyMessage = {StreamTask.LATEST_MAGIC_BYTE + 1};
        final String encodedString = Base64.getEncoder().encodeToString(emptyMessage);
        assertEquals(RecordQueue.UNKNOWN, task.decodeTimestamp(encodedString));
    }

    @Test
    public void shouldReturnUnknownTimestampIfEmptyMessage() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        assertEquals(RecordQueue.UNKNOWN, task.decodeTimestamp(""));
    }

    @Test
    public void shouldRespectCommitRequested() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        task.requestCommit();
        assertTrue(task.commitRequested());
    }

    @Test
    public void shouldBeProcessableIfAllPartitionsBuffered() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        assertFalse(task.isProcessable(0L));

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(0L));

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.isProcessable(0L));
    }

    @Test
    public void shouldBeProcessableIfWaitedForTooLong() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        final MetricName enforcedProcessMetric = metrics.metricName(
            "enforced-processing-total",
            "stream-task-metrics",
            mkMap(mkEntry("thread-id", Thread.currentThread().getName()), mkEntry("task-id", taskId00.toString()))
        );

        assertFalse(task.isProcessable(0L));
        assertEquals(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(time.milliseconds()));

        assertFalse(task.isProcessable(time.milliseconds() + 99L));

        assertTrue(task.isProcessable(time.milliseconds() + 100L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once decided to enforce, continue doing that
        assertTrue(task.isProcessable(time.milliseconds() + 101L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.isProcessable(time.milliseconds() + 130L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        // one resumed to normal processing, the timer should be reset
        task.process();

        assertFalse(task.isProcessable(time.milliseconds() + 150L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertFalse(task.isProcessable(time.milliseconds() + 249L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertTrue(task.isProcessable(time.milliseconds() + 250L));
        assertEquals(3.0, metrics.metric(enforcedProcessMetric).metricValue());
    }

    @Test
    public void shouldNotBeProcessableIfNoDataAvailble() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        final MetricName enforcedProcessMetric = metrics.metricName(
            "enforced-processing-total",
            "stream-task-metrics",
            mkMap(mkEntry("thread-id", Thread.currentThread().getName()), mkEntry("task-id", taskId00.toString()))
        );

        assertFalse(task.isProcessable(0L));
        assertEquals(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(time.milliseconds()));

        assertFalse(task.isProcessable(time.milliseconds() + 99L));

        assertTrue(task.isProcessable(time.milliseconds() + 100L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once the buffer is drained and no new records coming, the timer should be reset
        task.process();

        assertFalse(task.isProcessable(time.milliseconds() + 110L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // check that after time is reset, we only falls into enforced processing after the
        // whole timeout has elapsed again
        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(time.milliseconds() + 150L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertFalse(task.isProcessable(time.milliseconds() + 249L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertTrue(task.isProcessable(time.milliseconds() + 250L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());
    }


    @Test
    public void shouldPunctuateSystemTimeWhenIntervalElapsed() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(20);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
    }

    @Test
    public void shouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    @Test
    public void shouldPunctuateOnceSystemTimeAfterGap() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        final long now = time.milliseconds();
        time.sleep(100);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(12);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(7);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1); // punctuate at now + 130
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(105); // punctuate at now + 235
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.addRecords(partition2, singletonList(getConsumerRecord(partition2, 0)));

        try {
            task.process();
            fail("Should've thrown StreamsException");
        } catch (final Exception e) {
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorStreamTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorSystemTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldFlushRecordCollectorOnFlushState() {
        final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
        final MockRecordCollector collector = new MockRecordCollector();
        final StreamTask streamTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            collector);
        streamTask.flushState();
        assertTrue(collector.flushed());
    }

    @Test
    public void shouldCheckpointOffsetsOnCommit() throws IOException {
        task = createStatefulTask(createConfig(false), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(
            new File(stateDirectory.directoryForTask(taskId00), StateManagerUtil.CHECKPOINT_FILE_NAME)
        );

        assertThat(checkpoint.read(), equalTo(Collections.singletonMap(changelogPartition, offset)));
    }

    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        task = createStatefulTask(createConfig(true), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final File checkpointFile = new File(
            stateDirectory.directoryForTask(taskId00),
            StateManagerUtil.CHECKPOINT_FILE_NAME
        );

        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        task.processorContext.setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(((ProcessorContextImpl) task.context()).currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { });
    }

    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.processorContext.setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { });
    }

    @Test
    public void shouldNotCloseProducerOnCleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.close(true, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnUncleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.close(false, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnErrorDuringCleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnErrorDuringUncleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        task.close(false, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldCommitTransactionAndCloseProducerOnCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        task.close(true, false);
        task = null;

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotAbortTransactionAndNotCloseProducerOnErrorDuringCleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }

        assertTrue(producer.transactionInFlight());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldOnlyCloseProducerIfFencedOnCommitDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        producer.fenceProducer();

        try {
            task.close(true, false);
            fail("should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            task = null;
            assertTrue(expected.getCause() instanceof RecoverableClientException);
        }

        assertFalse(producer.transactionCommitted());
        assertTrue(producer.transactionInFlight());
        assertFalse(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerIfFencedOnCloseDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        producer.fenceProducerOnClose();

        try {
            task.close(true, false);
            fail("should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            task = null;
            assertTrue(expected.getCause() instanceof RecoverableClientException);
        }

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldAbortTransactionAndCloseProducerOnUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldAbortTransactionAndCloseProducerOnErrorDuringUncleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        task.close(false, false);

        assertTrue(producer.transactionAborted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldOnlyCloseProducerIfFencedOnAbortDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        producer.fenceProducer();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionInFlight());
        assertFalse(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldMigrateTaskIfFencedDuringFlush() {
        final StateStore stateStore = new MockKeyValueStore(storeName, true, true);

        final Map<String, String> storeToChangelogTopic = true ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap();
        final ProcessorTopology topology1 = new ProcessorTopology(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            Collections.emptyMap(),
            singletonList(stateStore),
            Collections.emptyList(),
            storeToChangelogTopic,
            Collections.emptySet()
        );

        task = new StreamTask(
            taskId00,
            partitions,
            topology1,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer)
        );
        task.initializeStateStores();
        task.initializeTopology();
        producer.fenceProducer();

        try {
            task.flushState();
            fail("Expected a TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertThat(expected.migratedTask(), is(task));
        }
    }

    @Test
    public void shouldMigrateTaskIfFencedDuringProcess() {
        final StateStore stateStore = new MockKeyValueStore(storeName, true, true);

        final Map<String, String> storeToChangelogTopic = true ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap();
        final SourceNode<Integer, Integer> sourceNode = new SourceNode<>("test", singletonList(topic1), new WallclockTimestampExtractor(), intDeserializer, intDeserializer);
        final SinkNode<Integer, Integer> sinkNode = new SinkNode<>("test-sink", new StaticTopicNameExtractor<>("out-topic"), intSerializer, intSerializer, new StreamPartitioner<Integer, Integer>() {
            @Override
            public Integer partition(final String topic,
                                     final Integer key,
                                     final Integer value,
                                     final int numPartitions) {
                return 1;
            }
        });

        sourceNode.addChild(sinkNode);

        final ProcessorTopology topology1 = new ProcessorTopology(
            asList(sourceNode, sinkNode),
            mkMap(mkEntry(topic1, sourceNode), mkEntry(topic2, sourceNode)),
            mkMap(mkEntry("out-topic", sinkNode)),
            singletonList(stateStore),
            Collections.emptyList(),
            storeToChangelogTopic,
            Collections.emptySet()
        );

        task = new StreamTask(
            taskId00,
            partitions,
            topology1,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<byte[], byte[]>(
                Cluster.empty(),
                false,
                new DefaultPartitioner(),
                bytesSerializer,
                bytesSerializer
            ) {
                @Override
                public List<PartitionInfo> partitionsFor(final String topic) {
                    return singletonList(null);
                }
            }
        );
        task.initializeStateStores();
        task.initializeTopology();
        producer.fenceProducer();

        try {

            task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
            task.process();
            fail("Expected a TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertThat(expected.migratedTask(), is(task));
        }
    }

    @Test
    public void shouldMigrateTaskIfFencedDuringPunctuate() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);

        final RecordCollectorImpl recordCollector = new RecordCollectorImpl("StreamTask",
                                                                            new LogContext("StreamTaskTest "), new DefaultProductionExceptionHandler(), new Metrics().sensor("skipped-records"));
        recordCollector.init(producer);

        task.initializeStateStores();
        task.initializeTopology();
        producer.fenceProducer();
        try {
            task.punctuate(
                processorSystemTime,
                5,
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> recordCollector.send(
                    "result-topic1",
                    3,
                    5,
                    null,
                    0,
                    time.milliseconds(),
                    new IntegerSerializer(),
                    new IntegerSerializer()
                )
            );
            fail("Expected a TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertThat(expected.migratedTask(), is(task));
        }
    }

    @Test
    public void shouldMigrateTaskIfFencedDuringCommit() {
        final ProcessorTopology topology1 = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        task = new StreamTask(
            taskId00,
            partitions,
            topology1,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer)) {
            @Override
            protected void flushState() {
                // do nothing so that we actually make it to the commit interaction.
            }
        };

        final RecordCollectorImpl recordCollector = new RecordCollectorImpl("StreamTask",
                                                                            new LogContext("StreamTaskTest "), new DefaultProductionExceptionHandler(), new Metrics().sensor("skipped-records"));
        recordCollector.init(producer);

        task.initializeStateStores();
        task.initializeTopology();
        producer.fenceProducer();
        try {
            task.commit();
            fail("Expected a TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertThat(expected.migratedTask(), is(task));
        }
    }

    @Test
    public void shouldOnlyCloseFencedProducerOnUncleanClosedWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        producer.fenceProducer();

        task.close(false, true);
        task = null;

        assertFalse(producer.transactionAborted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldAbortTransactionButNotCloseProducerIfFencedOnCloseDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        producer.fenceProducerOnClose();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionAborted());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }
        assertTrue(processorSystemTime.closed);
        assertTrue(processorStreamTime.closed);
        assertTrue(source1.closed);
    }

    @Test
    public void shouldInitAndBeginTransactionOnCreateIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        assertTrue(producer.transactionInitialized());
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigratedExceptionForBeginTransaction() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        producer.fenceProducer();

        try {
            task.initializeTopology();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }
    }

    @Test
    public void shouldNotThrowOnCloseIfTaskWasNotInitializedWithEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);

        assertFalse(producer.transactionInFlight());
        task.close(false, false);
    }

    @Test
    public void shouldNotInitOrBeginTransactionOnCreateIfEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        assertFalse(producer.transactionInitialized());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.suspend();
        assertTrue(producer.sentOffsets());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        task.suspend();

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        assertFalse(producer.sentOffsets());
        assertFalse(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigratedExceptionInSuspendWhenCommitting() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        producer.fenceProducer();

        try {
            task.suspend();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof RecoverableClientException);
        }
        task = null;

        assertFalse(producer.transactionCommitted());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenClosingProducer() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        producer.fenceProducerOnClose();
        try {
            task.suspend();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof RecoverableClientException);
        }

        assertTrue(producer.transactionCommitted());
    }

    @Test
    public void shouldInitTaskTimeOnResumeWithEosDisabled() {
        shouldInitTaskTimeOnResume(false);
    }

    @Test
    public void shouldInitTaskTimeOnResumeWithEosEnabled() {
        shouldInitTaskTimeOnResume(true);
    }

    private void shouldInitTaskTimeOnResume(final boolean eosEnabled) {
        task = createStatelessTask(createConfig(eosEnabled), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        assertThat(task.partitionTime(partition1), is(RecordQueue.UNKNOWN));
        assertThat(task.streamTime(), is(RecordQueue.UNKNOWN));

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        assertThat(task.partitionTime(partition1), is(0L));
        assertThat(task.streamTime(), is(0L));

        task.suspend();
        assertThat(task.partitionTime(partition1), is(RecordQueue.UNKNOWN));
        assertThat(task.streamTime(), is(RecordQueue.UNKNOWN));

        if (eosEnabled) {
            moveCommittedOffsetsFromProducerToConsumer(0L);
        }

        task.resume();
        assertThat(task.partitionTime(partition1), is(0L));
        assertThat(task.streamTime(), is(0L));
    }

    @Test
    public void shouldStartNewTransactionOnResumeIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        task.initializeTopology();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnResumeIfEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldStartNewTransactionOnCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnCommitIfEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotAbortTransactionOnZombieClosedIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.close(false, true);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnDirtyClosedIfEosDisabled() {
        task = createStatelessTask(createConfig(false), StreamsConfig.METRICS_LATEST);
        task.close(false, false);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldCloseProducerOnCloseWhenEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        task.close(true, false);
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldCloseProducerOnUncleanCloseNotZombieWhenEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        task.close(false, false);
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldCloseProducerOnUncleanCloseIsZombieWhenEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        task.initializeTopology();
        task.close(false, true);
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushing() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.commit();
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() {
        final StreamTask task = createTaskThatThrowsException(false);

        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.suspend();
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }
    }

    @Test
    public void shouldCloseStateManagerIfFailureOnTaskClose() {
        task = createStatefulTaskThatThrowsExceptionOnClose();
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.close(true, false);
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }

        task = null;
        assertFalse(stateStore.isOpen());
    }

    @Test
    public void shouldNotCloseTopologyProcessorNodesIfNotInitialized() {
        final StreamTask task = createTaskThatThrowsException(false);
        try {
            task.close(false, false);
        } catch (final Exception e) {
            fail("should have not closed non-initialized topology");
        }
    }

    @Test
    public void shouldBeInitializedIfChangelogPartitionsIsEmpty() {
        final StreamTask task = createStatefulTask(createConfig(false), false);

        assertTrue(task.initializeStateStores());
    }

    @Test
    public void shouldNotBeInitializedIfChangelogPartitionsIsNonEmpty() {
        final StreamTask task = createStatefulTask(createConfig(false), true);

        assertFalse(task.initializeStateStores());
    }

    @Test
    public void shouldReturnOffsetsForRepartitionTopicsForPurging() {
        final TopicPartition repartition = new TopicPartition("repartition", 1);

        final ProcessorTopology topology = withRepartitionTopics(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(repartition.topic(), source2)),
            Collections.singleton(repartition.topic())
        );
        consumer.assign(asList(partition1, repartition));

        task = new StreamTask(
            taskId00,
            mkSet(partition1, repartition),
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 5L)));
        task.addRecords(repartition, singletonList(getConsumerRecord(repartition, 10L)));

        assertTrue(task.process());
        assertTrue(task.process());

        task.commit();

        final Map<TopicPartition, Long> map = task.purgableOffsets();

        assertThat(map, equalTo(Collections.singletonMap(repartition, 11L)));
    }

    @Test
    public void shouldThrowOnCleanCloseTaskWhenEosEnabledIfTransactionInFlight() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);
        try {
            task.close(true, false);
            fail("should have throw IllegalStateException");
        } catch (final IllegalStateException expected) {
            // pass
        }
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldAlwaysCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true), StreamsConfig.METRICS_LATEST);

        final RecordCollectorImpl recordCollector =  new RecordCollectorImpl(
            "StreamTask",
            new LogContext("StreamTaskTest "),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("dropped-records")
        );
        recordCollector.init(producer);

        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorSystemTime, 5, PunctuationType.WALL_CLOCK_TIME, timestamp -> recordCollector.send("result-topic1", 3, 5, null, 0, time.milliseconds(),
                new IntegerSerializer(),  new IntegerSerializer()));
        task.commit();
        assertEquals(1, producer.history().size());
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException() {
        final Consumer<byte[], byte[]> consumer = mockConsumerWithCommittedException(new AuthorizationException("message"));
        final StreamTask task = createOptimizedStatefulTask(createConfig(false), consumer);
        task.initializeMetadata();
        task.initializeStateStores();
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException() {
        final Consumer<byte[], byte[]> consumer = mockConsumerWithCommittedException(new KafkaException("message"));
        final AbstractTask task = createOptimizedStatefulTask(createConfig(false), consumer);
        task.initializeMetadata();
        task.initializeStateStores();
    }

    @Test(expected = WakeupException.class)
    public void shouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException() {
        final Consumer<byte[], byte[]> consumer = mockConsumerWithCommittedException(new WakeupException());
        final AbstractTask task = createOptimizedStatefulTask(createConfig(false), consumer);
        task.initializeMetadata();
        task.initializeStateStores();
    }

    private Consumer<byte[], byte[]> mockConsumerWithCommittedException(final RuntimeException toThrow) {
        return new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw toThrow;
            }
        };
    }

    private void moveCommittedOffsetsFromProducerToConsumer(final long expectedPartitionTime) {
        // extract the committed metadata from MockProducer
        final List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> metadataList =
            producer.consumerGroupOffsetsHistory();
        final String storedMetadata = metadataList.get(0).get(APPLICATION_ID).get(partition1).metadata();
        final long partitionTime = task.decodeTimestamp(storedMetadata);
        assertThat(partitionTime, is(expectedPartitionTime));

        // since producer and consumer is mocked, we need to "connect" producer and consumer
        // so we should manually commit offsets here to simulate this "connection"
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        final String encryptedMetadata = task.encodeTimestamp(partitionTime);
        offsetMap.put(partition1, new OffsetAndMetadata(partitionTime, encryptedMetadata));
        consumer.commitSync(offsetMap);
    }

    private StreamTask createOptimizedStatefulTask(final StreamsConfig config, final Consumer<byte[], byte[]> consumer) {
        final StateStore stateStore = new MockKeyValueStore(storeName, true);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            singletonList(source1),
            mkMap(mkEntry(topic1, source1)),
            singletonList(stateStore),
            Collections.singletonMap(storeName, topic1));

        return new StreamTask(
            taskId00,
            mkSet(partition1),
            topology,
            consumer,
            changelogReader,
            config,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    private StreamTask createStatefulTask(final StreamsConfig config, final boolean logged) {
        final StateStore stateStore = new MockKeyValueStore(storeName, logged);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            config,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    private StreamTask createStatefulTaskThatThrowsExceptionOnClose() {
        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source3),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
            singletonList(stateStore),
            Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    private StreamTask createStatelessTask(final StreamsConfig streamsConfig,
                                           final String builtInMetricsVersion) {
        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            streamsConfig,
            new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion),
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    // this task will throw exception when processing (on partition2), flushing, suspending and closing
    private StreamTask createTaskThatThrowsException(final boolean enableEos) {
        final ProcessorTopology topology = withSources(
            asList(source1, source3, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3))
        );

        source1.addChild(processorStreamTime);
        source3.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source3.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(enableEos),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer)) {
            @Override
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord(final TopicPartition topicPartition, final long offset) {
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            recordKey,
            recordValue
        );
    }

}
