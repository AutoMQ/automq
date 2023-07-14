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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.MockFileConfigProvider;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.SinkUtils;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.clients.admin.AdminClientConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstructionWithAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class WorkerTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);
    private static final String WORKER_ID = "localhost:8083";
    private static final String CLUSTER_ID = "test-cluster";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();
    private final ConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy = new AllConnectorClientConfigOverridePolicy();

    private final Map<String, String> workerProps = new HashMap<>();
    private WorkerConfig config;
    private Worker worker;

    private final Map<String, String> defaultProducerConfigs = new HashMap<>();
    private final Map<String, String> defaultConsumerConfigs = new HashMap<>();

    @Mock
    private Plugins plugins;

    @Mock
    private PluginClassLoader pluginLoader;

    @Mock
    private LoaderSwap loaderSwap;

    @Mock
    private Runnable isolatedRunnable;

    @Mock
    private OffsetBackingStore offsetBackingStore;

    @Mock
    private TaskStatus.Listener taskStatusListener;

    @Mock
    private ConnectorStatus.Listener connectorStatusListener;

    @Mock
    private Herder herder;

    @Mock
    private StatusBackingStore statusBackingStore;

    @Mock
    private SourceConnector sourceConnector;

    @Mock
    private SinkConnector sinkConnector;

    @Mock
    private CloseableConnectorContext ctx;

    @Mock private TestSourceTask task;
    @Mock private Converter taskKeyConverter;
    @Mock private Converter taskValueConverter;
    @Mock private HeaderConverter taskHeaderConverter;
    @Mock private ExecutorService executorService;
    @Mock private ConnectorConfig connectorConfig;
    private String mockFileProviderTestId;
    private Map<String, String> connectorProps;

    private final boolean enableTopicCreation;

    private MockedConstruction<WorkerSourceTask> sourceTaskMockedConstruction;
    private MockitoSession mockitoSession;

    @ParameterizedTest.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public WorkerTest(boolean enableTopicCreation) {
        this.enableTopicCreation = enableTopicCreation;
    }

    @Before
    public void setup() {
        // Use strict mode to detect unused mocks
        mockitoSession = Mockito.mockitoSession()
                                .initMocks(this)
                                .strictness(Strictness.STRICT_STUBS)
                                .startMocking();

        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        workerProps.put("config.providers", "file");
        workerProps.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        mockFileProviderTestId = UUID.randomUUID().toString();
        workerProps.put("config.providers.file.param.testId", mockFileProviderTestId);
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        config = new StandaloneConfig(workerProps);

        defaultProducerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        defaultProducerConfigs.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        // By default, producers that are instantiated and used by Connect have idempotency disabled even after idempotency became
        // default for Kafka producers. This is chosen to avoid breaking changes when Connect contacts Kafka brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        defaultProducerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        defaultProducerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
        defaultProducerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        defaultProducerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));

        defaultConsumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        defaultConsumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        defaultConsumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        defaultConsumerConfigs
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        defaultConsumerConfigs
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Some common defaults. They might change on individual tests
        connectorProps = anyConnectorConfigMap();

        // Make calls to new WorkerSourceTask() return a mock to avoid the source task trying to connect to a broker.
        sourceTaskMockedConstruction = mockConstructionWithAnswer(WorkerSourceTask.class, invocation -> {

            // provide implementations of three methods used during testing
            switch (invocation.getMethod().getName()) {
                case "id":
                    return TASK_ID;
                case "loader":
                    return pluginLoader;
                case "awaitStop":
                    return true;
                default:
                    return null;
            }
        });
    }

    @After
    public void teardown() {
        // Critical to always close MockedStatics
        // Ideal would be to use try-with-resources in an individual test, but it introduced a rather large level of
        // indentation of most test bodies, hence sticking with setup() / teardown()
        sourceTaskMockedConstruction.close();

        mockitoSession.finishMocking();
    }

    @Test
    public void testStartAndStopConnector() throws Throwable {
        final String connectorClass = SampleSourceConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        // Create
        mockKafkaClusterId();
        mockConnectorIsolation(connectorClass, sourceConnector);
        mockExecutorRealSubmit(WorkerConnector.class);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());

        FutureCallback<TargetState> onFirstStart = new FutureCallback<>();

        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onFirstStart);

        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onFirstStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());


        FutureCallback<TargetState> onSecondStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onSecondStart);
        try {
            onSecondStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed while trying to start second connector with same name");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ConnectException.class));
        }

        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());

        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);


        verifyKafkaClusterId();
        verifyConnectorIsolation(sourceConnector);
        verifyExecutorSubmit();
        verify(sourceConnector).initialize(any(ConnectorContext.class));
        verify(sourceConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(sourceConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();
        MockFileConfigProvider.assertClosed(mockFileProviderTestId);
    }

    private void mockFileConfigProvider() {
        MockFileConfigProvider mockFileConfigProvider = new MockFileConfigProvider();
        mockFileConfigProvider.configure(Collections.singletonMap("testId", mockFileProviderTestId));
        when(plugins.newConfigProvider(any(AbstractConfig.class),
                                       eq("config.providers.file"),
                                       any(ClassLoaderUsage.class)))
               .thenReturn(mockFileConfigProvider);
    }

    @Test
    public void testStartConnectorFailure() throws Exception {
        final String nonConnectorClass = "java.util.HashMap";
        connectorProps.put(CONNECTOR_CLASS_CONFIG, nonConnectorClass); // Bad connector class name

        Exception exception = new ConnectException("Failed to find Connector");
        mockKafkaClusterId();
        mockGenericIsolation();

        when(plugins.newConnector(anyString())).thenThrow(exception);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        try {
            onStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed to start connector");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }

        assertStartupStatistics(worker, 1, 1, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);

        verify(plugins).newConnector(anyString());
        verifyKafkaClusterId();
        verifyGenericIsolation();
        verify(connectorStatusListener).onFailure(eq(CONNECTOR_ID), any(ConnectException.class));
    }

    @Test
    public void testAddConnectorByAlias() throws Throwable {
        final String connectorAlias = "SampleSourceConnector";
        mockKafkaClusterId();
        mockConnectorIsolation(connectorAlias, sinkConnector);
        mockExecutorRealSubmit(WorkerConnector.class);

        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorAlias);
        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);

        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);

        verifyKafkaClusterId();
        verifyConnectorIsolation(sinkConnector);
        verifyExecutorSubmit();
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(ctx).close();
    }

    @Test
    public void testAddConnectorByShortAlias() throws Throwable {
        final String shortConnectorAlias = "WorkerTest";

        mockKafkaClusterId();
        mockConnectorIsolation(shortConnectorAlias, sinkConnector);
        mockExecutorRealSubmit(WorkerConnector.class);
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, shortConnectorAlias);

        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());
        assertStatistics(worker, 1, 0);

        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyKafkaClusterId();
        verifyConnectorIsolation(sinkConnector);
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();
        verifyExecutorSubmit();
    }

    @Test
    public void testStopInvalidConnector() {
        mockKafkaClusterId();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        worker.stopAndAwaitConnector(CONNECTOR_ID);

        verifyKafkaClusterId();
        verifyConverters();
    }

    @Test
    public void testReconfigureConnectorTasks() throws Throwable {
        final String connectorClass = SampleSourceConnector.class.getName();

        mockKafkaClusterId();
        mockConnectorIsolation(connectorClass, sinkConnector);
        mockExecutorRealSubmit(WorkerConnector.class);

        Map<String, String> taskProps = Collections.singletonMap("foo", "bar");
        when(sinkConnector.taskConfigs(2)).thenReturn(Arrays.asList(taskProps, taskProps));

        // Use doReturn().when() syntax due to when().thenReturn() not being able to return wildcard generic types
        doReturn(TestSourceTask.class).when(sinkConnector).taskClass();


        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onFirstStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onFirstStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onFirstStart.get(1000, TimeUnit.MILLISECONDS));
        assertStatistics(worker, 1, 0);
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());

        FutureCallback<TargetState> onSecondStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onSecondStart);
        try {
            onSecondStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed while trying to start second connector with same name");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ConnectException.class));
        }

        Map<String, String> connProps = new HashMap<>(connectorProps);
        connProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "2");
        ConnectorConfig connConfig = new SinkConnectorConfig(plugins, connProps);
        List<Map<String, String>> taskConfigs = worker.connectorTaskConfigs(CONNECTOR_ID, connConfig);
        Map<String, String> expectedTaskProps = new HashMap<>();
        expectedTaskProps.put("foo", "bar");
        expectedTaskProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        expectedTaskProps.put(TOPICS_CONFIG, "foo,bar");
        assertEquals(2, taskConfigs.size());
        assertEquals(expectedTaskProps, taskConfigs.get(0));
        assertEquals(expectedTaskProps, taskConfigs.get(1));
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyKafkaClusterId();
        verifyConnectorIsolation(sinkConnector);
        verifyExecutorSubmit();
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(sinkConnector).taskClass();
        verify(sinkConnector).taskConfigs(2);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();
    }

    @Test
    public void testAddRemoveSourceTask() {
        mockKafkaClusterId();
        mockTaskIsolation(SampleSourceConnector.class, TestSourceTask.class, task);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy, null);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();

        verifyExecutorSubmit();
    }

    @Test
    public void testAddRemoveSinkTask() {
        // Most of the other cases use source tasks; we make sure to get code coverage for sink tasks here as well
        SinkTask task = mock(TestSinkTask.class);
        mockKafkaClusterId();
        mockTaskIsolation(SampleSinkConnector.class, TestSinkTask.class, task);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy, null);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        Map<String, String> connectorConfigs = anyConnectorConfigMap();
        connectorConfigs.put(TOPICS_CONFIG, "t1");
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, SampleSinkConnector.class.getName());

        worker.startSinkTask(TASK_ID, ClusterConfigState.EMPTY, connectorConfigs, origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();
        verifyExecutorSubmit();
    }

    @Test
    public void testAddRemoveExactlyOnceSourceTask() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        workerProps.put("config.providers", "file");
        workerProps.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        mockFileProviderTestId = UUID.randomUUID().toString();
        workerProps.put("config.providers.file.param.testId", mockFileProviderTestId);
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        workerProps.put(GROUP_ID_CONFIG, "connect-cluster");
        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:2606");
        workerProps.put(OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(CONFIG_TOPIC_CONFIG, "connect-configs");
        workerProps.put(STATUS_STORAGE_TOPIC_CONFIG, "connect-statuses");
        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        config = new DistributedConfig(workerProps);

        mockKafkaClusterId();
        mockTaskIsolation(SampleSourceConnector.class, TestSourceTask.class, task);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);

        Runnable preProducer = mock(Runnable.class);
        Runnable postProducer = mock(Runnable.class);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy, null);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startExactlyOnceSourceTask(TASK_ID, ClusterConfigState.EMPTY,  anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED, preProducer, postProducer);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();
        verifyExecutorSubmit();
    }

    @Test
    public void testTaskStatusMetricsStatuses() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);

        mockKafkaClusterId();
        mockTaskIsolation(SampleSourceConnector.class, TestSourceTask.class, task);
        // Expect that the worker will create converters and will find them using the current classloader ...
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);


        // Each time we check the task metrics, the worker will call the herder
        when(herder.taskStatus(TASK_ID)).thenReturn(
                new ConnectorStateInfo.TaskState(0, "RUNNING", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "PAUSED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "FAILED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "DESTROYED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "UNASSIGNED", "worker", "msg")
        );

        worker = new Worker(WORKER_ID,
            new MockTime(),
            plugins,
            config,
            offsetBackingStore,
            executorService,
            noneConnectorClientConfigOverridePolicy,
            null);

        worker.herder = herder;

        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startSourceTask(
            TASK_ID,
            ClusterConfigState.EMPTY,
            anyConnectorConfigMap(),
            origProps,
            taskStatusListener,
            TargetState.STARTED);

        assertStatusMetrics(1L, "connector-running-task-count");
        assertStatusMetrics(1L, "connector-paused-task-count");
        assertStatusMetrics(1L, "connector-failed-task-count");
        assertStatusMetrics(1L, "connector-destroyed-task-count");
        assertStatusMetrics(1L, "connector-unassigned-task-count");

        worker.stopAndAwaitTask(TASK_ID);
        assertStatusMetrics(0L, "connector-running-task-count");
        assertStatusMetrics(0L, "connector-paused-task-count");
        assertStatusMetrics(0L, "connector-failed-task-count");
        assertStatusMetrics(0L, "connector-destroyed-task-count");
        assertStatusMetrics(0L, "connector-unassigned-task-count");

        WorkerSourceTask instantiatedTask = sourceTaskMockedConstruction.constructed().get(0);
        verify(instantiatedTask).initialize(taskConfig);
        verify(herder, times(5)).taskStatus(TASK_ID);
        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyExecutorSubmit();
        verify(instantiatedTask, atLeastOnce()).id();
        verify(instantiatedTask).awaitStop(anyLong());
        verify(instantiatedTask).removeMetrics();

        // Called when we stop the worker
        verify(instantiatedTask).loader();
        verify(instantiatedTask).stop();
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();
    }

    @Test
    public void testConnectorStatusMetricsGroup_taskStatusCounter() {
        ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
        tasks.put(new ConnectorTaskId("c1", 0), mock(WorkerSourceTask.class));
        tasks.put(new ConnectorTaskId("c1", 1), mock(WorkerSourceTask.class));
        tasks.put(new ConnectorTaskId("c2", 0), mock(WorkerSourceTask.class));

        mockKafkaClusterId();
        mockInternalConverters();
        mockFileConfigProvider();

        worker = new Worker(WORKER_ID,
            new MockTime(),
            plugins,
            config,
            offsetBackingStore,
            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;

        Worker.ConnectorStatusMetricsGroup metricGroup = new Worker.ConnectorStatusMetricsGroup(
            worker.metrics(), tasks, herder
        );
        assertEquals(2L, (long) metricGroup.taskCounter("c1").metricValue(0L));
        assertEquals(1L, (long) metricGroup.taskCounter("c2").metricValue(0L));
        assertEquals(0L, (long) metricGroup.taskCounter("fakeConnector").metricValue(0L));

        verifyKafkaClusterId();
    }

    @Test
    public void testStartTaskFailure() {
        mockInternalConverters();
        mockFileConfigProvider();

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, "missing.From.This.Workers.Classpath");

        mockKafkaClusterId();
        mockGenericIsolation();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);

        assertFalse(worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED));
        assertStartupStatistics(worker, 0, 0, 1, 1);

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 1, 1);
        assertEquals(Collections.emptySet(), worker.taskIds());

        verify(taskStatusListener).onFailure(eq(TASK_ID), any(ConfigException.class));
        verifyKafkaClusterId();
        verifyGenericIsolation();
    }

    @Test
    public void testCleanupTasksOnStop() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();

        mockKafkaClusterId();
        mockTaskIsolation(SampleSourceConnector.class, TestSourceTask.class, task);
        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        mockTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy, null);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyStorage();

        WorkerSourceTask constructedMockTask = sourceTaskMockedConstruction.constructed().get(0);
        verify(constructedMockTask).initialize(taskConfig);
        verify(constructedMockTask).loader();
        verify(constructedMockTask).stop();
        verify(constructedMockTask).awaitStop(anyLong());
        verify(constructedMockTask).removeMetrics();
        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyConverters();
        verifyExecutorSubmit();
    }

    @Test
    public void testConverterOverrides() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        TaskConfig taskConfig = new TaskConfig(origProps);

        mockKafkaClusterId();
        mockTaskIsolation(SampleSourceConnector.class, TestSourceTask.class, task);
        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        mockTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);
        mockExecutorFakeSubmit(WorkerTask.class);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy, null);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        Map<String, String> connProps = anyConnectorConfigMap();
        connProps.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, SampleConverterWithHeaders.class.getName());
        connProps.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, SampleConverterWithHeaders.class.getName());
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, connProps, origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        // We've mocked the Plugin.newConverter method, so we don't currently configure the converters
        WorkerSourceTask instantiatedTask = sourceTaskMockedConstruction.constructed().get(0);
        verify(instantiatedTask).initialize(taskConfig);

        // Remove
        verify(instantiatedTask).stop();
        verify(instantiatedTask).awaitStop(anyLong());
        verify(instantiatedTask).removeMetrics();

        verifyKafkaClusterId();
        verifyTaskIsolation(task);
        verifyExecutorSubmit();
        verifyStorage();
    }

    @Test
    public void testProducerConfigsWithoutOverrides() {
        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());
        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("client.id", "connector-producer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);
        assertEquals(expectedConfigs,
                Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, config, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));
        verify(connectorConfig).originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testProducerConfigsWithOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("producer.acks", "-1");
        props.put("producer.linger.ms", "1000");
        props.put("producer.client.id", "producer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("acks", "-1");
        expectedConfigs.put("linger.ms", "1000");
        expectedConfigs.put("client.id", "producer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());

        assertEquals(expectedConfigs,
                Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
        verify(connectorConfig).originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testProducerConfigsWithClientOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("producer.acks", "-1");
        props.put("producer.linger.ms", "1000");
        props.put("producer.client.id", "producer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("acks", "-1");
        expectedConfigs.put("linger.ms", "5000");
        expectedConfigs.put("batch.size", "1000");
        expectedConfigs.put("client.id", "producer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("linger.ms", "5000");
        connConfig.put("batch.size", "1000");

        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertEquals(expectedConfigs,
                Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
        verify(connectorConfig).originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsWithoutOverrides() {
        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test-connector");
        expectedConfigs.put("client.id", "connector-consumer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());

        assertEquals(expectedConfigs, Worker.baseConsumerConfigs(CONNECTOR_ID, "connector-consumer-" + TASK_ID, config, connectorConfig,
                null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
    }

    @Test
    public void testConsumerConfigsWithOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.group.id", "connect-test");
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "1000");
        props.put("consumer.client.id", "consumer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("auto.offset.reset", "latest");
        expectedConfigs.put("max.poll.records", "1000");
        expectedConfigs.put("client.id", "consumer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());

        assertEquals(expectedConfigs, Worker.baseConsumerConfigs(CONNECTOR_ID, "connector-consumer-" + TASK_ID, configWithOverrides, connectorConfig,
                null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsWithClientOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test-connector");
        expectedConfigs.put("auto.offset.reset", "latest");
        expectedConfigs.put("max.poll.records", "5000");
        expectedConfigs.put("max.poll.interval.ms", "1000");
        expectedConfigs.put("client.id", "connector-consumer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertEquals(expectedConfigs, Worker.baseConsumerConfigs(CONNECTOR_ID, "connector-consumer-" + TASK_ID, configWithOverrides, connectorConfig,
                null, allConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");
        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertThrows(ConnectException.class, () -> Worker.baseConsumerConfigs(CONNECTOR_ID, "connector-consumer-" + TASK_ID,
                configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testAdminConfigsClientOverridesWithAllPolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        props.put("producer.bootstrap.servers", "localhost:1234");
        props.put("consumer.bootstrap.servers", "localhost:4761");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = Collections.singletonMap("metadata.max.age.ms", "10000");
        Map<String, String> expectedConfigs = new HashMap<>(workerProps);
        expectedConfigs.put("bootstrap.servers", "localhost:9092");
        expectedConfigs.put("client.id", "testid");
        expectedConfigs.put("metadata.max.age.ms", "10000");

        //we added a config on the fly
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertEquals(expectedConfigs, Worker.adminConfigs(CONNECTOR_ID, "", configWithOverrides, connectorConfig,
                null, allConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
        verify(connectorConfig).originalsWithPrefix(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX);
    }

    @Test
    public void testAdminConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);
        Map<String, Object> connConfig = Collections.singletonMap("metadata.max.age.ms", "10000");

        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertThrows(ConnectException.class, () -> Worker.adminConfigs("test",
                "", configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
        verify(connectorConfig).originalsWithPrefix(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX);
    }

    @Test
    public void testRegularSourceOffsetsConsumerConfigs() {
        final Map<String, Object> connectorConsumerOverrides = new HashMap<>();
        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connectorConsumerOverrides);

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.put("consumer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("consumer." + ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is allowed to override the isolation level for regular (non-exactly-once) source connectors and their tasks
        assertEquals("read_uncommitted", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.remove("consumer." + ISOLATION_LEVEL_CONFIG);
        connectorConsumerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorConsumerOverrides.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is allowed to override the isolation level for regular (non-exactly-once) source connectors and their tasks
        assertEquals("read_uncommitted", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testExactlyOnceSourceOffsetsConsumerConfigs() {
        final Map<String, Object> connectorConsumerOverrides = new HashMap<>();
        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connectorConsumerOverrides);

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.put("consumer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("consumer." + ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override isolation level when exactly-once support is enabled
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.remove("consumer." + ISOLATION_LEVEL_CONFIG);
        connectorConsumerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorConsumerOverrides.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override isolation level when exactly-once support is enabled
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testExactlyOnceSourceTaskProducerConfigs() {
        final Map<String, Object> connectorProducerOverrides = new HashMap<>();
        when(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(connectorProducerOverrides);

        final String groupId = "connect-cluster";
        final String transactionalId = Worker.taskTransactionalId(groupId, TASK_ID.connector(), TASK_ID.task());

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", groupId);
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));

        workerProps.put("producer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("producer." + ENABLE_IDEMPOTENCE_CONFIG, "false");
        workerProps.put("producer." + TRANSACTIONAL_ID_CONFIG, "some-other-transactional-id");
        config = new DistributedConfig(workerProps);
        producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override idempotence or transactional ID for exactly-once source tasks
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));

        workerProps.remove("producer." + ENABLE_IDEMPOTENCE_CONFIG);
        workerProps.remove("producer." + TRANSACTIONAL_ID_CONFIG);
        connectorProducerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorProducerOverrides.put(ENABLE_IDEMPOTENCE_CONFIG, "false");
        connectorProducerOverrides.put(TRANSACTIONAL_ID_CONFIG, "yet-another-transactional-id");
        config = new DistributedConfig(workerProps);
        producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override idempotence or transactional ID for exactly-once source tasks
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));

        // Rare case: somehow, an explicit null has made it into the connector config
        connectorProducerOverrides.put(TRANSACTIONAL_ID_CONFIG, null);
        producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        // User is still not allowed to override idempotence or transactional ID for exactly-once source tasks
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void testOffsetStoreForRegularSourceConnector() {
        mockInternalConverters();
        mockFileConfigProvider();

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "disabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);
        mockKafkaClusterId();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, we should only use the worker-global offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, even with an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should still only use the worker-global offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        verifyKafkaClusterId();
    }

    @Test
    public void testOffsetStoreForExactlyOnceSourceConnector() {
        mockInternalConverters();
        mockFileConfigProvider();

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);
        mockKafkaClusterId();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, we should only use a connector-specific offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's,  we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        verifyKafkaClusterId();
    }

    @Test
    public void testOffsetStoreForRegularSourceTask() {
        mockInternalConverters();
        mockFileConfigProvider();

        Map<String, Object> producerProps = new HashMap<>();
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> producer = mock(Producer.class);
        TopicAdmin topicAdmin = mock(TopicAdmin.class);

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "disabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);
        mockKafkaClusterId();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");

        final SourceConnectorConfig sourceConfigWithoutOffsetsTopic = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        // With no connector-specific offsets topic in the config, we should only use the worker-global store
        // Pass in a null topic admin to make sure that with these parameters, the method doesn't require a topic admin
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithoutOffsetsTopic, sourceConnector.getClass(), producer, producerProps, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        final SourceConnectorConfig sourceConfigWithOffsetsTopic = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithOffsetsTopic, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertThrows(NullPointerException.class,
                () -> worker.offsetStoreForRegularSourceTask(
                        TASK_ID, sourceConfigWithOffsetsTopic, sourceConnector.getClass(), producer, producerProps, null
                )
        );

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        final SourceConnectorConfig sourceConfigWithSameOffsetsTopicAsWorker = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertThrows(
                NullPointerException.class,
                () -> worker.offsetStoreForRegularSourceTask(
                        TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, null
                )
        );

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertThrows(
                NullPointerException.class,
                () -> worker.offsetStoreForRegularSourceTask(
                        TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, null
                )
        );

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertThrows(
                NullPointerException.class,
                () -> worker.offsetStoreForRegularSourceTask(
                        TASK_ID, sourceConfigWithSameOffsetsTopicAsWorker, sourceConnector.getClass(), producer, producerProps, null
                )
        );

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should still only use the worker-global store
        // Pass in a null topic admin to make sure that with these parameters, the method doesn't require a topic admin
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfigWithoutOffsetsTopic, sourceConnector.getClass(), producer, producerProps, null);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        verifyKafkaClusterId();
    }

    @Test
    public void testOffsetStoreForExactlyOnceSourceTask() {
        mockInternalConverters();
        mockFileConfigProvider();

        Map<String, Object> producerProps = new HashMap<>();
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> producer = mock(Producer.class);
        TopicAdmin topicAdmin = mock(TopicAdmin.class);

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);
        mockKafkaClusterId();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        // With no connector-specific offsets topic in the config, we should only use a connector-specific offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's,  we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        verifyKafkaClusterId();
    }

    @Test
    public void testWorkerMetrics() throws Exception {
        mockKafkaClusterId();
        mockInternalConverters();
        mockFileConfigProvider();

        Worker worker = new Worker("worker-1",
                Time.SYSTEM,
                plugins,
                config,
                offsetBackingStore,
                noneConnectorClientConfigOverridePolicy
                );
        MetricName name = worker.metrics().metrics().metricName("test.avg", "grp1");
        worker.metrics().metrics().addMetric(name, new Avg());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        List<MetricsReporter> list = worker.metrics().metrics().reporters();
        for (MetricsReporter reporter : list) {
            if (reporter instanceof MockMetricsReporter) {
                MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) reporter;
                //verify connect cluster is set in MetricsContext
                assertEquals(CLUSTER_ID, mockMetricsReporter.getMetricsContext().contextLabels().get(WorkerConfig.CONNECT_KAFKA_CLUSTER_ID));
            }
        }
        //verify metric is created with correct jmx prefix
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.connect:type=grp1")));

        verifyKafkaClusterId();
    }

    @Test
    public void testExecutorServiceShutdown() throws InterruptedException {
        mockKafkaClusterId();
        ExecutorService executorService = mock(ExecutorService.class);
        doNothing().when(executorService).shutdown();
        when(executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS)).thenReturn(true);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config,
                            offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy, null);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.stop();
        verifyKafkaClusterId();
        verify(executorService, times(1)).shutdown();
        verify(executorService, times(1)).awaitTermination(1000L, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(executorService);

    }

    @Test
    public void testExecutorServiceShutdownWhenTerminationFails() throws InterruptedException {
        mockKafkaClusterId();
        ExecutorService executorService = mock(ExecutorService.class);
        doNothing().when(executorService).shutdown();
        when(executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS)).thenReturn(false);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config,
                            offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy, null);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.stop();
        verifyKafkaClusterId();
        verify(executorService, times(1)).shutdown();
        verify(executorService, times(1)).shutdownNow();
        verify(executorService, times(2)).awaitTermination(1000L, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(executorService);

    }

    @Test
    public void testExecutorServiceShutdownWhenTerminationThrowsException() throws InterruptedException {
        mockKafkaClusterId();
        ExecutorService executorService = mock(ExecutorService.class);
        doNothing().when(executorService).shutdown();
        when(executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("interrupt"));
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config,
                            offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy, null);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.stop();
        verifyKafkaClusterId();
        verify(executorService, times(1)).shutdown();
        verify(executorService, times(1)).shutdownNow();
        verify(executorService, times(1)).awaitTermination(1000L, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(executorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testZombieFencing() {
        Admin admin = mock(Admin.class);
        AtomicReference<Map<String, Object>> adminConfig = new AtomicReference<>();
        Function<Map<String, Object>, Admin> mockAdminConstructor = actualAdminConfig -> {
            adminConfig.set(actualAdminConfig);
            return admin;
        };
        FenceProducersResult fenceProducersResult = mock(FenceProducersResult.class);
        KafkaFuture<Void> fenceProducersFuture = mock(KafkaFuture.class);
        KafkaFuture<Void> expectedZombieFenceFuture = mock(KafkaFuture.class);
        when(admin.fenceProducers(any(), any())).thenReturn(fenceProducersResult);
        when(fenceProducersResult.all()).thenReturn(fenceProducersFuture);
        when(fenceProducersFuture.whenComplete(any())).thenReturn(expectedZombieFenceFuture);

        mockKafkaClusterId();
        mockGenericIsolation();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, mockAdminConstructor);
        worker.herder = herder;
        worker.start();

        Map<String, String> connectorConfig = anyConnectorConfigMap();
        connectorConfig.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + RETRY_BACKOFF_MS_CONFIG, "4761");

        KafkaFuture<Void> actualZombieFenceFuture =
                worker.fenceZombies(CONNECTOR_ID, 12, connectorConfig);

        assertEquals(expectedZombieFenceFuture, actualZombieFenceFuture);
        assertNotNull(adminConfig.get());
        assertEquals("Admin should be configured with user-specified overrides",
                "4761",
                adminConfig.get().get(RETRY_BACKOFF_MS_CONFIG)
        );

        verifyKafkaClusterId();
        verifyGenericIsolation();
    }

    @Test
    public void testGetSinkConnectorOffsets() throws Exception {
        mockKafkaClusterId();

        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();
        mockAdminListConsumerGroupOffsets(admin, Collections.singletonMap(new TopicPartition("test-topic", 0), new OffsetAndMetadata(10)), null);

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        worker.sinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, cb);
        ConnectorOffsets offsets = cb.get(1000, TimeUnit.MILLISECONDS);

        assertEquals(1, offsets.offsets().size());
        assertEquals(10L, offsets.offsets().get(0).offset().get(SinkUtils.KAFKA_OFFSET_KEY));
        assertEquals(0, offsets.offsets().get(0).partition().get(SinkUtils.KAFKA_PARTITION_KEY));
        assertEquals("test-topic", offsets.offsets().get(0).partition().get(SinkUtils.KAFKA_TOPIC_KEY));

        verify(admin).listConsumerGroupOffsets(eq(SinkUtils.consumerGroupId(CONNECTOR_ID)), any(ListConsumerGroupOffsetsOptions.class));
        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    @Test
    public void testGetSinkConnectorOffsetsAdminClientSynchronousError() {
        mockKafkaClusterId();

        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(admin.listConsumerGroupOffsets(anyString(), any(ListConsumerGroupOffsetsOptions.class))).thenThrow(new ClusterAuthorizationException("Test exception"));

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        worker.sinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin).listConsumerGroupOffsets(eq(SinkUtils.consumerGroupId(CONNECTOR_ID)), any(ListConsumerGroupOffsetsOptions.class));
        verify(admin).close();
        verifyKafkaClusterId();
    }

    @Test
    public void testGetSinkConnectorOffsetsAdminClientAsynchronousError() {
        mockKafkaClusterId();

        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        mockAdminListConsumerGroupOffsets(admin, null, new ClusterAuthorizationException("Test exception"));

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        worker.sinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin).listConsumerGroupOffsets(eq(SinkUtils.consumerGroupId(CONNECTOR_ID)), any(ListConsumerGroupOffsetsOptions.class));
        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    private void mockAdminListConsumerGroupOffsets(Admin admin, Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets, Exception e) {
        mockAdminListConsumerGroupOffsets(admin, consumerGroupOffsets, e, null, 0);
    }

    private void mockAdminListConsumerGroupOffsets(Admin admin, Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets, Exception e, Time time, long delayMs) {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(admin.listConsumerGroupOffsets(anyString(), any(ListConsumerGroupOffsetsOptions.class))).thenReturn(result);
        KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> adminFuture = new KafkaFutureImpl<>();
        if (e != null) {
            adminFuture.completeExceptionally(e);
        } else {
            adminFuture.complete(consumerGroupOffsets);
        }
        when(result.partitionsToOffsetAndMetadata()).thenAnswer(invocation -> {
            if (time != null) {
                time.sleep(delayMs);
            }
            return adminFuture;
        });
    }

    @Test
    public void testGetSourceConnectorOffsets() throws Exception {
        mockKafkaClusterId();

        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        CloseableOffsetStorageReader offsetReader = mock(CloseableOffsetStorageReader.class);

        when(executorService.submit(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        });
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        Set<Map<String, Object>> connectorPartitions =
                Collections.singleton(Collections.singletonMap("partitionKey", "partitionValue"));

        Map<Map<String, Object>, Map<String, Object>> partitionOffsets = Collections.singletonMap(
                Collections.singletonMap("partitionKey", "partitionValue"),
                Collections.singletonMap("offsetKey", "offsetValue")
        );

        when(offsetStore.connectorPartitions(CONNECTOR_ID)).thenReturn(connectorPartitions);
        when(offsetReader.offsets(connectorPartitions)).thenReturn(partitionOffsets);

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        worker.sourceConnectorOffsets(CONNECTOR_ID, offsetStore, offsetReader, cb);
        ConnectorOffsets offsets = cb.get(1000, TimeUnit.MILLISECONDS);

        assertEquals(1, offsets.offsets().size());
        assertEquals("partitionValue", offsets.offsets().get(0).partition().get("partitionKey"));
        assertEquals("offsetValue", offsets.offsets().get(0).offset().get("offsetKey"));

        verify(offsetStore).start();
        verify(offsetReader).close();
        verify(offsetStore).stop();
        verifyKafkaClusterId();
    }

    @Test
    public void testGetSourceConnectorOffsetsError() {
        mockKafkaClusterId();

        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        CloseableOffsetStorageReader offsetReader = mock(CloseableOffsetStorageReader.class);

        when(executorService.submit(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        });
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        when(offsetStore.connectorPartitions(CONNECTOR_ID)).thenThrow(new ConnectException("Test exception"));

        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        worker.sourceConnectorOffsets(CONNECTOR_ID, offsetStore, offsetReader, cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(offsetStore).start();
        verify(offsetReader).close();
        verify(offsetStore).stop();
        verifyKafkaClusterId();
    }

    @Test
    public void testAlterOffsetsConnectorDoesNotSupportOffsetAlteration() {
        mockKafkaClusterId();

        mockInternalConverters();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        mockGenericIsolation();
        when(plugins.newConnector(anyString())).thenReturn(sourceConnector);
        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sourceConnector.alterOffsets(eq(connectorProps), anyMap())).thenThrow(new UnsupportedOperationException("This connector doesn't " +
                "support altering of offsets"));

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifyConnectorOffsets(CONNECTOR_ID, connectorProps,
                Collections.singletonMap(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue")),
                cb);

        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());
        assertEquals("Failed to modify offsets for connector " + CONNECTOR_ID + " because it doesn't support external modification of offsets",
                e.getCause().getMessage());

        verifyGenericIsolation();
        verifyKafkaClusterId();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAlterOffsetsSourceConnector() throws Exception {
        mockKafkaClusterId();
        mockInternalConverters();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sourceConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);
        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        OffsetStorageWriter offsetWriter = mock(OffsetStorageWriter.class);

        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue"));
        partitionOffsets.put(Collections.singletonMap("partitionKey", "partitionValue2"), Collections.singletonMap("offsetKey", "offsetValue"));

        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            invocation.getArgument(0, Callback.class).onCompletion(null, null);
            return null;
        });

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySourceConnectorOffsets(CONNECTOR_ID, sourceConnector, connectorProps, partitionOffsets, offsetStore, producer,
                offsetWriter, Thread.currentThread().getContextClassLoader(), cb);
        assertEquals("The offsets for this connector have been altered successfully", cb.get(1000, TimeUnit.MILLISECONDS).message());

        verify(offsetStore).start();
        partitionOffsets.forEach((partition, offset) -> verify(offsetWriter).offset(partition, offset));
        verify(offsetWriter).beginFlush();
        verify(offsetWriter).doFlush(any());
        verify(offsetStore, timeout(1000)).stop();
        verifyKafkaClusterId();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAlterOffsetsSourceConnectorError() throws Exception {
        mockKafkaClusterId();
        mockInternalConverters();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sourceConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);
        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        OffsetStorageWriter offsetWriter = mock(OffsetStorageWriter.class);

        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(Collections.singletonMap("partitionKey", "partitionValue"), Collections.singletonMap("offsetKey", "offsetValue"));
        partitionOffsets.put(Collections.singletonMap("partitionKey", "partitionValue2"), Collections.singletonMap("offsetKey", "offsetValue"));

        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            invocation.getArgument(0, Callback.class).onCompletion(new RuntimeException("Test exception"), null);
            return null;
        });

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySourceConnectorOffsets(CONNECTOR_ID, sourceConnector, connectorProps, partitionOffsets, offsetStore, producer,
                offsetWriter, Thread.currentThread().getContextClassLoader(), cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS).message());
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(offsetStore).start();
        partitionOffsets.forEach((partition, offset) -> verify(offsetWriter).offset(partition, offset));
        verify(offsetWriter).beginFlush();
        verify(offsetWriter).doFlush(any());
        verify(offsetStore, timeout(1000)).stop();
        verifyKafkaClusterId();
    }

    @Test
    public void testNormalizeSourceConnectorOffsets() throws Exception {
        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap("filename", "/path/to/filename"),
                Collections.singletonMap("position", 20)
        );

        assertTrue(offsets.values().iterator().next().get("position") instanceof Integer);

        mockInternalConverters();

        mockKafkaClusterId();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy, null);

        Map<Map<String, ?>, Map<String, ?>> normalizedOffsets = worker.normalizeSourceConnectorOffsets(offsets);
        assertEquals(1, normalizedOffsets.size());

        // The integer value 20 gets deserialized as a long value by the JsonConverter
        assertTrue(normalizedOffsets.values().iterator().next().get("position") instanceof Long);
    }

    @Test
    public void testAlterOffsetsSinkConnectorNoDeletes() throws Exception {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> alterOffsetsMapCapture = ArgumentCaptor.forClass(Map.class);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        partitionOffsets.put(partition1, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 500));
        Map<String, String> partition2 = new HashMap<>();
        partition2.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition2.put(SinkUtils.KAFKA_PARTITION_KEY, "20");
        partitionOffsets.put(partition2, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 100));

        // A null value for deleteOffsetsSetCapture indicates that we don't expect any call to Admin::deleteConsumerGroupOffsets
        alterOffsetsSinkConnector(partitionOffsets, alterOffsetsMapCapture, null);

        assertEquals(2, alterOffsetsMapCapture.getValue().size());
        assertEquals(500, alterOffsetsMapCapture.getValue().get(new TopicPartition("test_topic", 10)).offset());
        assertEquals(100, alterOffsetsMapCapture.getValue().get(new TopicPartition("test_topic", 20)).offset());
    }

    @Test
    public void testAlterOffsetSinkConnectorOnlyDeletes() throws Exception {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<TopicPartition>> deleteOffsetsSetCapture = ArgumentCaptor.forClass(Set.class);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        partitionOffsets.put(partition1, null);
        Map<String, String> partition2 = new HashMap<>();
        partition2.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition2.put(SinkUtils.KAFKA_PARTITION_KEY, "20");
        partitionOffsets.put(partition2, null);

        // A null value for alterOffsetsMapCapture indicates that we don't expect any call to Admin::alterConsumerGroupOffsets
        alterOffsetsSinkConnector(partitionOffsets, null, deleteOffsetsSetCapture);

        Set<TopicPartition> expectedTopicPartitionsForOffsetDelete = new HashSet<>();
        expectedTopicPartitionsForOffsetDelete.add(new TopicPartition("test_topic", 10));
        expectedTopicPartitionsForOffsetDelete.add(new TopicPartition("test_topic", 20));

        // Verify that contents are equal without caring about order
        assertEquals(expectedTopicPartitionsForOffsetDelete, deleteOffsetsSetCapture.getValue());
    }

    @Test
    public void testAlterOffsetsSinkConnectorAltersAndDeletes() throws Exception {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> alterOffsetsMapCapture = ArgumentCaptor.forClass(Map.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<TopicPartition>> deleteOffsetsSetCapture = ArgumentCaptor.forClass(Set.class);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        partitionOffsets.put(partition1, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, "100"));
        Map<String, String> partition2 = new HashMap<>();
        partition2.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition2.put(SinkUtils.KAFKA_PARTITION_KEY, "20");
        partitionOffsets.put(partition2, null);

        alterOffsetsSinkConnector(partitionOffsets, alterOffsetsMapCapture, deleteOffsetsSetCapture);

        assertEquals(1, alterOffsetsMapCapture.getValue().size());
        assertEquals(100, alterOffsetsMapCapture.getValue().get(new TopicPartition("test_topic", 10)).offset());

        Set<TopicPartition> expectedTopicPartitionsForOffsetDelete = Collections.singleton(new TopicPartition("test_topic", 20));
        assertEquals(expectedTopicPartitionsForOffsetDelete, deleteOffsetsSetCapture.getValue());
    }

    private void alterOffsetsSinkConnector(Map<Map<String, ?>, Map<String, ?>> partitionOffsets,
                                           ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> alterOffsetsMapCapture,
                                           ArgumentCaptor<Set<TopicPartition>> deleteOffsetsSetCapture) throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newCachedThreadPool(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sinkConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);

        // If alterOffsetsMapCapture is null, then we won't stub any of the following methods resulting in test failures in case
        // offsets for certain topic partitions are actually attempted to be altered.
        if (alterOffsetsMapCapture != null) {
            AlterConsumerGroupOffsetsResult alterConsumerGroupOffsetsResult = mock(AlterConsumerGroupOffsetsResult.class);
            when(admin.alterConsumerGroupOffsets(anyString(), alterOffsetsMapCapture.capture(), any(AlterConsumerGroupOffsetsOptions.class)))
                    .thenReturn(alterConsumerGroupOffsetsResult);
            KafkaFuture<Void> alterFuture = KafkaFuture.completedFuture(null);
            when(alterConsumerGroupOffsetsResult.all()).thenReturn(alterFuture);
        }

        // If deleteOffsetsSetCapture is null, then we won't stub any of the following methods resulting in test failures in case
        // offsets for certain topic partitions are actually attempted to be deleted.
        if (deleteOffsetsSetCapture != null) {
            DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult = mock(DeleteConsumerGroupOffsetsResult.class);
            when(admin.deleteConsumerGroupOffsets(anyString(), deleteOffsetsSetCapture.capture(), any(DeleteConsumerGroupOffsetsOptions.class)))
                    .thenReturn(deleteConsumerGroupOffsetsResult);
            KafkaFuture<Void> deleteFuture = KafkaFuture.completedFuture(null);
            when(deleteConsumerGroupOffsetsResult.all()).thenReturn(deleteFuture);
        }

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, partitionOffsets,
                Thread.currentThread().getContextClassLoader(), cb);
        assertEquals("The offsets for this connector have been altered successfully", cb.get(1000, TimeUnit.MILLISECONDS).message());

        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    @Test
    public void testAlterOffsetsSinkConnectorAlterOffsetsError() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sinkConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);

        AlterConsumerGroupOffsetsResult alterConsumerGroupOffsetsResult = mock(AlterConsumerGroupOffsetsResult.class);
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap(), any(AlterConsumerGroupOffsetsOptions.class)))
                .thenReturn(alterConsumerGroupOffsetsResult);
        KafkaFutureImpl<Void> alterFuture = new KafkaFutureImpl<>();
        alterFuture.completeExceptionally(new ClusterAuthorizationException("Test exception"));
        when(alterConsumerGroupOffsetsResult.all()).thenReturn(alterFuture);

        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = Collections.singletonMap(partition1,
                Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, "100"));

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, partitionOffsets,
                Thread.currentThread().getContextClassLoader(), cb);

        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin, timeout(1000)).close();
        verifyNoMoreInteractions(admin);
        verifyKafkaClusterId();
    }

    @Test
    public void testAlterOffsetsSinkConnectorDeleteOffsetsError() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sinkConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);

        AlterConsumerGroupOffsetsResult alterConsumerGroupOffsetsResult = mock(AlterConsumerGroupOffsetsResult.class);
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap(), any(AlterConsumerGroupOffsetsOptions.class)))
                .thenReturn(alterConsumerGroupOffsetsResult);
        KafkaFuture<Void> alterFuture = KafkaFuture.completedFuture(null);
        when(alterConsumerGroupOffsetsResult.all()).thenReturn(alterFuture);

        DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult = mock(DeleteConsumerGroupOffsetsResult.class);
        when(admin.deleteConsumerGroupOffsets(anyString(), anySet(), any(DeleteConsumerGroupOffsetsOptions.class)))
                .thenReturn(deleteConsumerGroupOffsetsResult);
        KafkaFutureImpl<Void> deleteFuture = new KafkaFutureImpl<>();
        deleteFuture.completeExceptionally(new ClusterAuthorizationException("Test exception"));
        when(deleteConsumerGroupOffsetsResult.all()).thenReturn(deleteFuture);

        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        partitionOffsets.put(partition1, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, "100"));
        Map<String, String> partition2 = new HashMap<>();
        partition2.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition2.put(SinkUtils.KAFKA_PARTITION_KEY, "20");
        partitionOffsets.put(partition2, null);

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, partitionOffsets,
                Thread.currentThread().getContextClassLoader(), cb);

        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin, timeout(1000)).close();
        verifyNoMoreInteractions(admin);
        verifyKafkaClusterId();
    }

    @Test
    public void testAlterOffsetsSinkConnectorSynchronousError() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sinkConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);

        when(admin.alterConsumerGroupOffsets(anyString(), anyMap(), any(AlterConsumerGroupOffsetsOptions.class)))
                .thenThrow(new RuntimeException("Test Exception"));

        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        Map<String, String> partition1 = new HashMap<>();
        partition1.put(SinkUtils.KAFKA_TOPIC_KEY, "test_topic");
        partition1.put(SinkUtils.KAFKA_PARTITION_KEY, "10");
        partitionOffsets.put(partition1, Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, "100"));

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, partitionOffsets,
                Thread.currentThread().getContextClassLoader(), cb);

        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin, timeout(1000)).close();
        verifyNoMoreInteractions(admin);
        verifyKafkaClusterId();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResetOffsetsSourceConnectorExactlyOnceSupportEnabled() throws Exception {
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:9092");
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);
        mockKafkaClusterId();
        mockInternalConverters();
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sourceConnector.alterOffsets(eq(connectorProps), anyMap())).thenReturn(true);
        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        OffsetStorageWriter offsetWriter = mock(OffsetStorageWriter.class);

        Set<Map<String, Object>> connectorPartitions = new HashSet<>();
        connectorPartitions.add(Collections.singletonMap("partitionKey", "partitionValue1"));
        connectorPartitions.add(Collections.singletonMap("partitionKey", "partitionValue2"));
        when(offsetStore.connectorPartitions(eq(CONNECTOR_ID))).thenReturn(connectorPartitions);
        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            invocation.getArgument(0, Callback.class).onCompletion(null, null);
            return null;
        });

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySourceConnectorOffsets(CONNECTOR_ID, sourceConnector, connectorProps, null, offsetStore, producer,
                offsetWriter, Thread.currentThread().getContextClassLoader(), cb);
        assertEquals("The offsets for this connector have been reset successfully", cb.get(1000, TimeUnit.MILLISECONDS).message());

        InOrder inOrder = Mockito.inOrder(offsetStore, offsetWriter, producer);
        inOrder.verify(offsetStore).start();
        connectorPartitions.forEach(partition -> inOrder.verify(offsetWriter).offset(partition, null));
        inOrder.verify(offsetWriter).beginFlush();
        inOrder.verify(producer).initTransactions();
        inOrder.verify(producer).beginTransaction();
        inOrder.verify(offsetWriter).doFlush(any());
        inOrder.verify(producer).commitTransaction();
        inOrder.verify(offsetStore, timeout(1000)).stop();
        verifyKafkaClusterId();
    }

    @Test
    public void testResetOffsetsSinkConnector() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        Time time = new MockTime();
        worker = new Worker(WORKER_ID, time, plugins, config, offsetBackingStore, Executors.newCachedThreadPool(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());

        TopicPartition tp = new TopicPartition("test-topic", 0);
        mockAdminListConsumerGroupOffsets(admin, Collections.singletonMap(tp, new OffsetAndMetadata(10L)), null, time, 2000);
        when(sinkConnector.alterOffsets(eq(connectorProps), eq(Collections.singletonMap(tp, null)))).thenAnswer(invocation -> {
            time.sleep(3000);
            return true;
        });

        DeleteConsumerGroupsResult deleteConsumerGroupsResult = mock(DeleteConsumerGroupsResult.class);
        when(admin.deleteConsumerGroups(anyCollection(), any(DeleteConsumerGroupsOptions.class))).thenReturn(deleteConsumerGroupsResult);
        when(deleteConsumerGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, null,
                Thread.currentThread().getContextClassLoader(), cb);
        assertEquals("The offsets for this connector have been reset successfully", cb.get(1000, TimeUnit.MILLISECONDS).message());

        ArgumentCaptor<DeleteConsumerGroupsOptions> deleteConsumerGroupsOptionsArgumentCaptor = ArgumentCaptor.forClass(DeleteConsumerGroupsOptions.class);
        verify(admin).deleteConsumerGroups(anyCollection(), deleteConsumerGroupsOptionsArgumentCaptor.capture());
        // Expect the call to Admin::deleteConsumerGroups to have a timeout value equal to the overall timeout value of DEFAULT_REST_REQUEST_TIMEOUT_MS
        // minus the delay introduced in the call to Admin::listConsumerGroupOffsets (2000 ms) and the delay introduced in the call to
        // SinkConnector::alterOffsets (3000 ms)
        assertEquals((int) ConnectResource.DEFAULT_REST_REQUEST_TIMEOUT_MS - 2000L - 3000L,
                deleteConsumerGroupsOptionsArgumentCaptor.getValue().timeoutMs().intValue());
        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    @Test
    public void testResetOffsetsSinkConnectorDeleteConsumerGroupError() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, Executors.newCachedThreadPool(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());

        TopicPartition tp = new TopicPartition("test-topic", 0);
        mockAdminListConsumerGroupOffsets(admin, Collections.singletonMap(tp, new OffsetAndMetadata(10L)), null);
        when(sinkConnector.alterOffsets(eq(connectorProps), eq(Collections.singletonMap(tp, null)))).thenReturn(true);

        DeleteConsumerGroupsResult deleteConsumerGroupsResult = mock(DeleteConsumerGroupsResult.class);
        when(admin.deleteConsumerGroups(anyCollection(), any(DeleteConsumerGroupsOptions.class))).thenReturn(deleteConsumerGroupsResult);
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new ClusterAuthorizationException("Test exception"));
        when(deleteConsumerGroupsResult.all()).thenReturn(future);

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, null,
                Thread.currentThread().getContextClassLoader(), cb);

        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(ConnectException.class, e.getCause().getClass());

        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModifySourceConnectorOffsetsTimeout() throws Exception {
        mockKafkaClusterId();
        Time time = new MockTime();
        mockInternalConverters();
        worker = new Worker(WORKER_ID, time, plugins, config, offsetBackingStore, Executors.newSingleThreadExecutor(),
                allConnectorClientConfigOverridePolicy, null);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
        when(sourceConnector.alterOffsets(eq(connectorProps), anyMap())).thenAnswer(invocation -> {
            time.sleep(ConnectResource.DEFAULT_REST_REQUEST_TIMEOUT_MS + 1000);
            return true;
        });
        ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        OffsetStorageWriter offsetWriter = mock(OffsetStorageWriter.class);

        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = Collections.singletonMap(
                Collections.singletonMap("partitionKey", "partitionValue"),
                Collections.singletonMap("offsetKey", "offsetValue"));

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySourceConnectorOffsets(CONNECTOR_ID, sourceConnector, connectorProps, partitionOffsets, offsetStore, producer,
                offsetWriter, Thread.currentThread().getContextClassLoader(), cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS).message());
        assertEquals(ConnectException.class, e.getCause().getClass());
        assertThat(e.getCause().getMessage(), containsString("Timed out"));

        verify(offsetStore).start();
        verify(offsetStore, timeout(1000)).stop();
        verifyKafkaClusterId();
    }

    @Test
    public void testModifyOffsetsSinkConnectorTimeout() throws Exception {
        mockKafkaClusterId();
        String connectorClass = SampleSinkConnector.class.getName();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

        Admin admin = mock(Admin.class);
        Time time = new MockTime();
        worker = new Worker(WORKER_ID, time, plugins, config, offsetBackingStore, Executors.newCachedThreadPool(),
                allConnectorClientConfigOverridePolicy, config -> admin);
        worker.start();

        when(plugins.withClassLoader(any(ClassLoader.class), any(Runnable.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());

        when(sinkConnector.alterOffsets(eq(connectorProps), anyMap())).thenAnswer(invocation -> {
            time.sleep(ConnectResource.DEFAULT_REST_REQUEST_TIMEOUT_MS + 1000);
            return true;
        });

        FutureCallback<Message> cb = new FutureCallback<>();
        worker.modifySinkConnectorOffsets(CONNECTOR_ID, sinkConnector, connectorProps, new HashMap<>(),
                Thread.currentThread().getContextClassLoader(), cb);
        ExecutionException e = assertThrows(ExecutionException.class, () -> cb.get(1000, TimeUnit.MILLISECONDS).message());
        assertEquals(ConnectException.class, e.getCause().getClass());
        assertThat(e.getCause().getMessage(), containsString("Timed out"));

        verify(admin, timeout(1000)).close();
        verifyKafkaClusterId();
    }

    private void assertStatusMetrics(long expected, String metricName) {
        MetricGroup statusMetrics = worker.connectorStatusMetricsGroup().metricGroup(TASK_ID.connector());
        if (expected == 0L) {
            assertNull(statusMetrics);
            return;
        }
        assertEquals(expected, MockConnectMetrics.currentMetricValue(worker.metrics(), statusMetrics, metricName));
    }

    private void assertStatistics(Worker worker, int connectors, int tasks) {
        assertStatusMetrics(tasks, "connector-total-task-count");
        MetricGroup workerMetrics = worker.workerMetricsGroup().metricGroup();
        assertEquals(connectors, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-count"), 0.0001d);
        assertEquals(tasks, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-count"), 0.0001d);
        assertEquals(tasks, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-count"), 0.0001d);
    }

    private void assertStartupStatistics(Worker worker, int connectorStartupAttempts, int connectorStartupFailures, int taskStartupAttempts, int taskStartupFailures) {
        double connectStartupSuccesses = connectorStartupAttempts - connectorStartupFailures;
        double taskStartupSuccesses = taskStartupAttempts - taskStartupFailures;
        double connectStartupSuccessPct = 0.0d;
        double connectStartupFailurePct = 0.0d;
        double taskStartupSuccessPct = 0.0d;
        double taskStartupFailurePct = 0.0d;
        if (connectorStartupAttempts != 0) {
            connectStartupSuccessPct = connectStartupSuccesses / connectorStartupAttempts;
            connectStartupFailurePct = (double) connectorStartupFailures / connectorStartupAttempts;
        }
        if (taskStartupAttempts != 0) {
            taskStartupSuccessPct = taskStartupSuccesses / taskStartupAttempts;
            taskStartupFailurePct = (double) taskStartupFailures / taskStartupAttempts;
        }
        MetricGroup workerMetrics = worker.workerMetricsGroup().metricGroup();
        assertEquals(connectorStartupAttempts, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-attempts-total"), 0.0001d);
        assertEquals(connectStartupSuccesses, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-success-total"), 0.0001d);
        assertEquals(connectorStartupFailures, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-failure-total"), 0.0001d);
        assertEquals(connectStartupSuccessPct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-success-percentage"), 0.0001d);
        assertEquals(connectStartupFailurePct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-failure-percentage"), 0.0001d);
        assertEquals(taskStartupAttempts, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-attempts-total"), 0.0001d);
        assertEquals(taskStartupSuccesses, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-success-total"), 0.0001d);
        assertEquals(taskStartupFailures, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-failure-total"), 0.0001d);
        assertEquals(taskStartupSuccessPct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-success-percentage"), 0.0001d);
        assertEquals(taskStartupFailurePct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-failure-percentage"), 0.0001d);
    }

    private void mockStorage() {
        when(herder.statusBackingStore()).thenReturn(statusBackingStore);
    }


    private void verifyStorage() {
        verify(offsetBackingStore).start();
        verify(herder).statusBackingStore();
        verify(offsetBackingStore).stop();
    }

    private void mockInternalConverters() {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap(SCHEMAS_ENABLE_CONFIG, false), false);

        when(plugins.newInternalConverter(eq(true), anyString(), anyMap()))
                       .thenReturn(jsonConverter);

        when(plugins.newInternalConverter(eq(false), anyString(), anyMap()))
                       .thenReturn(jsonConverter);
    }

    private void verifyConverters() {
        verify(plugins, times(1)).newInternalConverter(eq(true), anyString(), anyMap());
        verify(plugins).newInternalConverter(eq(false), anyString(), anyMap());
    }

    private void mockTaskConverter(ClassLoaderUsage classLoaderUsage, String converterClassConfig, Converter returning) {
        when(plugins.newConverter(any(AbstractConfig.class), eq(converterClassConfig), eq(classLoaderUsage)))
                       .thenReturn(returning);
    }

    private void verifyTaskConverter(String converterClassConfig) {
        verify(plugins).newConverter(any(AbstractConfig.class), eq(converterClassConfig), eq(ClassLoaderUsage.CURRENT_CLASSLOADER));
    }

    private void mockTaskHeaderConverter(ClassLoaderUsage classLoaderUsage, HeaderConverter returning) {
        when(plugins.newHeaderConverter(any(AbstractConfig.class), eq(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG), eq(classLoaderUsage)))
               .thenReturn(returning);
    }

    private void verifyTaskHeaderConverter() {
        verify(plugins).newHeaderConverter(any(AbstractConfig.class), eq(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG), eq(ClassLoaderUsage.CURRENT_CLASSLOADER));
    }

    private void mockGenericIsolation() {
        when(plugins.connectorLoader(anyString())).thenReturn(pluginLoader);
        when(plugins.withClassLoader(pluginLoader)).thenReturn(loaderSwap);
    }

    private void verifyGenericIsolation() {
        verify(plugins, atLeastOnce()).withClassLoader(pluginLoader);
        verify(loaderSwap, atLeastOnce()).close();
    }

    private void mockConnectorIsolation(String connectorClass, Connector connector) {
        mockGenericIsolation();
        when(plugins.newConnector(connectorClass)).thenReturn(connector);
        when(connector.version()).thenReturn("1.0");
    }

    private void verifyConnectorIsolation(Connector connector) {
        verifyGenericIsolation();
        verify(plugins).newConnector(anyString());
        verify(connector, atLeastOnce()).version();
    }

    private void mockTaskIsolation(Class<? extends Connector> connector, Class<? extends Task> taskClass, Task task) {
        mockGenericIsolation();
        doReturn(connector).when(plugins).connectorClass(connector.getName());
        when(plugins.newTask(taskClass)).thenReturn(task);
        when(task.version()).thenReturn("1.0");
    }

    private void verifyTaskIsolation(Task task) {
        verifyGenericIsolation();
        verify(plugins).connectorClass(anyString());
        verify(plugins).newTask(any());
        verify(task).version();
    }

    private void mockExecutorRealSubmit(Class<? extends Runnable> runnableClass) {
        // This test expects the runnable to be executed, so have the isolated runnable pass-through.
        // Requires using the Worker constructor without the mocked executorService
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(runnableClass);
        when(plugins.withClassLoader(same(pluginLoader), runnableCaptor.capture())).thenReturn(isolatedRunnable);
        doAnswer(invocation -> {
            runnableCaptor.getValue().run();
            return null;
        }).when(isolatedRunnable).run();
    }

    private void mockExecutorFakeSubmit(Class<? extends Runnable> runnableClass) {
        // This test does not expect the runnable to be executed, so skip it.
        // Requires using the Worker constructor with the mocked executorService
        when(plugins.withClassLoader(same(pluginLoader), any(runnableClass))).thenReturn(isolatedRunnable);
        doNothing().when(isolatedRunnable).run();
        when(executorService.submit(isolatedRunnable)).thenAnswer(invocation -> {
            isolatedRunnable.run(); // performs the doNothing action but marks the isolatedRunnable as having run.
            return null;
        });
    }

    private void verifyExecutorSubmit() {
        verify(plugins).withClassLoader(same(pluginLoader), any(Runnable.class));
        verify(isolatedRunnable).run();
        // Don't assert that the executorService.submit() was called explicitly, in case the real executor was used.
        // We learn that it was called via the isolatedRunnable.run() method being called.
    }

    private void mockKafkaClusterId() {
        config = spy(config);
        doReturn(CLUSTER_ID).when(config).kafkaClusterId();
    }

    private void verifyKafkaClusterId() {
        verify(config, atLeastOnce()).kafkaClusterId();
    }

    private Map<String, String> anyConnectorConfigMap() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }

    private static class TestSourceTask extends SourceTask {
        public TestSourceTask() {
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public List<SourceRecord> poll() {
            return null;
        }

        @Override
        public void stop() {
        }
    }

    private static class TestSinkTask extends SinkTask {
        public TestSinkTask() {
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records) {
        }

        @Override
        public void stop() {
        }

    }

}
