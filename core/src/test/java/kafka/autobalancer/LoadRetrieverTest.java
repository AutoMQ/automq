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

package kafka.autobalancer;

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.config.AutoBalancerConfig;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.metricsreporter.AutoBalancerMetricsReporter;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import kafka.autobalancer.utils.AutoBalancerClientsIntegrationTestHarness;
import kafka.cluster.EndPoint;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag("esUnit")
public class LoadRetrieverTest extends AutoBalancerClientsIntegrationTestHarness {
    protected static final String METRIC_TOPIC = "AutoBalancerMetricsReporterTest";

    /**
     * Setup the unit test.
     */
    @BeforeEach
    public void setUp() {
        super.setUp();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Override
    protected Map<String, String> overridingNodeProps() {
        Map<String, String> props = new HashMap<>();
        props.put(AutoBalancerConfig.AUTO_BALANCER_TOPIC_CONFIG, METRIC_TOPIC);
        props.put(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        props.put(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        props.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        props.put(KafkaConfig.DefaultReplicationFactorProp(), "1");
        return props;
    }

    @Override
    public Map<String, String> overridingBrokerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AutoBalancerMetricsReporter.class.getName());
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG, "1000");

        return props;
    }

    private boolean checkConsumeRecord(ClusterModel clusterModel, int brokerId) {
        ClusterModelSnapshot snapshot = clusterModel.snapshot();
        if (snapshot.broker(brokerId) == null) {
            return false;
        }
        TopicPartition testTp = new TopicPartition(TOPIC_0, 0);
        TopicPartition metricTp = new TopicPartition(METRIC_TOPIC, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica testReplica = snapshot.replica(brokerId, testTp);
        TopicPartitionReplicaUpdater.TopicPartitionReplica metricReplica = snapshot.replica(brokerId, metricTp);
        if (testReplica == null || metricReplica == null) {
            return false;
        }

        return testReplica.load(Resource.NW_IN) != 0
                && testReplica.load(Resource.NW_OUT) == 0
                && metricReplica.load(Resource.NW_IN) != 0
                && metricReplica.load(Resource.NW_OUT) != 0;
    }

    @Test
    public void testLoadRetrieverShutdown() {
        Map<String, Object> props = new HashMap<>();
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(props, false);
        ClusterModel clusterModel = new ClusterModel(config);
        LoadRetriever loadRetriever = new LoadRetriever(config,
                cluster.controllers().values().iterator().next().controller(), clusterModel);
        loadRetriever.start();

        Assertions.assertTimeout(Duration.ofMillis(5000), loadRetriever::shutdown);

        LoadRetriever loadRetriever2 = new LoadRetriever(config,
                cluster.controllers().values().iterator().next().controller(), clusterModel);
        loadRetriever2.start();
        BrokerServer broker = cluster.brokers().values().iterator().next();
        KafkaConfig brokerConfig = broker.config();
        EndPoint endpoint = brokerConfig.effectiveAdvertisedListeners().iterator().next();
        RegisterBrokerRecord record = new RegisterBrokerRecord()
                .setBrokerId(brokerConfig.brokerId())
                .setEndPoints(new RegisterBrokerRecord.BrokerEndpointCollection(
                        Collections.singletonList(new RegisterBrokerRecord.BrokerEndpoint()
                                .setName("PLAINTEXT")
                                .setHost(endpoint.host())
                                .setPort(endpoint.port())
                                .setSecurityProtocol(endpoint.securityProtocol().id)).iterator()));
        loadRetriever2.onBrokerRegister(record);
        Assertions.assertTimeout(Duration.ofMillis(5000), loadRetriever2::shutdown);
    }

    @Test
    public void testConsume() throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_TOPIC_CONFIG, METRIC_TOPIC);
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, 3000L);
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(props, false);

        ClusterModel clusterModel = new ClusterModel(config);
        LoadRetriever loadRetriever = new LoadRetriever(config,
                cluster.controllers().values().iterator().next().controller(), clusterModel);
        loadRetriever.start();

        BrokerServer broker = cluster.brokers().values().iterator().next();
        KafkaConfig brokerConfig = broker.config();
        EndPoint endpoint = brokerConfig.effectiveAdvertisedListeners().iterator().next();
        RegisterBrokerRecord record = new RegisterBrokerRecord()
                .setBrokerId(brokerConfig.brokerId())
                .setEndPoints(new RegisterBrokerRecord.BrokerEndpointCollection(
                        Collections.singletonList(new RegisterBrokerRecord.BrokerEndpoint()
                                .setName("PLAINTEXT")
                                .setHost(endpoint.host())
                                .setPort(endpoint.port())
                                .setSecurityProtocol(endpoint.securityProtocol().id)).iterator()))
                .setFenced(false)
                .setInControlledShutdown(false);
        clusterModel.onBrokerRegister(record);
        Uuid testTopicId = Uuid.randomUuid();
        Uuid metricTopicId = Uuid.randomUuid();
        clusterModel.onTopicCreate(new TopicRecord()
                .setName(TOPIC_0)
                .setTopicId(testTopicId));
        clusterModel.onPartitionCreate(new PartitionRecord()
                .setReplicas(List.of(brokerConfig.brokerId()))
                .setTopicId(testTopicId)
                .setPartitionId(0));
        clusterModel.onTopicCreate(new TopicRecord()
                .setName(METRIC_TOPIC)
                .setTopicId(metricTopicId));
        clusterModel.onPartitionCreate(new PartitionRecord()
                .setReplicas(List.of(brokerConfig.brokerId()))
                .setTopicId(metricTopicId)
                .setPartitionId(0));
        loadRetriever.onBrokerRegister(record);

        TestUtils.waitForCondition(() -> checkConsumeRecord(clusterModel, brokerConfig.brokerId()),
                15000L, 1000L, () -> "cluster model failed to reach expected status");

        UnregisterBrokerRecord unregisterRecord = new UnregisterBrokerRecord()
                .setBrokerId(brokerConfig.brokerId());
        loadRetriever.onBrokerUnregister(unregisterRecord);
        Thread.sleep(5000);
        Assertions.assertTrue(() -> {
            ClusterModelSnapshot snapshot = clusterModel.snapshot();
            if (snapshot.broker(brokerConfig.brokerId()) != null) {
                return false;
            }
            TopicPartitionReplicaUpdater.TopicPartitionReplica testReplica = snapshot.replica(brokerConfig.brokerId(),
                    new TopicPartition(TOPIC_0, 0));
            TopicPartitionReplicaUpdater.TopicPartitionReplica metricReplica = snapshot.replica(brokerConfig.brokerId(),
                    new TopicPartition(METRIC_TOPIC, 0));
            return testReplica == null && metricReplica == null;
        });

        clusterModel.onBrokerRegister(record);
        loadRetriever.onBrokerRegister(record);
        TestUtils.waitForCondition(() -> checkConsumeRecord(clusterModel, brokerConfig.brokerId()),
                15000L, 1000L, () -> "cluster model failed to reach expected status");
        Assertions.assertTimeout(Duration.ofMillis(5000), loadRetriever::shutdown);
    }
}
