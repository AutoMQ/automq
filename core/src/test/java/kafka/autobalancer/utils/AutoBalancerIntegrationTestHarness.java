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

package kafka.autobalancer.utils;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.server.KafkaConfig;
import kafka.testkit.BrokerNode;
import kafka.testkit.ControllerNode;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.test.TestUtils;


public abstract class AutoBalancerIntegrationTestHarness {
    public static final String HOST = "localhost";

    protected KafkaClusterTestKit cluster = null;
    protected String bootstrapUrl;

    public void setUp() {
        if (cluster != null) {
            return;
        }

        try {
            // do not support multi-broker cluster since ElasticLogManager use shared singleton
            TestKitNodes nodes = new TestKitNodes.Builder().
                    setNumBrokerNodes(1).
                    setNumControllerNodes(1).build();

            int i = 0;
            int[] port = AutoBalancerTestUtils.findLocalPorts(1);
            for (BrokerNode broker : nodes.brokerNodes().values()) {
                broker.propertyOverrides().put(KafkaConfig.ListenersProp(), "EXTERNAL://" + HOST + ":" + port[i]);
                broker.propertyOverrides().put(AutoBalancerMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), HOST + ":" + port[i]);
                broker.propertyOverrides().put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID,
                        AutoBalancerMetricsReporterConfig.DEFAULT_AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID + "-" + TestUtils.RANDOM.nextLong());
                broker.propertyOverrides().put(KafkaConfig.ElasticStreamEndpointProp(), "memory://");
                broker.propertyOverrides().put(KafkaConfig.ElasticStreamEnableProp(), "true");
                broker.propertyOverrides().putAll(overridingBrokerProps());
                broker.propertyOverrides().putAll(overridingNodeProps());
                i++;
            }
            for (ControllerNode controller : nodes.controllerNodes().values()) {
                controller.propertyOverrides().put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE, "true");
                controller.propertyOverrides().putAll(overridingControllerProps());
                controller.propertyOverrides().putAll(overridingNodeProps());
                controller.propertyOverrides().put(KafkaConfig.ElasticStreamEnableProp(), "true");
                controller.propertyOverrides().put(KafkaConfig.S3MockEnableProp(), "true");
            }

            KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
            cluster = builder.build();
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            TestUtils.waitForCondition(() -> cluster.brokers().values().stream().noneMatch(b -> b.brokerState() != BrokerState.RUNNING),
                    TestUtils.DEFAULT_MAX_WAIT_MS, 1000, () -> "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().values().stream().noneMatch(r -> r.client().leaderAndEpoch().leaderId().isEmpty()),
                    TestUtils.DEFAULT_MAX_WAIT_MS, 1000, () -> "RaftManager was not initialized.");
        } catch (Exception e) {
            throw new RuntimeException("create cluster failed", e);
        }

        bootstrapUrl = cluster.bootstrapServers();
    }

    public void tearDown() {
        if (cluster != null) {
            System.out.printf("tear down%n");
            try {
                cluster.close();
            } catch (Exception e) {
                throw new RuntimeException("close cluster failed", e);
            }
        }
    }

    public String bootstrapServers() {
        return bootstrapUrl;
    }

    protected SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    protected File trustStoreFile() {
        return null;
    }

    protected int clusterSize() {
        return 1;
    }

    protected Map<String, String> overridingNodeProps() {
        return Collections.emptyMap();
    }

    protected Map<String, String> overridingBrokerProps() {
        return Collections.emptyMap();
    }

    protected Map<String, String> overridingControllerProps() {
        return Collections.emptyMap();
    }
}
