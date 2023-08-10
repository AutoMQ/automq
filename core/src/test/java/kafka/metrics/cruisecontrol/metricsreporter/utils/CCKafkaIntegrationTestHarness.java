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
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.utils;

import kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import kafka.server.KafkaConfig;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerState;

import java.io.File;
import java.util.Collections;
import java.util.Map;


public abstract class CCKafkaIntegrationTestHarness {
    public static final String HOST = "localhost";

    protected KafkaClusterTestKit cluster = null;
    protected String bootstrapUrl;

    public void setUp() {
        if (cluster != null) {
            return;
        }

        try {
            TestKitNodes nodes = new TestKitNodes.Builder().
                setNumBrokerNodes(1).
                setNumControllerNodes(1).setCoResident(true).build();

            Map<String, String> propOverrides = nodes.brokerNodes().values().iterator().next().propertyOverrides();

            int[] port = CCKafkaTestUtils.findLocalPorts(1);
            propOverrides.put(KafkaConfig.ListenersProp(), "EXTERNAL://" + HOST + ":" + port[0]);
            propOverrides.put(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), HOST + ":" + port[0]);
            propOverrides.put(KafkaConfig.ElasticStreamEndpointProp(), "memory://");

            for (Map.Entry<Object, Object> entry : overridingProps().entrySet()) {
                propOverrides.put((String) entry.getKey(), (String) entry.getValue());
            }
            KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
            cluster = builder.build();
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitUntilTrue(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                () -> "Broker never made it to RUNNING state.", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 1000);
            TestUtils.waitUntilTrue(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                () -> "RaftManager was not initialized.", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 1000);
        } catch (Exception e) {
            throw new RuntimeException("create cluster failed", e);
        }

        bootstrapUrl = cluster.bootstrapServers();
    }

    public void tearDown() {
        if (cluster != null) {
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

    protected Map<Object, Object> overridingProps() {
        return Collections.emptyMap();
    }
}
