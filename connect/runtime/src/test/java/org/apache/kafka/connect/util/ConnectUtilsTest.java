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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class ConnectUtilsTest {

    @Test
    public void testLookupKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(cluster).build();
        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupNullKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(cluster).clusterId(null).build();
        assertNull(ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupKafkaClusterIdTimeout() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(cluster).build();
        adminClient.timeoutNextRequest(1);

        assertThrows(ConnectException.class, () -> ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testAddMetricsContextPropertiesDistributed() {
        Map<String, String> props = new HashMap<>();
        props.put(DistributedConfig.GROUP_ID_CONFIG, "connect-cluster");
        props.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        props.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        props.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        props.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DistributedConfig config = new DistributedConfig(props);

        Map<String, Object> prop = new HashMap<>();
        ConnectUtils.addMetricsContextProperties(prop, config, "cluster-1");
        assertEquals("connect-cluster", prop.get(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_GROUP_ID));
        assertEquals("cluster-1", prop.get(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_KAFKA_CLUSTER_ID));
    }

    @Test
    public void testAddMetricsContextPropertiesStandalone() {
        Map<String, String> props = new HashMap<>();
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "offsetStorageFile");
        props.put(StandaloneConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        StandaloneConfig config = new StandaloneConfig(props);

        Map<String, Object> prop = new HashMap<>();
        ConnectUtils.addMetricsContextProperties(prop, config, "cluster-1");
        assertNull(prop.get(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_GROUP_ID));
        assertEquals("cluster-1", prop.get(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_KAFKA_CLUSTER_ID));

    }

    @Test
    public void testNoOverrideWarning() {
        Map<String, ? super String> props = new HashMap<>();
        assertEquals(
                Optional.empty(),
                ConnectUtils.ensurePropertyAndGetWarning(props, "key", "value", "because i say so", true)
        );
        assertEquals("value", props.get("key"));

        props.clear();
        assertEquals(
                Optional.empty(),
                ConnectUtils.ensurePropertyAndGetWarning(props, "key", "value", "because i say so", false)
        );
        assertEquals("value", props.get("key"));

        props.clear();
        props.put("key", "value");
        assertEquals(
                Optional.empty(),
                ConnectUtils.ensurePropertyAndGetWarning(props, "key", "value", "because i say so", true)
        );
        assertEquals("value", props.get("key"));

        props.clear();
        props.put("key", "VALUE");
        assertEquals(
                Optional.empty(),
                ConnectUtils.ensurePropertyAndGetWarning(props, "key", "value", "because i say so", false)
        );
        assertEquals("VALUE", props.get("key"));
    }

    @Test
    public void testOverrideWarning() {
        Map<String, ? super String> props = new HashMap<>();
        props.put("\u1984", "little brother");
        String expectedWarning = "The value 'little brother' for the '\u1984' property will be ignored as it cannot be overridden "
                + "thanks to newly-introduced federal legislation. "
                + "The value 'big brother' will be used instead.";
        assertEquals(
                Optional.of(expectedWarning),
                ConnectUtils.ensurePropertyAndGetWarning(
                        props,
                        "\u1984",
                        "big brother",
                        "thanks to newly-introduced federal legislation",
                        false)
        );
        assertEquals(Collections.singletonMap("\u1984", "big brother"), props);

        props.clear();
        props.put("\u1984", "BIG BROTHER");
        expectedWarning = "The value 'BIG BROTHER' for the '\u1984' property will be ignored as it cannot be overridden "
                + "thanks to newly-introduced federal legislation. "
                + "The value 'big brother' will be used instead.";
        assertEquals(
                Optional.of(expectedWarning),
                ConnectUtils.ensurePropertyAndGetWarning(
                        props,
                        "\u1984",
                        "big brother",
                        "thanks to newly-introduced federal legislation",
                        true)
        );
        assertEquals(Collections.singletonMap("\u1984", "big brother"), props);
    }

}
