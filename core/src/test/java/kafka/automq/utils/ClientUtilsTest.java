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

package kafka.automq.utils;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientUtilsTest {

    @Test
    public void clusterConfigIncludesListenerSecuritySettings() {
        Properties props = baseBrokerConfig("INTERNAL", SecurityProtocol.SASL_SSL, "SCRAM-SHA-512", 19093);
        String listenerLower = "internal";
        String mechanismLower = "scram-sha-512";
        props.put("listener.name." + listenerLower + ".ssl.keystore.location", "/path/keystore.jks");
        props.put("listener.name." + listenerLower + ".ssl.truststore.location", "/path/truststore.jks");
        props.put("listener.name." + listenerLower + "." + mechanismLower + ".sasl.jaas.config", "listener-jaas");
        props.put("ssl.truststore.password", "secret");

        KafkaConfig kafkaConfig = KafkaConfig.fromProps(props);
        Properties clientProps = ClientUtils.clusterClientBaseConfig(kafkaConfig);

        assertEquals("broker.local:19093", clientProps.getProperty("bootstrap.servers"));
        assertEquals(SecurityProtocol.SASL_SSL.name(), clientProps.getProperty("security.protocol"));
        assertEquals("SCRAM-SHA-512", clientProps.getProperty("sasl.mechanism"));
        assertEquals("/path/keystore.jks", clientProps.get("ssl.keystore.location"));
        assertEquals("/path/truststore.jks", clientProps.get("ssl.truststore.location"));
        Object jaasConfig = clientProps.get("sasl.jaas.config");
        assertNotNull(jaasConfig);
        assertEquals("listener-jaas", jaasConfig.toString());
        Object truststorePassword = clientProps.get("ssl.truststore.password");
        assertNotNull(truststorePassword);
        if (truststorePassword instanceof Password) {
            assertEquals("secret", ((Password) truststorePassword).value());
        } else {
            assertEquals("secret", truststorePassword.toString());
        }
    }

    @Test
    public void setsSaslMechanismWhenAbsentInListenerConfigs() {
        Properties props = baseBrokerConfig("INTERNAL", SecurityProtocol.SASL_SSL, "SCRAM-SHA-256", 19094);
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(props);

        Properties clientProps = ClientUtils.clusterClientBaseConfig(kafkaConfig);

        assertEquals("SCRAM-SHA-256", clientProps.getProperty("sasl.mechanism"));
    }

    @Test
    public void throwsWhenInterBrokerEndpointMissing() {
        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        ListenerName listenerName = new ListenerName("INTERNAL");
        when(kafkaConfig.interBrokerListenerName()).thenReturn(listenerName);

        List<EndPoint> endpoints = new ArrayList<>();
        endpoints.add(new EndPoint("broker.local", 9092, new ListenerName("EXTERNAL"), SecurityProtocol.PLAINTEXT));
        when(kafkaConfig.effectiveAdvertisedBrokerListeners()).thenReturn(scalaEndpoints(endpoints));

        assertThrows(IllegalArgumentException.class, () -> ClientUtils.clusterClientBaseConfig(kafkaConfig));
    }

    private Properties baseBrokerConfig(String listenerName,
                                        SecurityProtocol securityProtocol,
                                        String saslMechanism,
                                        int port) {
        Properties props = kafka.utils.TestUtils.createDummyBrokerConfig();
        String listener = listenerName.toUpperCase(Locale.ROOT);
        String listenerLower = listenerName.toLowerCase(Locale.ROOT);
        props.put(SocketServerConfigs.LISTENERS_CONFIG, listener + "://broker.local:" + port);
        props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listener + "://broker.local:" + port);
        props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, listener + ":" + securityProtocol.name() + ",CONTROLLER:PLAINTEXT");
        props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, listener);
        props.remove(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG);
        props.put(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, saslMechanism);
        props.put("listener.name." + listenerLower + ".sasl.enabled.mechanisms", saslMechanism);
        return props;
    }

    private scala.collection.Seq<EndPoint> scalaEndpoints(List<EndPoint> endpoints) {
        return scala.jdk.javaapi.CollectionConverters.asScala(endpoints).toSeq();
    }
}
