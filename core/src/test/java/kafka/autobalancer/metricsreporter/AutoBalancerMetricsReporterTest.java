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

package kafka.autobalancer.metricsreporter;

import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.config.StaticAutoBalancerConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Timeout(60)
@Tag("S3Unit")
public class AutoBalancerMetricsReporterTest {

    private static class CapturingAutoBalancerMetricsReporter extends AutoBalancerMetricsReporter {
        private Properties producerProps;

        @Override
        protected void createAutoBalancerMetricsProducer(Properties producerProps) {
            this.producerProps = producerProps;
        }
    }

    @Test
    public void testBootstrapServersConfig() {
        AutoBalancerMetricsReporter reporter = Mockito.mock(AutoBalancerMetricsReporter.class);
        Mockito.doCallRealMethod().when(reporter).getBootstrapServers(Mockito.anyMap(), Mockito.anyString());

        // test default config
        StaticAutoBalancerConfig staticConfig = new StaticAutoBalancerConfig(new HashMap<>(), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test default config with multiple listeners
        StaticAutoBalancerConfig staticConfig1 = new StaticAutoBalancerConfig(new HashMap<>(), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,BROKER://127.0.0.1:9092"
        ), staticConfig1.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test illegal listener
        StaticAutoBalancerConfig staticConfig2 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "CONTROLLER"
        ), false);
        Assertions.assertThrows(ConfigException.class, () -> reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://127.0.0.1:9092"
        ), staticConfig2.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test not existed listener
        StaticAutoBalancerConfig staticConfig3 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "BROKER"
        ), false);
        Assertions.assertThrows(ConfigException.class, () -> reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig3.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test valid listener
        StaticAutoBalancerConfig staticConfig4 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "PLAINTEXT"
        ), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test multiple listeners
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,PLAINTEXT://127.0.0.1:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test default hostname
        Assertions.assertEquals("localhost:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,PLAINTEXT://:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

    }

    /**
     * Given shared Auto Balancer mTLS settings, the metrics reporter passes them to its Kafka producer.
     */
    @Test
    public void testMetricsProducerReceivesClientSslConfig() {
        Password keyStorePassword = new Password("key-store-password");
        Password keyPassword = new Password("key-password");
        Password trustStorePassword = new Password("trust-store-password");
        Map<String, Object> configs = new HashMap<>();
        configs.put(SocketServerConfigs.LISTENERS_CONFIG, "INTERNAL_SSL://localhost:9095");
        configs.put(KRaftConfigs.NODE_ID_CONFIG, "1");
        configs.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "INTERNAL_SSL");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), SecurityProtocol.SSL.name);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), "/ssl/client.keystore.jks");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), keyStorePassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEY_PASSWORD_CONFIG), keyPassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG), "/ssl/client.truststore.jks");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), trustStorePassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), "");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig("future.kafka.config"), "future-value");
        CapturingAutoBalancerMetricsReporter reporter = new CapturingAutoBalancerMetricsReporter();

        reporter.configure(configs);

        Assertions.assertEquals("localhost:9095", reporter.producerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals(SecurityProtocol.SSL.name, reporter.producerProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals("/ssl/client.keystore.jks", reporter.producerProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        Assertions.assertEquals(keyStorePassword, reporter.producerProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        Assertions.assertEquals(keyPassword, reporter.producerProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        Assertions.assertEquals("/ssl/client.truststore.jks", reporter.producerProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        Assertions.assertEquals(trustStorePassword, reporter.producerProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        Assertions.assertEquals("", reporter.producerProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        Assertions.assertEquals("future-value", reporter.producerProps.getProperty("future.kafka.config"));
    }

    /**
     * Given a reporter-specific SSL value and no shared value, the metrics producer retains the reporter value.
     */
    @Test
    public void testMetricsProducerRetainsReporterSslConfigWithoutSharedOverride() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SocketServerConfigs.LISTENERS_CONFIG, "INTERNAL_SSL://localhost:9095");
        configs.put(KRaftConfigs.NODE_ID_CONFIG, "1");
        configs.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "INTERNAL_SSL");
        configs.put(AutoBalancerMetricsReporterConfig.config(SslConfigs.SSL_PROTOCOL_CONFIG), "TLSv1.2");
        CapturingAutoBalancerMetricsReporter reporter = new CapturingAutoBalancerMetricsReporter();

        reporter.configure(configs);

        Assertions.assertEquals("TLSv1.2", reporter.producerProps.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    /**
     * Given reporter-specific and shared SSL values, the metrics producer uses the reporter-specific value.
     */
    @Test
    public void testReporterConfigOverridesSharedClientConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SocketServerConfigs.LISTENERS_CONFIG, "INTERNAL_SSL://localhost:9095");
        configs.put(KRaftConfigs.NODE_ID_CONFIG, "1");
        configs.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "INTERNAL_SSL");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_PROTOCOL_CONFIG), "TLSv1.2");
        configs.put(AutoBalancerMetricsReporterConfig.config(SslConfigs.SSL_PROTOCOL_CONFIG), "TLSv1.3");
        CapturingAutoBalancerMetricsReporter reporter = new CapturingAutoBalancerMetricsReporter();

        reporter.configure(configs);

        Assertions.assertEquals("TLSv1.3", reporter.producerProps.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
    }
}
