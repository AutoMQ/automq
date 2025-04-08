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

import kafka.autobalancer.config.StaticAutoBalancerConfig;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.network.SocketServerConfigs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

@Timeout(60)
@Tag("S3Unit")
public class AutoBalancerMetricsReporterTest {

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
}
