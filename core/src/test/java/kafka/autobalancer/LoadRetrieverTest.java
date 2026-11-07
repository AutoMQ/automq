/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.config.StaticAutoBalancerConfig;
import kafka.autobalancer.model.ClusterModel;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Tag("S3Unit")
public class LoadRetrieverTest {

    @Test
    public void testBrokerChanged() {
        LoadRetriever loadRetriever = Mockito.spy(new LoadRetriever(Mockito.mock(AutoBalancerControllerConfig.class), Mockito.mock(Controller.class), Mockito.mock(ClusterModel.class)));
        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(0).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.0").setPort(9092)).iterator())));
        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(1).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.1").setPort(9093)).iterator())));
        loadRetriever.checkAndCreateConsumer(0);

        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.1:9093,192.168.0.0:9092");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(0).setFenced(BrokerRegistrationFencingChange.FENCE.value()));
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(1).setFenced(BrokerRegistrationFencingChange.FENCE.value()));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertFalse(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(1).setFenced(BrokerRegistrationFencingChange.UNFENCE.value()));
        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.1:9093");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerRegister(new RegisterBrokerRecord().setBrokerId(1).setFenced(false).setEndPoints(
            new RegisterBrokerRecord.BrokerEndpointCollection(List.of(
                new RegisterBrokerRecord.BrokerEndpoint().setHost("192.168.0.2").setPort(9094)).iterator())));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        Assertions.assertEquals(loadRetriever.buildBootstrapServer(), "192.168.0.2:9094");
        Assertions.assertTrue(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertTrue(loadRetriever.hasAvailableBroker());

        loadRetriever.onBrokerUnregister(new UnregisterBrokerRecord().setBrokerId(1));
        Assertions.assertFalse(loadRetriever.hasAvailableBrokerInUse());
        Assertions.assertFalse(loadRetriever.hasAvailableBroker());
    }

    /**
     * Given shared client settings, the load retriever passes them to its Kafka consumer while retaining
     * authoritative values for its internal settings.
     */
    @Test
    public void testMetricsConsumerUsesSharedClientConfig() {
        Password keyStorePassword = new Password("key-store-password");
        Password keyPassword = new Password("key-password");
        Password trustStorePassword = new Password("trust-store-password");
        Map<String, Object> configs = new HashMap<>();
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), SecurityProtocol.SSL.name);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), "/ssl/client.keystore.jks");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), keyStorePassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_KEY_PASSWORD_CONFIG), keyPassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG), "/ssl/client.truststore.jks");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), trustStorePassword);
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), "");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig("future.kafka.config"), "future-value");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "shared:9092");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.CLIENT_ID_CONFIG), "shared-client");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG), "9999");
        configs.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), String.class.getName());
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        LoadRetriever loadRetriever = new LoadRetriever(controllerConfig, Mockito.mock(Controller.class), Mockito.mock(ClusterModel.class));

        Properties consumerProps = loadRetriever.buildConsumerProps("broker:9095");

        Assertions.assertEquals(SecurityProtocol.SSL.name, consumerProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals("/ssl/client.keystore.jks", consumerProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        Assertions.assertEquals(keyStorePassword, consumerProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        Assertions.assertEquals(keyPassword, consumerProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        Assertions.assertEquals("/ssl/client.truststore.jks", consumerProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        Assertions.assertEquals(trustStorePassword, consumerProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        Assertions.assertEquals("", consumerProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        Assertions.assertEquals("future-value", consumerProps.getProperty("future.kafka.config"));
        Assertions.assertEquals("broker:9095", consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertNotEquals("shared-client", consumerProps.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
        Assertions.assertEquals("1000", consumerProps.getProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG));
        Assertions.assertEquals(StringDeserializer.class.getName(), consumerProps.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    }
}
