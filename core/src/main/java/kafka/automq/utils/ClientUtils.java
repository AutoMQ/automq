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

package kafka.automq.utils;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.network.ListenerName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static scala.jdk.javaapi.CollectionConverters.asJava;

public class ClientUtils {
    public static Properties clusterClientBaseConfig(KafkaConfig kafkaConfig) {
        ListenerName listenerName = kafkaConfig.interBrokerListenerName();
        List<EndPoint> endpoints = asJava(kafkaConfig.effectiveAdvertisedBrokerListeners());
        Optional<EndPoint> endpointOpt = endpoints.stream().filter(e -> listenerName.equals(e.listenerName())).findFirst();
        if (endpointOpt.isEmpty()) {
            throw new IllegalArgumentException("Cannot find " + listenerName + " in endpoints " + endpoints);
        }
        EndPoint endpoint = endpointOpt.get();
        Map<String, ?> securityConfig = kafkaConfig.originalsWithPrefix(listenerName.saslMechanismConfigPrefix(kafkaConfig.saslMechanismInterBrokerProtocol()));
        Properties clientConfig = new Properties();
        clientConfig.putAll(securityConfig);
        clientConfig.put("security.protocol", kafkaConfig.interBrokerSecurityProtocol().toString());
        clientConfig.put("sasl.mechanism", kafkaConfig.saslMechanismInterBrokerProtocol());
        clientConfig.put("bootstrap.servers", String.format("%s:%d", endpoint.host(), endpoint.port()));
        return clientConfig;
    }

}
