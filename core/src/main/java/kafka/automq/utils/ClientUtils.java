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
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static scala.jdk.javaapi.CollectionConverters.asJava;

public class ClientUtils {
    public static Properties clusterClientBaseConfig(KafkaConfig kafkaConfig) {
        ListenerName listenerName = kafkaConfig.interBrokerListenerName();
        List<EndPoint> endpoints = asJava(kafkaConfig.effectiveAdvertisedBrokerListeners());
        
        EndPoint endpoint = endpoints.stream()
            .filter(e -> listenerName.equals(e.listenerName()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Cannot find " + listenerName + " in endpoints " + endpoints));

        SecurityProtocol securityProtocol = kafkaConfig.interBrokerSecurityProtocol();
        Map<String, Object> parsedConfigs = kafkaConfig.valuesWithPrefixOverride(listenerName.configPrefix());
        
        // mirror ChannelBuilders#channelBuilderConfigs
        kafkaConfig.originals().entrySet().stream()
            .filter(entry -> !parsedConfigs.containsKey(entry.getKey()))
            // exclude already parsed listener prefix configs
            .filter(entry -> {
                String key = entry.getKey();
                String prefix = listenerName.configPrefix();
                if (!key.startsWith(prefix)) return true;
                return !parsedConfigs.containsKey(key.substring(prefix.length()));
            })
            // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
            .filter(entry -> {
                String key = entry.getKey();
                int dotIndex = key.indexOf('.');
                if (dotIndex < 0) return true;
                return !parsedConfigs.containsKey(key.substring(dotIndex + 1));
            })
            .forEach(entry -> parsedConfigs.put(entry.getKey(), entry.getValue()));

        Properties clientConfig = new Properties();
        parsedConfigs.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .filter(entry -> isSecurityKey(entry.getKey(), listenerName))
            .forEach(entry -> clientConfig.put(entry.getKey(), entry.getValue()));

        String interBrokerSaslMechanism = kafkaConfig.saslMechanismInterBrokerProtocol();
        if (interBrokerSaslMechanism != null && !interBrokerSaslMechanism.isEmpty()) {
            kafkaConfig.originalsWithPrefix(listenerName.saslMechanismConfigPrefix(interBrokerSaslMechanism)).entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> clientConfig.put(entry.getKey(), entry.getValue()));
            clientConfig.putIfAbsent("sasl.mechanism", interBrokerSaslMechanism);
        }

        clientConfig.put("security.protocol", securityProtocol.toString());
        clientConfig.put("bootstrap.servers", String.format("%s:%d", endpoint.host(), endpoint.port()));
        return clientConfig;
    }

    // Filter out non-security broker options (e.g. compression.type, log.retention.hours) so internal clients
    // only inherit listener-specific SSL/SASL settings.
    private static boolean isSecurityKey(String key, ListenerName listenerName) {
        return key.startsWith("ssl.")
            || key.startsWith("sasl.")
            || key.startsWith("security.")
            || key.startsWith(listenerName.configPrefix());
    }
}
