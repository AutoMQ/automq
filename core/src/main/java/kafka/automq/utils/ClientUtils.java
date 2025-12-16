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
import java.util.Optional;
import java.util.Properties;

import static kafka.automq.utils.ClientUtils.isSecurityKey;
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
            .filter(entry -> !isExcludedListenerConfig(entry, listenerName, parsedConfigs))
            .filter(entry -> !isMechanismConfigShadowed(entry, parsedConfigs))
            .forEach(entry -> parsedConfigs.put(entry.getKey(), entry.getValue()));

        Properties clientConfig = new Properties();
        populateSecurityConfigs(clientConfig, parsedConfigs, listenerName);
        populateSaslConfigs(clientConfig, kafkaConfig, listenerName);
        
        clientConfig.put("security.protocol", securityProtocol.toString());
        clientConfig.put("bootstrap.servers", endpoint.host() + ":" + endpoint.port());
        return clientConfig;
    }

    private static boolean isExcludedListenerConfig(Map.Entry<String, Object> entry, 
                                                   ListenerName listenerName, 
                                                   Map<String, Object> parsedConfigs) {
        String key = entry.getKey();
        String prefix = listenerName.configPrefix();
        if (!key.startsWith(prefix)) return false;
        
        String suffixKey = key.substring(prefix.length());
        return parsedConfigs.containsKey(suffixKey);
    }

    private static boolean isMechanismConfigShadowed(Map.Entry<String, Object> entry, 
                                                    Map<String, Object> parsedConfigs) {
        String key = entry.getKey();
        int dotIndex = key.indexOf('.');
        if (dotIndex < 0) return false;
        
        return parsedConfigs.containsKey(key.substring(dotIndex + 1));
    }

    private static void populateSecurityConfigs(Properties clientConfig, 
                                               Map<String, Object> parsedConfigs, 
                                               ListenerName listenerName) {
        parsedConfigs.entrySet().stream()
            .filter(ClientUtils::hasNonNullValue)
            .filter(entry -> isSecurityKey(entry.getKey(), listenerName))
            .forEach(entry -> clientConfig.put(entry.getKey(), entry.getValue()));
    }

    private static void populateSaslConfigs(Properties clientConfig, 
                                           KafkaConfig kafkaConfig, 
                                           ListenerName listenerName) {
        String interBrokerSaslMechanism = kafkaConfig.saslMechanismInterBrokerProtocol();
        if (interBrokerSaslMechanism == null || interBrokerSaslMechanism.isEmpty()) {
            return;
        }

        kafkaConfig.originalsWithPrefix(listenerName.saslMechanismConfigPrefix(interBrokerSaslMechanism))
            .entrySet().stream()
            .filter(ClientUtils::hasNonNullValue)
            .forEach(entry -> clientConfig.put(entry.getKey(), entry.getValue()));
            
        clientConfig.putIfAbsent("sasl.mechanism", interBrokerSaslMechanism);
    }

    private static boolean hasNonNullValue(Map.Entry<String, ?> entry) {
        return entry.getValue() != null;
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
