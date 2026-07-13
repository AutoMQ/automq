/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.metadata;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites unavailable kafka-go partition metadata to route requests to the controller-selected first replica.
 */
public final class KafkaGoMetadataResponseRewriter implements Reconfigurable {

    public static final Set<String> RECONFIGURABLE_CONFIGS =
        Set.of(AutoMQConfig.KAFKA_GO_METADATA_COMPATIBILITY_ENABLED_CONFIG);

    private static final String KAFKA_GO_CLIENT_ID = "github.com/segmentio/kafka-go";
    private volatile boolean enabled;

    public KafkaGoMetadataResponseRewriter(KafkaConfig config) {
        this.enabled = config.kafkaGoMetadataCompatibilityEnabled();
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (configs.containsKey(AutoMQConfig.KAFKA_GO_METADATA_COMPATIBILITY_ENABLED_CONFIG)) {
            enabled = (Boolean) configs.get(AutoMQConfig.KAFKA_GO_METADATA_COMPATIBILITY_ENABLED_CONFIG);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    /**
     * Rewrites each leader-unavailable partition with replicas for clients using kafka-go's default client ID.
     * All partition fields other than the error code and leader ID are preserved.
     *
     * @param clientId request header client ID
     * @param topics mutable Metadata response topics
     */
    public void rewrite(String clientId, Collection<MetadataResponseTopic> topics) {
        if (!enabled
            || clientId == null
            || (!clientId.isEmpty() && !clientId.contains(KAFKA_GO_CLIENT_ID))) {
            return;
        }
        topics.forEach(topic -> topic.partitions().forEach(partition -> {
            if (partition.errorCode() == Errors.LEADER_NOT_AVAILABLE.code()
                && !partition.replicaNodes().isEmpty()) {
                partition.setErrorCode(Errors.NONE.code());
                partition.setLeaderId(partition.replicaNodes().get(0));
            }
        }));
    }
}
