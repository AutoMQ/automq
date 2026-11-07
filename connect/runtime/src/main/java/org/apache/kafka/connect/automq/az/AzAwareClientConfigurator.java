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

package org.apache.kafka.connect.automq.az;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public final class AzAwareClientConfigurator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzAwareClientConfigurator.class);

    private AzAwareClientConfigurator() {
    }

    public enum ClientFamily {
        PRODUCER,
        CONSUMER,
        ADMIN
    }

    public static void maybeApplyAz(Map<String, Object> props, ClientFamily family, String roleDescriptor) {
        Optional<String> azOpt = AzMetadataProviderHolder.provider().availabilityZoneId();
        LOGGER.info("AZ-aware client.id configuration for role {}: resolved availability zone id '{}'",
            roleDescriptor, azOpt.orElse("unknown"));
        if (azOpt.isEmpty()) {
            LOGGER.info("Skipping AZ-aware client.id configuration for role {} as no availability zone id is available",
                roleDescriptor);
            return;
        }

        String az = azOpt.get();

        String encodedAz = URLEncoder.encode(az, StandardCharsets.UTF_8);
        String automqClientId;

        if (props.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            Object currentId = props.get(CommonClientConfigs.CLIENT_ID_CONFIG);
            if (currentId instanceof String currentIdStr) {
                automqClientId = "automq_az=" + encodedAz + "&" + currentIdStr;
            } else {
                LOGGER.warn("client.id for role {} is not a string ({});",
                    roleDescriptor, currentId.getClass().getName());
                return;
            }
        } else {
            automqClientId = "automq_az=" + encodedAz;
        }
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, automqClientId);
        LOGGER.info("Applied AZ-aware client.id for role {} -> {}", roleDescriptor, automqClientId);

        if (family == ClientFamily.CONSUMER) {
            LOGGER.info("Applying client.rack configuration for consumer role {} -> {}", roleDescriptor, az);
            Object rackValue = props.get(ConsumerConfig.CLIENT_RACK_CONFIG);
            if (rackValue == null || String.valueOf(rackValue).isBlank()) {
                props.put(ConsumerConfig.CLIENT_RACK_CONFIG, az);
            }
        }
    }

    public static void maybeApplyProducerAz(Map<String, Object> props, String roleDescriptor) {
        maybeApplyAz(props, ClientFamily.PRODUCER, roleDescriptor);
    }

    public static void maybeApplyConsumerAz(Map<String, Object> props, String roleDescriptor) {
        maybeApplyAz(props, ClientFamily.CONSUMER, roleDescriptor);
    }

    public static void maybeApplyAdminAz(Map<String, Object> props, String roleDescriptor) {
        maybeApplyAz(props, ClientFamily.ADMIN, roleDescriptor);
    }
}
