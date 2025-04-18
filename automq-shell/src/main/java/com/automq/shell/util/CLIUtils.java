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

package com.automq.shell.util;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;

public class CLIUtils {
    public static NetworkClient buildNetworkClient(
        String prefix,
        AdminClientConfig config,
        Metrics metrics,
        Time time,
        LogContext logContext) {
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);

        String metricGroupPrefix = prefix + "-channel";

        Selector selector = new Selector(
            NetworkReceive.UNLIMITED,
            config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
            metrics,
            time,
            metricGroupPrefix,
            Collections.emptyMap(),
            false,
            channelBuilder,
            logContext
        );

        String clientId = prefix + "-network-client";
        return new NetworkClient(
            selector,
            new ManualMetadataUpdater(),
            clientId,
            100,
            config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
            config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
            config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
            config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
            config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
            config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
            config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
            time,
            false,
            new ApiVersions(),
            logContext,
            MetadataRecoveryStrategy.NONE
        );
    }
}
