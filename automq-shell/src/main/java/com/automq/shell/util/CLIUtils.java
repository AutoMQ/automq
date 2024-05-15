/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.shell.util;

import java.util.Collections;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

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
            logContext
        );
    }
}
