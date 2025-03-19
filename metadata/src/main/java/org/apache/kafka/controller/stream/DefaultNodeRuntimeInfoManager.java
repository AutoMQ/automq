/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller.stream;

import org.apache.kafka.controller.BrokerHeartbeatManager;
import org.apache.kafka.controller.ClusterControlManager;

import java.util.concurrent.TimeUnit;

public class DefaultNodeRuntimeInfoManager implements NodeRuntimeInfoManager {
    private static final long SHUTDOWN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(60);

    private final ClusterControlManager clusterControlManager;
    private final StreamControlManager streamControlManager;

    public DefaultNodeRuntimeInfoManager(ClusterControlManager clusterControlManager, StreamControlManager streamControlManager) {
        this.clusterControlManager = clusterControlManager;
        this.streamControlManager = streamControlManager;
    }

    @Override
    public NodeState state(int nodeId) {
        BrokerHeartbeatManager brokerHeartbeatManager = clusterControlManager.getHeartbeatManager();
        if (null == brokerHeartbeatManager) {
            // This controller is not the active controller, so we don't have the heartbeat manager.
            return NodeState.UNKNOWN;
        }
        return brokerHeartbeatManager.brokerState(nodeId, SHUTDOWN_TIMEOUT_NS);
    }

    @Override
    public boolean hasOpeningStreams(int nodeId) {
        return streamControlManager.hasOpeningStreams(nodeId);
    }

    @Override
    public void lock(int nodeId) {
        streamControlManager.lock(nodeId);
    }

    @Override
    public void unlock(int nodeId) {
        streamControlManager.unlock(nodeId);
    }
}
