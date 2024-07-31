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

import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.metadata.BrokerRegistration;

public class DefaultNodeRuntimeInfoGetter implements NodeRuntimeInfoGetter {
    private final ClusterControlManager clusterControlManager;
    private final StreamControlManager streamControlManager;

    public DefaultNodeRuntimeInfoGetter(ClusterControlManager clusterControlManager, StreamControlManager streamControlManager) {
        this.clusterControlManager = clusterControlManager;
        this.streamControlManager = streamControlManager;
    }

    @Override
    public String state(int nodeId) {
        BrokerRegistration brokerRegistration = clusterControlManager.registration(nodeId);
        if (brokerRegistration == null) {
            return NodeState.UNKNOWN.name();
        }
        if (brokerRegistration.fenced()) {
            return NodeState.FENCED.name();
        }
        if (brokerRegistration.inControlledShutdown()) {
            return NodeState.CONTROLLED_SHUTDOWN.name();
        }
        return NodeState.ACTIVE.name();
    }

    @Override
    public boolean hasOpeningStreams(int nodeId) {
        return !streamControlManager.getOpeningStreams(nodeId).isEmpty();
    }
}
