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

import org.apache.kafka.controller.BrokerControlState;

public enum NodeState {
    /**
     * The node is active and can handle requests.
     */
    ACTIVE,
    /**
     * The node is shut down and cannot handle requests.
     */
    FENCED,
    /**
     * The node is shutting down in a controlled manner.
     * Note: In AutoMQ, this state is different from {@link BrokerControlState#CONTROLLED_SHUTDOWN}. In some cases,
     * a node in {@link BrokerControlState#FENCED} state may still be shutting down in a controlled manner.
     */
    CONTROLLED_SHUTDOWN,
    /**
     * The state of the node is unknown, possibly because it has not yet registered.
     */
    UNKNOWN
}
