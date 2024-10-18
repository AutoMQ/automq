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

package org.apache.kafka.connect.mirror;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AutoMQIdentityReplicationPolicy is a custom implementation of the ReplicationPolicy interface that allows for the
 * configuration of the offset-syncs-topic, checkpoints-topic, and heartbeats-topic via environment variables.
 * <p>
 * See more details from KIP-690.
 */
public class AutoMQIdentityReplicationPolicy extends IdentityReplicationPolicy {
    private static final Logger log = LoggerFactory.getLogger(AutoMQIdentityReplicationPolicy.class);

    private static final String OFFSET_SYNC_TOPIC_ENV_KEY = "OFFSET_SYNCS_TOPIC";
    private static final String CHECKPOINTS_TOPIC_ENV_KEY = "CHECKPOINTS_TOPIC";
    private static final String HEARTBEATS_TOPIC_ENV_KEY = "HEARTBEATS_TOPIC";

    @Override
    public String offsetSyncsTopic(String clusterAlias) {
        String offsetSyncsTopic = System.getenv(OFFSET_SYNC_TOPIC_ENV_KEY);
        if (offsetSyncsTopic == null) {
            return super.offsetSyncsTopic(clusterAlias);
        }
        return offsetSyncsTopic;
    }

    @Override
    public String checkpointsTopic(String clusterAlias) {
        String checkpointsTopic = System.getenv(CHECKPOINTS_TOPIC_ENV_KEY);
        if (checkpointsTopic == null) {
            return super.checkpointsTopic(clusterAlias);
        }
        return checkpointsTopic;
    }

    @Override
    public String heartbeatsTopic() {
        String heartbeatsTopic = System.getenv(HEARTBEATS_TOPIC_ENV_KEY);
        if (heartbeatsTopic == null) {
            return super.heartbeatsTopic();
        }
        return heartbeatsTopic;
    }

    @Override
    public boolean isCheckpointsTopic(String topic) {
        String checkpointsTopic = System.getenv(CHECKPOINTS_TOPIC_ENV_KEY);
        return super.isCheckpointsTopic(topic) || topic.equals(checkpointsTopic);
    }

    @Override
    public boolean isHeartbeatsTopic(String topic) {
        return super.isHeartbeatsTopic(topic) || topic.equals(heartbeatsTopic());
    }

    @Override
    public boolean isMM2InternalTopic(String topic) {
        return super.isMM2InternalTopic(topic) || isHeartbeatsTopic(topic) || isCheckpointsTopic(topic);
    }
}
