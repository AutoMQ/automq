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
