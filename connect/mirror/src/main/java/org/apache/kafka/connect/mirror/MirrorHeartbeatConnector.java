/*
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

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.List;
import java.util.Collections;

/** Emits heartbeats to Kafka.
 */
public class MirrorHeartbeatConnector extends SourceConnector {
    private MirrorConnectorConfig config;
    private Scheduler scheduler;

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        scheduler = new Scheduler(MirrorHeartbeatConnector.class, config.adminTimeout());
        scheduler.execute(this::createInternalTopics, "creating internal topics");
    }

    @Override
    public void stop() {
        Utils.closeQuietly(scheduler, "scheduler");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorHeartbeatTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // just need a single task
        return Collections.singletonList(config.originalsStrings());
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1";
    }

    private void createInternalTopics() {
        MirrorUtils.createSinglePartitionCompactedTopic(config.heartbeatsTopic(),
            config.heartbeatsTopicReplicationFactor(), config.targetAdminConfig());
    }
}
