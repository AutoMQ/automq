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
package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.AppInfoParser;

public class ServerInfo {
    private final String version;
    private final String commit;
    private final String kafkaClusterId;

    @JsonCreator
    private ServerInfo(@JsonProperty("version") String version,
                       @JsonProperty("commit") String commit,
                       @JsonProperty("kafka_cluster_id") String kafkaClusterId) {
        this.version = version;
        this.commit = commit;
        this.kafkaClusterId = kafkaClusterId;
    }

    public ServerInfo(String kafkaClusterId) {
        this(AppInfoParser.getVersion(), AppInfoParser.getCommitId(), kafkaClusterId);
    }

    @JsonProperty
    public String version() {
        return version;
    }

    @JsonProperty
    public String commit() {
        return commit;
    }

    @JsonProperty("kafka_cluster_id")
    public String clusterId() {
        return kafkaClusterId;
    }
}
