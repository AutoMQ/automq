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

package kafka.automq.failover;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FailoverContext {
    /**
     * Failed node id
     */
    @JsonProperty("n")
    private int nodeId;

    /**
     * Failover target node id
     *
     * @since failover v0
     */
    @JsonProperty("t")
    private int target;

    /**
     * Failed node epoch
     *
     * @since failover v1
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @JsonProperty(value = "e", defaultValue = "0")
    private long nodeEpoch;

    /**
     * WAL configs for failover
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("c")
    private String kraftWalConfigs;

    // for json deserialize
    public FailoverContext() {}

    public FailoverContext(int nodeId, long nodeEpoch, int target, String kraftWalConfigs) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.target = target;
        this.kraftWalConfigs = kraftWalConfigs;
    }

    @JsonIgnore
    public FailedNode getFailedNode() {
        return FailedNode.from(this);
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getTarget() {
        return target;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    public String getKraftWalConfigs() {
        return kraftWalConfigs;
    }

    @Override
    public String toString() {
        return "FailoverContext{" +
            "nodeId=" + nodeId +
            ", target=" + target +
            ", nodeEpoch=" + nodeEpoch +
            ", kraftWalConfigs=" + kraftWalConfigs +
            '}';
    }
}
