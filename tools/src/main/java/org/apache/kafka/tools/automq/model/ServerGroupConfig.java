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
package org.apache.kafka.tools.automq.model;

import java.util.List;
import java.util.Map;

/**
 * Contain several server's config
 */
public class ServerGroupConfig {
    final private List<Integer> nodeIdList;

    final private String quorumVoters;

    /**
     * Key is allocated node id and value is listener info
     */
    final private Map<Integer, String> listenerMap;

    public ServerGroupConfig(List<Integer> nodeIdList, String quorumVoters, Map<Integer, String> listenerMap) {
        this.nodeIdList = nodeIdList;
        this.quorumVoters = quorumVoters;
        this.listenerMap = listenerMap;
    }

    public List<Integer> getNodeIdList() {
        return nodeIdList;
    }

    public String getQuorumVoters() {
        return quorumVoters;
    }

    public Map<Integer, String> getListenerMap() {
        return listenerMap;
    }
}