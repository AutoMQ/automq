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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.errors.s3.NodeLockedException;
import org.apache.kafka.common.errors.s3.UnregisterNodeWithOpenStreamsException;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;
import org.apache.kafka.common.message.AutomqRegisterNodeRequestData;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.AutomqGetNodesRequest;
import org.apache.kafka.common.requests.s3.AutomqRegisterNodeRequest;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeControlManager.class);
    private static final String KEY_PREFIX = "__automq_node/";

    final TimelineHashMap<Integer, NodeMetadata> nodeMetadataMap;
    final TimelineHashSet<Integer> lockedNodes;

    private final NodeRuntimeInfoManager nodeRuntimeInfoManager;

    public NodeControlManager(SnapshotRegistry registry, NodeRuntimeInfoManager nodeRuntimeInfoManager) {
        this.nodeMetadataMap = new TimelineHashMap<>(registry, 100);
        this.lockedNodes = new TimelineHashSet<>(registry, 100);
        this.nodeRuntimeInfoManager = nodeRuntimeInfoManager;
    }

    public ControllerResult<AutomqRegisterNodeResponseData> register(AutomqRegisterNodeRequest req) {
        AutomqRegisterNodeResponseData resp = new AutomqRegisterNodeResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        int nodeId = req.data().nodeId();
        long nodeEpoch = req.data().nodeEpoch();
        String walConfig = req.data().walConfig();
        Map<String, String> tags = req.data().tags().valuesList()
            .stream()
            .collect(Collectors.toMap(AutomqRegisterNodeRequestData.Tag::key, AutomqRegisterNodeRequestData.Tag::value));

        NodeMetadata oldNodeMetadata = nodeMetadataMap.get(nodeId);
        if (oldNodeMetadata != null && oldNodeMetadata.getNodeEpoch() > nodeEpoch) {
            resp.setErrorCode(Errors.NODE_EPOCH_EXPIRED.code());
            LOGGER.warn("[REGISTER_NODE] expired node epoch, nodeId={}, request nodeEpoch={} less than current {}",
                nodeEpoch, nodeEpoch, oldNodeMetadata.getNodeEpoch());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        NodeMetadata newNodeMetadata = new NodeMetadata(nodeId, nodeEpoch, walConfig, tags);
        records.add(registerNodeRecord(nodeId, newNodeMetadata));
        return ControllerResult.of(records, resp);
    }

    public ControllerResult<AutomqGetNodesResponseData> getMetadata(AutomqGetNodesRequest req) {
        List<Integer> nodeIds = req.data().nodeIds();
        if (nodeIds.isEmpty()) {
            nodeIds = new ArrayList<>(nodeMetadataMap.keySet());
        }
        List<NodeMetadata> nodeMetadataList = new ArrayList<>();
        for (Integer nodeId : nodeIds) {
            NodeMetadata nodeMetadata = nodeMetadataMap.get(nodeId);
            if (nodeMetadata == null) {
                LOGGER.warn("[GET_NODES] cannot find NodeMetadata for nodeId={}", nodeId);
                continue;
            }
            nodeMetadataList.add(nodeMetadata);
        }
        AutomqGetNodesResponseData resp = new AutomqGetNodesResponseData().setNodes(
            nodeMetadataList.stream().map(src -> {
                AutomqGetNodesResponseData.NodeMetadata metadata = src.to();
                int nodeId = src.getNodeId();
                metadata.setState(state(nodeId).name());
                metadata.setHasOpeningStreams(hasOpeningStreams(nodeId));
                return metadata;
            }).collect(Collectors.toList())
        );
        return ControllerResult.of(Collections.emptyList(), resp);
    }

    public Collection<NodeMetadata> getMetadata() {
        return nodeMetadataMap.values();
    }

    public NodeState state(int nodeId) {
        return nodeRuntimeInfoManager.state(nodeId);
    }

    /**
     * Note: It is costly to check if a node has opening streams, so it is recommended to use this method only when necessary.
     */
    public boolean hasOpeningStreams(int nodeId) {
        return nodeRuntimeInfoManager.hasOpeningStreams(nodeId);
    }

    public synchronized void replay(KVRecord kvRecord) {
        for (KVRecord.KeyValue kv : kvRecord.keyValues()) {
            if (!(kv.key() != null && kv.key().startsWith(KEY_PREFIX))) {
                continue;
            }
            try {
                int nodeId = Integer.parseInt(kv.key().substring(KEY_PREFIX.length()));
                NodeMetadata nodeMetadata = NodeMetadataCodec.decode(kv.value());
                nodeMetadataMap.put(nodeId, nodeMetadata);
                if ("CLOSED".equals(nodeMetadata.getTags().getOrDefault("CIRCUIT_BREAKER", "CLOSED"))) {
                    nodeRuntimeInfoManager.unlock(nodeId);
                    lockedNodes.remove(nodeId);
                } else {
                    nodeRuntimeInfoManager.lock(nodeId);
                    lockedNodes.add(nodeId);
                }
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay NodeMetadata from KV fail", e);
            }
        }
    }

    public synchronized Set<Integer> lockedNodes() {
        return new HashSet<>(lockedNodes);
    }

    ApiMessageAndVersion registerNodeRecord(int nodeId, NodeMetadata newNodeMetadata) {
        KVRecord kvRecord = new KVRecord().setKeyValues(List.of(
            new KVRecord.KeyValue()
                .setKey(KEY_PREFIX + nodeId)
                .setValue(NodeMetadataCodec.encode(newNodeMetadata))
        ));
        return new ApiMessageAndVersion(kvRecord, (short) 0);
    }

    public void replay(RemoveKVRecord kvRecord) {
        for (String key : kvRecord.keys()) {
            if (!key.startsWith(KEY_PREFIX)) {
                continue;
            }
            try {
                int nodeId = Integer.parseInt(key.substring(KEY_PREFIX.length()));
                nodeMetadataMap.remove(nodeId);
                nodeRuntimeInfoManager.unlock(nodeId);
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay NodeMetadata from KV fail", e);
            }
        }
    }

    public ApiMessageAndVersion unregisterNodeRecord(int nodeId) {
        if (hasOpeningStreams(nodeId)) {
            throw new UnregisterNodeWithOpenStreamsException(String.format("Node %d has opening streams", nodeId));
        }
        if (lockedNodes.contains(nodeId)) {
            throw new NodeLockedException(String.format("Node %d is locked", nodeId));
        }
        RemoveKVRecord removeKVRecord = new RemoveKVRecord().setKeys(List.of(KEY_PREFIX + nodeId));
        return new ApiMessageAndVersion(removeKVRecord, (short) 0);
    }
}
