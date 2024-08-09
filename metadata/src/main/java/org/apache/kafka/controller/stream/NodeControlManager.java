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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeControlManager.class);
    private static final String KEY_PREFIX = "__automq_node/";

    final TimelineHashMap<Integer, NodeMetadata> nodeMetadataMap;

    private final NodeRuntimeInfoGetter nodeRuntimeInfoGetter;

    public NodeControlManager(SnapshotRegistry registry, NodeRuntimeInfoGetter nodeRuntimeInfoGetter) {
        this.nodeMetadataMap = new TimelineHashMap<>(registry, 100);
        this.nodeRuntimeInfoGetter = nodeRuntimeInfoGetter;
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
        records.add(new ApiMessageAndVersion(registerNodeKVRecord(nodeId, newNodeMetadata), (short) 0));
        return ControllerResult.of(records, resp);
    }

    public ControllerResult<AutomqGetNodesResponseData> getMetadata(AutomqGetNodesRequest req) {
        List<Integer> nodeIds = req.data().nodeIds();
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
        return nodeRuntimeInfoGetter.state(nodeId);
    }

    /**
     * Note: It is costly to check if a node has opening streams, so it is recommended to use this method only when necessary.
     */
    public boolean hasOpeningStreams(int nodeId) {
        return nodeRuntimeInfoGetter.hasOpeningStreams(nodeId);
    }

    public void replay(KVRecord kvRecord) {
        for (KVRecord.KeyValue kv : kvRecord.keyValues()) {
            if (!(kv.key() != null && kv.key().startsWith(KEY_PREFIX))) {
                continue;
            }
            try {
                int nodeId = Integer.parseInt(kv.key().substring(KEY_PREFIX.length()));
                NodeMetadata nodeMetadata = NodeMetadataCodec.decode(kv.value());
                nodeMetadataMap.put(nodeId, nodeMetadata);
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay NodeMetadata from KV fail", e);
            }
        }
    }

    private static KVRecord registerNodeKVRecord(int nodeId, NodeMetadata newNodeMetadata) {
        return new KVRecord().setKeyValues(List.of(
            new KVRecord.KeyValue()
                .setKey(KEY_PREFIX + nodeId)
                .setValue(NodeMetadataCodec.encode(newNodeMetadata))
        ));
    }

    public void replay(RemoveKVRecord kvRecord) {
        for (String key : kvRecord.keys()) {
            if (!key.startsWith(KEY_PREFIX)) {
                continue;
            }
            try {
                int nodeId = Integer.parseInt(key.substring(KEY_PREFIX.length()));
                nodeMetadataMap.remove(nodeId);
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay NodeMetadata from KV fail", e);
            }
        }
    }

    public static RemoveKVRecord unregisterNodeKVRecord(int nodeId) {
        return new RemoveKVRecord().setKeys(List.of(KEY_PREFIX + nodeId));
    }
}
