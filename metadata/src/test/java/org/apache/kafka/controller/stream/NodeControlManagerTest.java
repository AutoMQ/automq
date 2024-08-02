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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.AutomqGetNodesRequestData;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;
import org.apache.kafka.common.message.AutomqRegisterNodeRequestData;
import org.apache.kafka.common.message.AutomqRegisterNodeRequestData.TagCollection;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.AutomqGetNodesRequest;
import org.apache.kafka.common.requests.s3.AutomqRegisterNodeRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class NodeControlManagerTest {
    SnapshotRegistry registry;
    NodeRuntimeInfoGetter nodeRuntimeInfoGetter;

    NodeControlManager nodeControlManager;

    @BeforeEach
    public void setup() {
        registry = new SnapshotRegistry(new LogContext());
        nodeRuntimeInfoGetter = mock(NodeRuntimeInfoGetter.class);

        nodeControlManager = new NodeControlManager(registry, nodeRuntimeInfoGetter);
    }

    @Test
    public void testRegister() {
        ControllerResult<AutomqRegisterNodeResponseData> rst = nodeControlManager.register(
            new AutomqRegisterNodeRequest(new AutomqRegisterNodeRequestData()
                .setNodeId(0)
                .setNodeEpoch(1L)
                .setWalConfig("wal1")
                .setTags(tags(Map.of("k1", "v1"))),
                (short) 0));
        replay(nodeControlManager, rst.records());
        assertEquals(Errors.NONE.code(), rst.response().errorCode());
        assertTrue(nodeControlManager.nodeMetadataMap.containsKey(0));

        // try register old node
        rst = nodeControlManager.register(
            new AutomqRegisterNodeRequest(new AutomqRegisterNodeRequestData()
                .setNodeId(0)
                .setNodeEpoch(0L)
                .setWalConfig("wal1")
                .setTags(tags(Map.of("k1", "v1"))),
                (short) 0));
        assertEquals(Errors.NODE_EPOCH_EXPIRED.code(), rst.response().errorCode());

        // update
        rst = nodeControlManager.register(
            new AutomqRegisterNodeRequest(new AutomqRegisterNodeRequestData()
                .setNodeId(0)
                .setNodeEpoch(2L)
                .setWalConfig("wal2")
                .setTags(tags(Map.of("k1", "v2"))),
                (short) 0));
        replay(nodeControlManager, rst.records());
        assertEquals(Errors.NONE.code(), rst.response().errorCode());
        assertTrue(nodeControlManager.nodeMetadataMap.containsKey(0));

        when(nodeRuntimeInfoGetter.hasOpeningStreams(eq(0))).thenReturn(true);
        when(nodeRuntimeInfoGetter.state(eq(0))).thenReturn(NodeState.FENCED);

        ControllerResult<AutomqGetNodesResponseData> getRst = nodeControlManager.getMetadata(
            new AutomqGetNodesRequest(new AutomqGetNodesRequestData().setNodeIds(List.of(0, 1)),
                (short) 0
            ));
        assertEquals(Errors.NONE.code(), getRst.response().errorCode());
        List<AutomqGetNodesResponseData.NodeMetadata> nodes = getRst.response().nodes();
        assertEquals(1, nodes.size());
        assertEquals(0, nodes.get(0).nodeId());
        assertEquals(2L, nodes.get(0).nodeEpoch());
        assertEquals("wal2", nodes.get(0).walConfig());
    }

    AutomqRegisterNodeRequestData.TagCollection tags(Map<String, String> tags) {
        AutomqRegisterNodeRequestData.TagCollection tagCollection = new TagCollection();
        tags.forEach((k, v) -> tagCollection.add(new AutomqRegisterNodeRequestData.Tag().setKey(k).setValue(v)));
        return tagCollection;
    }

    void replay(NodeControlManager manager, List<ApiMessageAndVersion> records) {
        for (ApiMessage record : records.stream().map(ApiMessageAndVersion::message).collect(Collectors.toList())) {
            MetadataRecordType type = MetadataRecordType.fromId(record.apiKey());
            switch (type) {
                case KVRECORD:
                    manager.replay((KVRecord) record);
                    break;
                case REMOVE_KVRECORD:
                    manager.replay((RemoveKVRecord) record);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }
}
