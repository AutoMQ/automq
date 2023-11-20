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
package org.apache.kafka.controller.stream;

import org.apache.kafka.common.metadata.FailoverContextRecord;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.stream.FailoverStatus;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FailoverControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverControlManager.class);
    private static final int MAX_VOLUME_ATTACH_COUNT = 1;
    private final QuorumController quorumController;
    private final ClusterControlManager clusterControlManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("failover-controller", true));
    /**
     * failover contexts: failedNodeId -> context
     */
    private final TimelineHashMap<Integer, FailoverContextRecord> failoverContexts;
    private List<FailedNode> failedNodes;
    /**
     * attached: failedNodeId -> context with targetNodeId
     */
    private final Map<Integer, FailoverContextRecord> attached = new ConcurrentHashMap<>();

    public FailoverControlManager(
            QuorumController quorumController,
            ClusterControlManager clusterControlManager,
            SnapshotRegistry registry, boolean failoverEnable) {
        this.quorumController = quorumController;
        this.clusterControlManager = clusterControlManager;
        this.failoverContexts = new TimelineHashMap<>(registry, 0);
        if (failoverEnable) {
            this.scheduler.scheduleWithFixedDelay(this::runFailoverTask, 1, 1, TimeUnit.SECONDS);
        }
    }

    void runFailoverTask() {
        if (!quorumController.isActive()) {
            return;
        }
        try {
            scanFailedNodes();
            this.quorumController.failover(new ControllerRequestContext(null, null, OptionalLong.empty())).get();
            backgroundAttach();
        } catch (Throwable e) {
            LOGGER.error("run failover task failed", e);
        }
    }

    void scanFailedNodes() {
        // TODO: run command to get failed nodes
    }


    public ControllerResult<Void> failover() {
        // code in run should be non-blocking
        List<ApiMessageAndVersion> records = new ArrayList<>();
        List<FailedNode> failedNodes = this.failedNodes;
        addNewContext(failedNodes, records);
        doFailover(records);
        complete(failedNodes, records);
        return ControllerResult.of(records, null);
    }

    private void addNewContext(List<FailedNode> failedNodes, List<ApiMessageAndVersion> records) {
        for (FailedNode failedNode : failedNodes) {
            if (failoverContexts.containsKey(failedNode.getNodeId())) {
                // the failed node is already in failover mode, skip
                continue;
            }
            records.add(new ApiMessageAndVersion(
                    new FailoverContextRecord()
                            .setFailedNodeId(failedNode.getNodeId())
                            .setVolumeId(failedNode.getVolumeId())
                            .setStatus(FailoverStatus.WAITING.name()),
                    (short) 0));
        }
    }

    private void doFailover(List<ApiMessageAndVersion> records) {
        for (FailoverContextRecord record : attached.values()) {
            records.add(new ApiMessageAndVersion(record, (short) 0));
            failoverContexts.put(record.failedNodeId(), record);
        }
        attached.clear();
    }

    private void complete(List<FailedNode> failedNodes, List<ApiMessageAndVersion> records) {
        Set<Integer> failedNodeIdSet = failedNodes.stream()
                .map(FailedNode::getNodeId)
                .collect(Collectors.toSet());
        failoverContexts.forEach((nodeId, context) -> {
            if (!failedNodeIdSet.contains(nodeId)) {
                FailoverContextRecord completedRecord = context.duplicate();
                completedRecord.setStatus(FailoverStatus.DONE.name());
                // the target node already complete the recover and delete the volume, so remove the failover context
                records.add(new ApiMessageAndVersion(completedRecord, (short) 0));
            }
        });
    }

    /**
     * Select an alive node to perform failover and move the FailoverContext status.
     */

    void backgroundAttach() {
        try {
            backgroundAttach0();
        } catch (Throwable e) {
            LOGGER.error("failover background attach failed", e);
        }
    }

    void backgroundAttach0() {
        Map<Integer, Long> attachedCounts = new HashMap<>(attached.values().stream()
                .collect(Collectors.groupingBy(FailoverContextRecord::targetNodeId, Collectors.counting())));

        List<BrokerRegistration> brokers = clusterControlManager.getActiveBrokers();
        if (brokers.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> attachCfList = new ArrayList<>();
        int attachIndex = 0;
        for (FailoverContextRecord context : failoverContexts.values()) {
            int failedNodeId = context.failedNodeId();
            if (!FailoverStatus.WAITING.name().equals(context.status())) {
                continue;
            }
            if (attached.containsKey(context.failedNodeId())) {
                continue;
            }
            for (int i = 0; i < brokers.size(); i++, attachIndex++) {
                BrokerRegistration broker = brokers.get(Math.abs((attachIndex + i) % brokers.size()));
                long attachedCount = Optional.ofNullable(attachedCounts.get(broker.id())).orElse(0L);
                if (attachedCount < MAX_VOLUME_ATTACH_COUNT) {
                    CompletableFuture<String> attachCf = attach(new FailedNode(failedNodeId, context.volumeId()), broker.id());
                    attachCfList.add(attachCf.thenAccept(device -> {
                        FailoverContextRecord attachedRecord = context.duplicate();
                        attachedRecord.setTargetNodeId(broker.id());
                        attachedRecord.setDevice(device);
                        attachedRecord.setStatus(FailoverStatus.RECOVERING.name());
                        attached.put(failedNodeId, attachedRecord);
                    }).exceptionally(ex -> {
                        LOGGER.error("attach failed node {} to target node {} failed", context.failedNodeId(), broker.id(), ex);
                        return null;
                    }));
                    attachedCounts.put(broker.id(), attachedCount + 1);
                }
            }
        }
        CompletableFuture.allOf(attachCfList.toArray(new CompletableFuture[0])).join();
    }

    public void replay(FailoverContextRecord record) {
        failoverContexts.put(record.failedNodeId(), record);
    }

    /**
     * Attach the failed node volume to the target node.
     *
     * @param failedNode   {@link FailedNode}
     * @param targetNodeId target node id
     * @return the device name of volume attached to the target node
     */
    CompletableFuture<String> attach(FailedNode failedNode, int targetNodeId) {
        // TODO: run command to attach
        return CompletableFuture.completedFuture("");
    }

    static class FailedNode {
        private int nodeId;
        private String volumeId;

        public FailedNode(int nodeId, String volumeId) {
            this.nodeId = nodeId;
            this.volumeId = volumeId;
        }

        public int getNodeId() {
            return nodeId;
        }

        public void setNodeId(int nodeId) {
            this.nodeId = nodeId;
        }

        public String getVolumeId() {
            return volumeId;
        }

        public void setVolumeId(String volumeId) {
            this.volumeId = volumeId;
        }
    }

}
