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

import com.automq.stream.s3.failover.DefaultServerless;
import com.automq.stream.s3.failover.Serverless;
import com.automq.stream.s3.failover.Serverless.FailedNode;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FailoverControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverControlManager.class);
    private static final int MAX_VOLUME_ATTACH_COUNT = 1;
    /**
     * failover contexts: failedNodeId -> context
     */
    private final TimelineHashMap<Integer, FailoverContextRecord> failoverContexts;
    private List<FailedNode> failedNodes;
    /**
     * attached: failedNodeId -> context with targetNodeId
     */
    private final Map<Integer, FailoverContextRecord> attached = new ConcurrentHashMap<>();

    private final Serverless serverless;
    private final QuorumController quorumController;
    private final ClusterControlManager clusterControlManager;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("failover-controller", true));
    private final ExecutorService attachExecutor = Executors.newFixedThreadPool(4, ThreadUtils.createThreadFactory("failover-attach-%d", true));

    public FailoverControlManager(
            QuorumController quorumController,
            ClusterControlManager clusterControlManager,
            SnapshotRegistry registry, boolean failoverEnable) {
        this.quorumController = quorumController;
        this.clusterControlManager = clusterControlManager;
        this.serverless = new DefaultServerless();
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

    void scanFailedNodes() throws ExecutionException {
        failedNodes = serverless.scan();
    }


    public ControllerResult<Void> failover() {
        // code in run should be non-blocking
        List<ApiMessageAndVersion> records = new ArrayList<>();
        List<FailedNode> failedNodes = this.failedNodes;
        addNewContext(failedNodes, records);
        doFailover(records);
        complete(failedNodes, records);
        resetFailover(records);
        return ControllerResult.of(records, null);
    }

    /**
     * Reset the failover context whose target node is not alive
     */
    private void resetFailover(List<ApiMessageAndVersion> records) {
        for (FailoverContextRecord record : failoverContexts.values()) {
            if (FailoverStatus.RECOVERING.name().equals(record.status()) && !clusterControlManager.isActive(record.targetNodeId())) {
                records.add(new ApiMessageAndVersion(
                        new FailoverContextRecord()
                                .setFailedNodeId(record.failedNodeId())
                                .setVolumeId(record.volumeId())
                                .setStatus(FailoverStatus.WAITING.name()),
                        (short) 0));
            }
        }
    }

    private void addNewContext(List<FailedNode> failedNodes, List<ApiMessageAndVersion> records) {
        for (FailedNode failedNode : failedNodes) {
            if (failoverContexts.containsKey(failedNode.getNodeId())) {
                // the failed node is already in failover mode, skip
                continue;
            }
            LOGGER.info("scan new failed node {}", failedNode);
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
                    CompletableFuture<String> attachCf = attach(context.volumeId(), broker.id());
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
     * @param volumeId     volume id
     * @param targetNodeId target node id
     * @return the device name of volume attached to the target node
     */
    CompletableFuture<String> attach(String volumeId, int targetNodeId) {
        CompletableFuture<String> cf = new CompletableFuture<>();
        attachExecutor.submit(() -> {
            try {
                cf.complete(serverless.attach(volumeId, targetNodeId));
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

}
