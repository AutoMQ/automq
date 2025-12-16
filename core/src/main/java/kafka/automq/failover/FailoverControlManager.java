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

import kafka.automq.utils.JsonUtils;

import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.stream.NodeControlManager;
import org.apache.kafka.controller.stream.StreamControlManager;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static kafka.automq.failover.FailoverConstants.FAILOVER_KEY;

@SuppressWarnings({"NPathComplexity"})
public class FailoverControlManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverControlManager.class);
    private static final int MAX_FAILOVER_COUNT_IN_TARGET_NODE = 1;
    /**
     * failover contexts: failedNode -> context
     */
    private final TimelineObject<Map<FailedNode, FailoverContext>> failoverContexts;
    private final QuorumController quorumController;
    private final ClusterControlManager clusterControlManager;
    private final NodeControlManager nodeControlManager;
    private final StreamControlManager streamControlManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("failover-controller", true));

    public FailoverControlManager(
        SnapshotRegistry registry,
        QuorumController quorumController,
        ClusterControlManager clusterControlManager,
        NodeControlManager nodeControlManager,
        StreamControlManager streamControlManager
    ) {
        this.failoverContexts = new TimelineObject<>(registry, Collections.emptyMap());
        this.quorumController = quorumController;
        this.clusterControlManager = clusterControlManager;
        this.nodeControlManager = nodeControlManager;
        this.streamControlManager = streamControlManager;
        this.scheduler.scheduleWithFixedDelay(this::runFailoverTask, 10, 10, TimeUnit.SECONDS);
    }

    void runFailoverTask() {
        if (!quorumController.isActive()) {
            return;
        }
        try {
            this.quorumController.appendWriteEvent("failover", OptionalLong.empty(), this::failover).get();
        } catch (Throwable e) {
            LOGGER.error("run failover task failed", e);
        }
    }

    private ControllerResult<Void> failover() {
        List<NodeRuntimeMetadata> allNodes = allNodes();
        List<FailedWal> failedWalList = getFailedWal(allNodes);

        Map<FailedNode, FailoverContext> failoverContexts = this.failoverContexts.get();

        List<FailoverContext> newFailover = addNewFailover(failoverContexts, failedWalList);
        List<FailedNode> completed = delCompleted(failoverContexts, failedWalList);
        List<FailedNode> reset = resetNotAlive(failoverContexts);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[FAILOVER_TASK], new={}, completed={}, reset={}", newFailover, completed, reset);
        }

        failoverContexts = new HashMap<>(failoverContexts);
        for (FailedNode node : completed) {
            FailoverContext ctx = failoverContexts.remove(node);
            LOGGER.info("[FAILOVER_COMPLETE],context={}", ctx);
        }
        for (FailedNode node : reset) {
            // remove reset nodes, and they will be added again in the next round
            FailoverContext ctx = failoverContexts.remove(node);
            LOGGER.info("[RESET_NOT_ALIVE_FAILOVER],context={}", ctx);
        }
        int maxInflight = 1;
        List<FailedNode> excess = failoverContexts.keySet().stream()
            .skip(maxInflight)
            .collect(Collectors.toList());
        for (FailedNode node : excess) {
            FailoverContext ctx = failoverContexts.remove(node);
            LOGGER.info("[REMOVE_EXCESS_FAILOVER],context={}", ctx);
        }
        for (FailoverContext ctx : newFailover) {
            if (failoverContexts.size() < maxInflight) {
                failoverContexts.put(ctx.getFailedNode(), ctx);
                LOGGER.info("[ADD_NEW_FAILOVER],context={}", ctx);
            } else {
                LOGGER.info("[PENDING_FAILOVER],context={}", ctx);
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[INFLIGHT_FAILOVER],contexts={}", failoverContexts);
        }
        if (newFailover.isEmpty() && completed.isEmpty() && reset.isEmpty() && excess.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("No changes in failover contexts");
            }
            return ControllerResult.of(Collections.emptyList(), null);
        }
        String encoded = JsonUtils.encode(failoverContexts.values());
        ApiMessageAndVersion record = new ApiMessageAndVersion(new KVRecord()
            .setKeyValues(Collections.singletonList(new KVRecord.KeyValue()
                .setKey(FAILOVER_KEY)
                .setValue(encoded.getBytes(StandardCharsets.UTF_8)))), (short) 0);
        return ControllerResult.of(Collections.singletonList(record), null);
    }

    public void replay(KVRecord record) {
        if (!record.keyValues().isEmpty()) {
            KVRecord.KeyValue kv = record.keyValues().get(0);
            if (!FAILOVER_KEY.equals(kv.key())) {
                return;
            }
            FailoverContext[] contexts = JsonUtils.decode(new String(kv.value(), StandardCharsets.UTF_8), FailoverContext[].class);
            Map<FailedNode, FailoverContext> failoverContexts = new HashMap<>(contexts.length);
            for (FailoverContext ctx : contexts) {
                failoverContexts.put(ctx.getFailedNode(), ctx);
            }
            this.failoverContexts.set(failoverContexts);
        }
    }

    private List<NodeRuntimeMetadata> allNodes() {
        // source of truth for node epoch
        Map<Integer, Long> nodeEpochMap = streamControlManager.nodesMetadata().values().stream()
            .collect(Collectors.toMap(
                org.apache.kafka.controller.stream.NodeRuntimeMetadata::getNodeId,
                org.apache.kafka.controller.stream.NodeRuntimeMetadata::getNodeEpoch
            ));

        return nodeControlManager.getMetadata().stream()
            // Generally, any node in nodeControlManager should have a corresponding node in streamControlManager.
            // However, there is a very short period of time when a node is in nodeControlManager but not in streamControlManager when the node first joins the cluster.
            // In this case, we just ignore the node.
            .filter(node -> nodeEpochMap.containsKey(node.getNodeId()))
            .map(node -> new NodeRuntimeMetadata(
                node.getNodeId(),
                // There are node epochs in both streamControlManager and nodeControlManager, and they are the same in most cases.
                // However, in some rare cases, the node epoch in streamControlManager may be updated earlier than the node epoch in nodeControlManager.
                // So we use the node epoch in streamControlManager as the source of truth.
                nodeEpochMap.get(node.getNodeId()),
                node.getWalConfig(),
                node.getTags(),
                nodeControlManager.state(node.getNodeId()),
                nodeControlManager.hasOpeningStreams(node.getNodeId())
            ))
            .collect(Collectors.toList());
    }

    private List<FailoverContext> addNewFailover(Map<FailedNode, FailoverContext> failoverContexts,
        List<FailedWal> failedWalList) {
        List<FailoverContext> newFailover = new LinkedList<>();
        List<Integer> brokerIds = null;
        Map<Integer /* nodeId */, Integer /* count */> failoverCounts = new HashMap<>();
        // round-robin assign new failover
        int assignIndex = 0;
        for (FailedWal failedWal : failedWalList) {
            if (failoverContexts.containsKey(failedWal.node())) {
                continue;
            }
            if (brokerIds == null) {
                // lazy init
                brokerIds = clusterControlManager.getActiveBrokers().stream().map(BrokerRegistration::id).collect(Collectors.toList());
                failoverContexts.forEach((n, ctx) -> failoverCounts.merge(ctx.getTarget(), 1, Integer::sum));
            }
            boolean found = false;
            for (int i = 0; i < brokerIds.size(); i++, assignIndex++) {
                int brokerId = brokerIds.get(assignIndex % brokerIds.size());

                if (brokerId == failedWal.nodeId()) {
                    // skip the failed node itself
                    continue;
                }

                int attachedCount = Optional.ofNullable(failoverCounts.get(brokerId)).orElse(0);
                if (attachedCount >= MAX_FAILOVER_COUNT_IN_TARGET_NODE) {
                    continue;
                }
                failoverCounts.merge(brokerId, 1, Integer::sum);
                newFailover.add(failedWal.toFailoverContext(brokerId));
                found = true;
                break;
            }
            if (!found) {
                LOGGER.warn("No broker available for failover, failedWal={}", failedWal);
            }
        }
        return newFailover;
    }

    private List<FailedNode> delCompleted(Map<FailedNode, FailoverContext> failoverContexts,
        List<FailedWal> failedWalList) {
        Set<FailedNode> failedNodeSet = failedWalList.stream()
            .map(FailedWal::node)
            .collect(Collectors.toSet());
        return failoverContexts.keySet().stream()
            .filter(node -> !failedNodeSet.contains(node))
            .collect(Collectors.toList());
    }

    /**
     * Reset the failover context whose target node is not alive
     */
    private List<FailedNode> resetNotAlive(Map<FailedNode, FailoverContext> failoverContexts) {
        List<FailedNode> reset = new LinkedList<>();
        for (FailoverContext ctx : failoverContexts.values()) {
            if (!clusterControlManager.isActive(ctx.getTarget())) {
                reset.add(ctx.getFailedNode());
            }
        }
        return reset;
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdown();
    }

    private static List<FailedWal> getFailedWal(List<NodeRuntimeMetadata> allNodes) {
        List<FailedWal> result = allNodes.stream()
            .filter(NodeRuntimeMetadata::shouldFailover)
            .map(DefaultFailedWal::from)
            .collect(Collectors.toCollection(ArrayList::new));
        return result;
    }
}
