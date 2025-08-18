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

import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

public class RouterChannelEpochControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterChannelEpochControlManager.class);
    private static final long BUMP_EPOCH_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    private final TimelineObject<RouterChannelEpoch> routerChannelEpoch;
    private final TimelineHashMap<Integer, Long> node2commitedEpoch;
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("ROUTER_CHANNEL_EPOCH_MANAGER", true, LOGGER);

    private final QuorumController quorumController;
    private final NodeControlManager nodeControlManager;
    private final Time time;

    public RouterChannelEpochControlManager(SnapshotRegistry registry, QuorumController quorumController, NodeControlManager nodeControlManager, Time time) {
        this.routerChannelEpoch = new TimelineObject<>(registry, new RouterChannelEpoch(-3L, -2L, 0, 0));
        this.node2commitedEpoch = new TimelineHashMap<>(registry, 100);
        this.quorumController = quorumController;
        this.nodeControlManager = nodeControlManager;
        this.time = time;
        scheduler.scheduleWithFixedDelay(this::run, 1, 1, TimeUnit.SECONDS);
    }

    private void run() {
        if (!quorumController.isActive()) {
            return;
        }
        quorumController.appendWriteEvent("routerChannelEpochRun", OptionalLong.empty(), this::run0);
    }

    private ControllerResult<Void> run0() {
        List<ApiMessageAndVersion> records = new LinkedList<>();

        RouterChannelEpoch epoch = routerChannelEpoch.get();
        if (epoch == null) {
            // initial the epoch if not exist
            epoch = new RouterChannelEpoch(-3L, -2L, 0, time.milliseconds());
            records.add(new ApiMessageAndVersion(
                kv(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY, RouterChannelEpoch.encode(epoch, (short) 0).array()), (short) 0));
        } else {
            // update the commitedEpoch, then RouterChannel can clean up commited data.
            OptionalLong newCommitedEpoch = calCommitedEpoch();
            RouterChannelEpoch newEpoch = null;
            if (time.milliseconds() - epoch.getLastBumpUpTimestamp() >= BUMP_EPOCH_INTERVAL) {
                // bump to the next epoch.
                newEpoch = new RouterChannelEpoch(newCommitedEpoch.orElse(epoch.getCommitted()), epoch.getFenced() + 1, epoch.getCurrent() + 1, time.milliseconds());
            } else if (newCommitedEpoch.isPresent() && newCommitedEpoch.getAsLong() > epoch.getCommitted()) {
                newEpoch = new RouterChannelEpoch(newCommitedEpoch.getAsLong(), epoch.getFenced(), epoch.getCurrent(), epoch.getLastBumpUpTimestamp());
            }
            if (newEpoch != null) {
                records.add(new ApiMessageAndVersion(
                    kv(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY, RouterChannelEpoch.encode(newEpoch, (short) 0).array()), (short) 0));
            }
        }

        return ControllerResult.of(records, null);
    }

    /**
     * Calculate the newCommitedEpoch = min(nodeCommitedEpoch)
     */
    private OptionalLong calCommitedEpoch() {
        long newCommitedEpoch = Long.MAX_VALUE;
        for (NodeMetadata nodeMetadata : nodeControlManager.getMetadata()) {
            int nodeId = nodeMetadata.getNodeId();
            if (nodeControlManager.state(nodeId) != NodeState.ACTIVE && nodeControlManager.hasOpeningStreams(nodeId)) {
                // there is a node need to failover.
                return OptionalLong.empty();
            }
            Long nodeCommitedEpoch = node2commitedEpoch.get(nodeId);
            if (nodeCommitedEpoch == null) {
                // The node
                return OptionalLong.empty();
            }
            newCommitedEpoch = Math.min(newCommitedEpoch, nodeCommitedEpoch);
        }
        if (newCommitedEpoch == Long.MAX_VALUE) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(newCommitedEpoch);
    }

    public void replay(KVRecord kvRecord) {
        for (KVRecord.KeyValue kv : kvRecord.keyValues()) {
            String key = kv.key();
            if (key == null) {
                continue;
            }
            try {
                if (key.startsWith(NodeCommittedEpoch.NODE_COMMITED_EPOCH_KEY_PREFIX)) {
                    int nodeId = Integer.parseInt(key.substring(NodeCommittedEpoch.NODE_COMMITED_EPOCH_KEY_PREFIX.length()));
                    NodeCommittedEpoch epoch = NodeCommittedEpoch.decode(Unpooled.wrappedBuffer(kv.value()));
                    node2commitedEpoch.put(nodeId, epoch.getEpoch());
                } else if (key.startsWith(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY)) {
                    RouterChannelEpoch epoch = RouterChannelEpoch.decode(Unpooled.wrappedBuffer(kv.value()));
                    routerChannelEpoch.set(epoch);
                }
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay router channel epoch {} fail", kv, e);
            }
        }
    }

    public void replay(RemoveKVRecord kvRecord) {
        for (String key : kvRecord.keys()) {
            try {
                if (key.startsWith(NodeCommittedEpoch.NODE_COMMITED_EPOCH_KEY_PREFIX)) {
                    int nodeId = Integer.parseInt(key.substring(NodeCommittedEpoch.NODE_COMMITED_EPOCH_KEY_PREFIX.length()));
                    node2commitedEpoch.remove(nodeId);
                }
            } catch (Throwable e) {
                LOGGER.error("[FATAL] replay router channel epoch {} fail", key, e);
            }
        }
    }

    static KVRecord kv(String key, byte[] value) {
        return new KVRecord().setKeyValues(List.of(new KVRecord.KeyValue().setKey(key).setValue(value)));
    }
}
