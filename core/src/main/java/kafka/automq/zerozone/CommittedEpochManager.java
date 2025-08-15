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

package kafka.automq.zerozone;

import org.apache.kafka.controller.stream.NodeCommittedEpoch;
import org.apache.kafka.controller.stream.RouterChannelEpoch;

import com.automq.stream.Context;
import com.automq.stream.api.KeyValue;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CommittedEpochManager implements RouterChannelProvider.EpochListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommittedEpochManager.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private RouterChannelEpoch routerChannelEpoch;
    private long waitingCommitEpoch = -1L;
    private long committedEpoch = -1L;
    private CompletableFuture<Void> commitCf = CompletableFuture.completedFuture(null);
    private final NavigableMap<Long, AtomicLong> epoch2inflight = new ConcurrentSkipListMap<>();

    private final int nodeId;

    public CommittedEpochManager(int nodeId) {
        this.nodeId = nodeId;
    }

    public ReentrantReadWriteLock.ReadLock readLock() {
        return readLock;
    }

    public AtomicLong epochInflight(long epoch) {
        return epoch2inflight.computeIfAbsent(epoch, e -> new AtomicLong());
    }

    private void start() {
        Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(this::tryBumpCommittedEpoch, 1, 1, TimeUnit.SECONDS);
    }

    private void tryBumpCommittedEpoch() {
        writeLock.lock();
        try {
            tryBumpCommittedEpoch0();
        } catch (Exception e) {
            LOGGER.error("Error while trying bumping committed epoch", e);
        } finally {
            writeLock.unlock();
        }
    }

    private void tryBumpCommittedEpoch0() {
        long fencedEpoch = routerChannelEpoch.getFenced();
        Iterator<Map.Entry<Long, AtomicLong>> it = epoch2inflight.entrySet().iterator();
        long newWaitingEpoch = waitingCommitEpoch;
        while (it.hasNext()) {
            Map.Entry<Long, AtomicLong> entry = it.next();
            long epoch = entry.getKey();
            if (epoch > fencedEpoch) {
                break;
            }
            AtomicLong inflight = entry.getValue();
            if (inflight.get() == 0) {
                it.remove();
                newWaitingEpoch = epoch;
            }
        }
        if (epoch2inflight.isEmpty()) {
            newWaitingEpoch = fencedEpoch;
        }
        if (newWaitingEpoch != waitingCommitEpoch) {
            this.waitingCommitEpoch = newWaitingEpoch;
        }
        if (commitCf.isDone() && waitingCommitEpoch != committedEpoch) {
            long newCommittedEpoch = waitingCommitEpoch;
            commitCf = commitCf
                .thenCompose(nil -> Context.instance().confirmWAL().commit(TimeUnit.SECONDS.toMillis(10)))
                .thenCompose(nil ->
                    Context.instance().kvClient().putKV(KeyValue.of(
                        NodeCommittedEpoch.NODE_COMMITED_EPOCH_KEY_PREFIX + nodeId,
                        NodeCommittedEpoch.encode(new NodeCommittedEpoch(newCommittedEpoch), (short) 0).nioBuffer()
                    )).thenAccept(rst -> committedEpoch = newCommittedEpoch)
                ).exceptionally(ex -> {
                    LOGGER.error("[BUMP_COMMITTED_EPOCH_FAIL]", ex);
                    return null;
                });
        }
    }

    @Override
    public void onNewEpoch(RouterChannelEpoch epoch) {
        writeLock.lock();
        try {
            boolean first = routerChannelEpoch == null;
            routerChannelEpoch = epoch;
            if (first) {
                start();
            }
        } finally {
            writeLock.unlock();
        }
    }
}
