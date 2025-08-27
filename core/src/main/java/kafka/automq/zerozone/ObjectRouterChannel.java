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

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.buffer.ByteBuf;

public class ObjectRouterChannel implements RouterChannel {
    private final Logger logger;
    private final AtomicLong mockOffset = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ObjectWALService wal;
    private final int nodeId;
    private final short channelId;

    private long channelEpoch = 0L;
    private final Queue<Long> channelEpochQueue = new LinkedList<>();
    private final Map<Long, RecordOffset> channelEpoch2LastRecordOffset = new HashMap<>();

    public ObjectRouterChannel(int nodeId, short channelId, ObjectWALService wal) {
        this.logger = new LogContext(String.format("[OBJECT_ROUTER_CHANNEL-%s-%s] ", channelId, nodeId)).logger(ObjectRouterChannel.class);
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.wal = wal;
    }

    @Override
    public CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data) {
        StreamRecordBatch record = new StreamRecordBatch(targetNodeId, 0, mockOffset.incrementAndGet(), 1, data);
        try {
            return wal.append(TraceContext.DEFAULT, record).thenApply(walRst -> {
                readLock.lock();
                try {
                    long epoch = this.channelEpoch;
                    ChannelOffset channelOffset = ChannelOffset.of(channelId, orderHint, nodeId, targetNodeId, walRst.recordOffset().buffer());
                    channelEpoch2LastRecordOffset.put(epoch, walRst.recordOffset());
                    return new AppendResult(epoch, channelOffset.byteBuf());
                } finally {
                    readLock.unlock();
                }
            });
        } catch (OverCapacityException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> get(ByteBuf channelOffset) {
        return wal.get(DefaultRecordOffset.of(ChannelOffset.of(channelOffset).walRecordOffset())).thenApply(streamRecordBatch -> {
            ByteBuf payload = streamRecordBatch.getPayload().retainedSlice();
            streamRecordBatch.release();
            return payload;
        });
    }

    @Override
    public void nextEpoch(long epoch) {
        writeLock.lock();
        try {
            if (epoch > this.channelEpoch) {
                this.channelEpochQueue.add(epoch);
                this.channelEpoch = epoch;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void trim(long epoch) {
        writeLock.lock();
        try {
            for (;;) {
                Long channelEpoch = channelEpochQueue.peek();
                if (channelEpoch == null || channelEpoch > epoch) {
                    return;
                }
                channelEpochQueue.poll();
                RecordOffset recordOffset = channelEpoch2LastRecordOffset.remove(epoch);
                if (recordOffset != null) {
                    wal.trim(recordOffset);
                    logger.info("trim to epoch={} offset={}", epoch, recordOffset);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        wal.shutdownGracefully();
    }
}
