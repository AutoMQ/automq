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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.automq.stream.utils.threads.EventLoopSafe;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.automq.stream.s3.wal.impl.object.ObjectUtils.floorAlignOffset;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.genObjectPathV1;

@EventLoopSafe
public class Reader {
    private static final EventLoop[] EVENT_LOOPS = new EventLoop[4];
    private static final ByteBufSeqAlloc ALLOC = new ByteBufSeqAlloc(ByteBufAlloc.DECODE_RECORD, 4);

    static {
        for (int i = 0; i < EVENT_LOOPS.length; i++) {
            EVENT_LOOPS[i] = new EventLoop("OBJECT_WAL_READER_" + i);
        }
    }

    private final ObjectStorage objectStorage;
    private final String nodePrefix;
    private final Time time;

    private final Queue<ReadTask> tasks = new ConcurrentLinkedQueue<>();

    // the rebuild task that is in process
    private CompletableFuture<Void> rebuildIndexCf = CompletableFuture.completedFuture(null);
    // the rebuild task that is waiting for sequential running.
    private CompletableFuture<Void> awaitRebuildIndexCf;
    private NavigableMap<Long /* epoch's startOffset */, Long /* epoch */> indexMap = new TreeMap<>();

    private final EventLoop eventLoop;

    public Reader(ObjectStorage objectStorage, String clusterId, int nodeId, String type, Time time) {
        this.objectStorage = objectStorage;
        this.nodePrefix = ObjectUtils.nodePrefix(clusterId, nodeId, type);
        this.time = time;
        this.eventLoop = EVENT_LOOPS[Math.abs(nodeId % EVENT_LOOPS.length)];
    }

    public CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset) {
        DefaultRecordOffset offset = (DefaultRecordOffset) recordOffset;
        ReadTask readTask = new ReadTask(offset.offset(), offset.size());
        tasks.add(readTask);
        eventLoop.submit(this::doRun);
        return readTask.cf;
    }

    private void doRun() {
        for (; ; ) {
            ReadTask readTask = tasks.peek();
            if (readTask == null) {
                break;
            }
            Map.Entry<Long, Long> entry = indexMap.floorEntry(readTask.offset);
            if (entry == null) {
                readTask.notFoundTimes++;
                rebuildIndexMap().thenAcceptAsync(nil -> doRun(), eventLoop);
                return;
            }
            tasks.poll();
            long epoch = entry.getValue();
            long objectStartObject = floorAlignOffset(readTask.offset);
            String objectPath = genObjectPathV1(nodePrefix, epoch, objectStartObject);
            long relativeStartOffset = readTask.offset - objectStartObject + WALObjectHeader.WAL_HEADER_SIZE_V1;
            objectStorage.rangeRead(
                new ObjectStorage.ReadOptions().bucket(objectStorage.bucketId()).throttleStrategy(ThrottleStrategy.BYPASS),
                objectPath,
                relativeStartOffset,
                relativeStartOffset + readTask.size
            ).whenCompleteAsync((buf, ex) -> {
                try {
                    if (ex == null) {
                        readTask.cf.complete(ObjectUtils.duplicatedDecodeRecordBuf(buf, ALLOC));
                        return;
                    }
                    ex = FutureUtil.cause(ex);
                    if (!(ex instanceof ObjectNotExistException)) {
                        readTask.cf.completeExceptionally(ex);
                        return;
                    }
                    if (readTask.notFoundTimes > 0) {
                        readTask.cf.completeExceptionally(ex);
                        return;
                    }
                    CompletableFuture<Void> rebuildCf = rebuildIndexMap();
                    readTask.notFoundTimes++;
                    tasks.add(readTask);
                    rebuildCf.thenAcceptAsync(nil -> doRun(), eventLoop);
                } catch (Throwable e) {
                    readTask.cf.completeExceptionally(e);
                }
            }, eventLoop);
        }
    }

    private CompletableFuture<Void> rebuildIndexMap() {
        if (rebuildIndexCf.isDone()) {
            rebuildIndexCf = rebuildIndexMap0();
            return rebuildIndexCf;
        } else {
            if (awaitRebuildIndexCf == null) {
                awaitRebuildIndexCf = new CompletableFuture<>();
                CompletableFuture<Void> retCf = awaitRebuildIndexCf;
                rebuildIndexCf.whenCompleteAsync((nil, ex) -> {
                    awaitRebuildIndexCf = null;
                    rebuildIndexCf = rebuildIndexMap0();
                    rebuildIndexCf.whenCompleteAsync((nil2, ex2) -> {
                        if (ex2 != null) {
                            retCf.completeExceptionally(ex2);
                        } else {
                            retCf.complete(null);
                        }
                    });
                }, eventLoop);
            }
            return awaitRebuildIndexCf;
        }
    }

    private CompletableFuture<Void> rebuildIndexMap0() {
        return objectStorage.list(nodePrefix).thenAcceptAsync(list -> {
            List<WALObject> objects = ObjectUtils.parse(list);
            TreeMap<Long, Long> newIndexMap = new TreeMap<>();
            long lastEpoch = Long.MIN_VALUE;
            for (WALObject object : objects) {
                if (object.epoch() == lastEpoch) {
                    continue;
                }
                newIndexMap.put(object.startOffset(), object.epoch());
                lastEpoch = object.epoch();
            }
            this.indexMap = newIndexMap;
        }, eventLoop);
    }

    static class ReadTask {
        final long offset;
        final int size;
        final CompletableFuture<StreamRecordBatch> cf;
        int notFoundTimes = 0;

        public ReadTask(long offset, int size) {
            this.offset = offset;
            this.size = size;
            this.cf = new CompletableFuture<>();
        }
    }

}
