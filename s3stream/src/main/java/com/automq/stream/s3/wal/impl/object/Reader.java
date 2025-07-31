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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.automq.stream.s3.wal.impl.object.ObjectUtils.DATA_FILE_ALIGN_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.floorAlignOffset;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.genObjectPathV1;

@EventLoopSafe
public class Reader {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);
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

    private final Queue<SingleReadTask> singleReadTasks = new ConcurrentLinkedQueue<>();
    private final Queue<BatchReadTask> batchReadTasks = new ConcurrentLinkedQueue<>();

    // the rebuild task that is in process
    private CompletableFuture<Void> rebuildIndexCf = CompletableFuture.completedFuture(null);
    // the rebuild task that is waiting for sequential running.
    private CompletableFuture<Void> awaitRebuildIndexCf;
    private NavigableMap<Long /* epoch's startOffset */, Long /* epoch */> indexMap = new ConcurrentSkipListMap<>();

    private final EventLoop eventLoop;

    public Reader(ObjectStorage objectStorage, String clusterId, int nodeId, String type, Time time) {
        this.objectStorage = objectStorage;
        this.nodePrefix = ObjectUtils.nodePrefix(clusterId, nodeId, type);
        this.time = time;
        this.eventLoop = EVENT_LOOPS[Math.abs(nodeId % EVENT_LOOPS.length)];
    }

    public CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset) {
        DefaultRecordOffset offset = recordOffset instanceof DefaultRecordOffset ? (DefaultRecordOffset) recordOffset : DefaultRecordOffset.of(recordOffset.buffer());
        SingleReadTask readTask = new SingleReadTask(offset.offset(), offset.size());
        singleReadTasks.add(readTask);
        eventLoop.submit(this::doRunSingleGet);
        return readTask.cf;
    }

    public CompletableFuture<List<StreamRecordBatch>> get(long startOffset, long endOffset) {
        BatchReadTask readTask = new BatchReadTask(startOffset, endOffset);
        batchReadTasks.add(readTask);
        eventLoop.submit(this::doRunBatchGet);
        return readTask.cf;
    }

    private void doRunSingleGet() {
        for (; ; ) {
            SingleReadTask readTask = singleReadTasks.peek();
            if (readTask == null) {
                break;
            }
            Map.Entry<Long, Long> entry = indexMap.floorEntry(readTask.offset);
            if (entry == null) {
                if (readTask.notFoundTimes > 0) {
                    singleReadTasks.poll();
                    readTask.cf.completeExceptionally(new ObjectNotExistException(
                        String.format("Cannot find object for (offset=%s, size=%s)", readTask.offset, readTask.size)
                    ));
                    return;
                }
                readTask.notFoundTimes++;
                rebuildIndexMap().thenAcceptAsync(nil -> doRunSingleGet(), eventLoop);
                return;
            }
            singleReadTasks.poll();
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
                        readTask.cf.complete(ObjectUtils.duplicatedDecodeRecordBuf(buf.slice(), ALLOC));
                        buf.release();
                        return;
                    }
                    ex = FutureUtil.cause(ex);
                    if (!(ex instanceof ObjectNotExistException) || readTask.notFoundTimes > 0) {
                        readTask.cf.completeExceptionally(ex);
                        return;
                    }
                    CompletableFuture<Void> rebuildCf = rebuildIndexMap();
                    readTask.notFoundTimes++;
                    rebuildCf.thenAcceptAsync(nil -> {
                        singleReadTasks.add(readTask);
                        doRunSingleGet();
                    }, eventLoop);
                } catch (Throwable e) {
                    readTask.cf.completeExceptionally(e);
                }
            }, eventLoop);
        }
    }

    @SuppressWarnings("NPathComplexity")
    private void doRunBatchGet() {
        for (; ; ) {
            BatchReadTask readTask = batchReadTasks.peek();
            if (readTask == null) {
                break;
            }
            NavigableMap<Long, Long> indexMap;
            Long floorKey = this.indexMap.floorKey(readTask.startOffset);
            if (floorKey == null) {
                indexMap = new TreeMap<>();
            } else {
                indexMap = this.indexMap.subMap(floorKey, true, readTask.endOffset, false);
            }
            if (indexMap.isEmpty()) {
                if (readTask.notFoundTimes > 0) {
                    batchReadTasks.poll();
                    readTask.cf.completeExceptionally(new ObjectNotExistException(
                        String.format("Cannot find object for [%s, %s)", readTask.startOffset, readTask.endOffset)
                    ));
                    return;
                }
                readTask.notFoundTimes++;
                rebuildIndexMap().thenAcceptAsync(nil -> doRunBatchGet(), eventLoop);
                return;
            }
            batchReadTasks.poll();
            List<CompletableFuture<List<StreamRecordBatch>>> getCfList = new ArrayList<>();
            long nextGetOffset = readTask.startOffset;
            List<Map.Entry<Long, Long>> entries = new ArrayList<>(indexMap.entrySet());
            for (int i = 0; i < entries.size(); i++) {
                long epoch = entries.get(i).getValue();
                long epochEndOffset = (i == entries.size() - 1) ? Long.MAX_VALUE : entries.get(i + 1).getKey();
                while (nextGetOffset < epochEndOffset && nextGetOffset < readTask.endOffset) {
                    long objectStartOffset = floorAlignOffset(nextGetOffset);
                    String objectPath = genObjectPathV1(nodePrefix, epoch, objectStartOffset);
                    long relativeStartOffset = nextGetOffset - objectStartOffset + WALObjectHeader.WAL_HEADER_SIZE_V1;
                    // read to end
                    long finalNextGetOffset = nextGetOffset;
                    getCfList.add(objectStorage.rangeRead(
                        new ObjectStorage.ReadOptions().bucket(objectStorage.bucketId()).throttleStrategy(ThrottleStrategy.BYPASS),
                        objectPath,
                        relativeStartOffset,
                        -1
                    ).thenApply(buf -> {
                        try {
                            List<StreamRecordBatch> batches = new ArrayList<>();
                            buf = buf.slice();
                            long nextRecordOffset = finalNextGetOffset;
                            int lastReadableBytes = buf.readableBytes();
                            while (buf.readableBytes() > 0 && nextRecordOffset < readTask.endOffset) {
                                batches.add(ObjectUtils.duplicatedDecodeRecordBuf(buf, ALLOC));
                                nextRecordOffset += lastReadableBytes - buf.readableBytes();
                                lastReadableBytes = buf.readableBytes();
                            }
                            return batches;
                        } finally {
                            buf.release();
                        }
                    }));
                    nextGetOffset = objectStartOffset + DATA_FILE_ALIGN_SIZE;
                }
            }
            CompletableFuture.allOf(getCfList.toArray(new CompletableFuture[0])).whenCompleteAsync((nil, ex) -> {
                if (ex == null) {
                    List<StreamRecordBatch> batches = new ArrayList<>();
                    for (CompletableFuture<List<StreamRecordBatch>> cf : getCfList) {
                        batches.addAll(cf.join());
                    }
                    readTask.cf.complete(batches);
                    return;
                }
                ex = FutureUtil.cause(ex);
                if (!(ex instanceof ObjectNotExistException) || readTask.notFoundTimes > 0) {
                    getCfList.forEach(cf -> cf.thenAccept(l -> l.forEach(StreamRecordBatch::release)));
                    readTask.cf.completeExceptionally(ex);
                    return;
                }
                CompletableFuture<Void> rebuildCf = rebuildIndexMap();
                readTask.notFoundTimes++;
                rebuildCf.thenAcceptAsync(nil2 -> {
                    batchReadTasks.add(readTask);
                    doRunBatchGet();
                }, eventLoop);
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

    static class SingleReadTask {
        final long offset;
        final int size;
        final CompletableFuture<StreamRecordBatch> cf;
        int notFoundTimes = 0;

        public SingleReadTask(long offset, int size) {
            this.offset = offset;
            this.size = size;
            this.cf = new CompletableFuture<>();
        }
    }

    static class BatchReadTask {
        final long startOffset;
        final long endOffset;
        final CompletableFuture<List<StreamRecordBatch>> cf;
        int notFoundTimes = 0;

        public BatchReadTask(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.cf = new CompletableFuture<>();
        }
    }

}
