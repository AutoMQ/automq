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

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.automq.stream.utils.threads.EventLoopSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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

@SuppressWarnings("checkstyle:cyclomaticComplexity")
@EventLoopSafe
public class DefaultReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReader.class);
    private static final EventLoop[] EVENT_LOOPS = new EventLoop[4];

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
    private long indexLargestEpoch = -1L;

    private final EventLoop eventLoop;

    public DefaultReader(ObjectStorage objectStorage, String clusterId, int nodeId, String type, Time time) {
        this.objectStorage = objectStorage;
        this.nodePrefix = ObjectUtils.nodePrefix(clusterId, nodeId, type);
        this.time = time;
        this.eventLoop = EVENT_LOOPS[Math.abs(nodeId % EVENT_LOOPS.length)];
    }

    public CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset) {
        DefaultRecordOffset offset = recordOffset instanceof DefaultRecordOffset ? (DefaultRecordOffset) recordOffset : DefaultRecordOffset.of(recordOffset.buffer());
        SingleReadTask readTask = new SingleReadTask(offset.epoch(), offset.offset(), offset.size());
        singleReadTasks.add(readTask);
        eventLoop.execute(this::doRunSingleGet);
        return readTask.cf;
    }

    public CompletableFuture<List<StreamRecordBatch>> get(RecordOffset startOffset, RecordOffset endOffset) {
        BatchReadTask readTask = new BatchReadTask(startOffset, endOffset);
        if (readTask.startOffset.offset() == readTask.endOffset.offset()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        batchReadTasks.add(readTask);
        eventLoop.execute(this::doRunBatchGet);
        return readTask.cf;
    }

    private void doRunSingleGet() {
        for (; ; ) {
            SingleReadTask readTask = singleReadTasks.poll();
            if (readTask == null) {
                break;
            }
            long objectStartObject = floorAlignOffset(readTask.offset);
            String objectPath = genObjectPathV1(nodePrefix, readTask.epoch, objectStartObject);
            long relativeStartOffset = readTask.offset - objectStartObject + WALObjectHeader.WAL_HEADER_SIZE_V1;
            objectStorage.rangeRead(
                new ObjectStorage.ReadOptions().bucket(objectStorage.bucketId()).throttleStrategy(ThrottleStrategy.BYPASS),
                objectPath,
                relativeStartOffset,
                relativeStartOffset + readTask.size
            ).whenCompleteAsync((buf, ex) -> {
                try {
                    if (ex != null) {
                        readTask.cf.completeExceptionally(ex);
                    } else {
                        readTask.cf.complete(ObjectUtils.decodeRecordBuf(buf.slice()));
                        buf.release();
                    }
                } catch (Throwable e) {
                    readTask.cf.completeExceptionally(e);
                }
            }, eventLoop);
        }
    }

    private void doRunBatchGet() {
        for (; ; ) {
            BatchReadTask readTask = batchReadTasks.poll();
            if (readTask == null) {
                break;
            }
            try {
                doRunBatchGet0(readTask);
            } catch (Throwable e) {
                LOGGER.error("[UNEXPECTED] Failed to run {}", readTask, e);
                readTask.cf.completeExceptionally(e);
            }
        }
    }

    @SuppressWarnings("NPathComplexity")
    private void doRunBatchGet0(BatchReadTask readTask) {
        CompletableFuture<Void> indexCf;
        if (indexLargestEpoch < readTask.endOffset.epoch()) {
            indexCf = rebuildIndexMap();
        } else {
            indexCf = CompletableFuture.completedFuture(null);
        }
        indexCf
            .thenComposeAsync(nil -> {
                NavigableMap<Long, Long> indexMap;
                Long floorKey = this.indexMap.floorKey(readTask.startOffset.offset());
                if (floorKey == null) {
                    indexMap = new TreeMap<>();
                } else {
                    indexMap = this.indexMap.subMap(floorKey, true, readTask.endOffset.offset(), false);
                }
                if (indexMap.isEmpty()) {
                    readTask.cf.completeExceptionally(new ObjectNotExistException(
                        String.format("Cannot find epoch for [%s, %s)", readTask.startOffset, readTask.endOffset)
                    ));
                }

                List<CompletableFuture<List<StreamRecordBatch>>> getCfList = new ArrayList<>();
                long nextGetOffset = readTask.startOffset.offset();
                List<Map.Entry<Long, Long>> entries = new ArrayList<>(indexMap.entrySet());
                for (int i = 0; i < entries.size(); i++) {
                    long epoch = entries.get(i).getValue();
                    long epochEndOffset = (i == entries.size() - 1) ? Long.MAX_VALUE : entries.get(i + 1).getKey();
                    while (nextGetOffset < epochEndOffset && nextGetOffset < readTask.endOffset.offset()) {
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
                                while (buf.readableBytes() > 0 && nextRecordOffset < readTask.endOffset.offset()) {
                                    StreamRecordBatch batch = ObjectUtils.decodeRecordBuf(buf);
                                    boolean isTriggerTrimRecord = batch.getCount() == 0 && batch.getStreamId() == -1L && batch.getEpoch() == -1L;
                                    if (!isTriggerTrimRecord) {
                                        batches.add(batch);
                                    } else {
                                        batch.release();
                                    }
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
                return CompletableFuture.allOf(getCfList.toArray(new CompletableFuture[0])).whenCompleteAsync((nil2, ex) -> {
                    if (ex != null) {
                        getCfList.forEach(cf -> cf.thenAccept(l -> l.forEach(StreamRecordBatch::release)));
                        readTask.cf.completeExceptionally(ex);
                        return;
                    }
                    List<StreamRecordBatch> batches = new ArrayList<>();
                    for (CompletableFuture<List<StreamRecordBatch>> cf : getCfList) {
                        batches.addAll(cf.join());
                    }
                    readTask.cf.complete(batches);
                }, eventLoop);
            }, eventLoop)
            .whenComplete((nil, ex) -> {
                if (ex != null && !readTask.cf.isDone()) {
                    LOGGER.error("[UNEXPECTED] Failed to run {}", readTask, ex);
                    readTask.cf.completeExceptionally(ex);
                }
            });
    }

    private CompletableFuture<Void> rebuildIndexMap() {
        if (rebuildIndexCf.isDone()) {
            rebuildIndexCf = rebuildIndexMap0();
            return rebuildIndexCf;
        } else {
            if (awaitRebuildIndexCf != null) {
                return awaitRebuildIndexCf;
            }
            awaitRebuildIndexCf = new CompletableFuture<>();
            CompletableFuture<Void> retCf = awaitRebuildIndexCf;
            rebuildIndexCf.whenCompleteAsync((nil, ex) -> {
                awaitRebuildIndexCf = null;
                rebuildIndexCf = rebuildIndexMap0();
                rebuildIndexCf.whenComplete((nil2, ex2) -> {
                    if (ex2 != null) {
                        retCf.completeExceptionally(ex2);
                    } else {
                        retCf.complete(null);
                    }
                });
            }, eventLoop);
            return retCf;
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
                indexLargestEpoch = lastEpoch;
            }
            this.indexMap = newIndexMap;
        }, eventLoop);
    }

    static class SingleReadTask {
        final long epoch;
        final long offset;
        final int size;
        final CompletableFuture<StreamRecordBatch> cf;

        public SingleReadTask(long epoch, long offset, int size) {
            this.epoch = epoch;
            this.offset = offset;
            this.size = size;
            this.cf = new CompletableFuture<>();
        }
    }

    static class BatchReadTask {
        final DefaultRecordOffset startOffset;
        final DefaultRecordOffset endOffset;
        final CompletableFuture<List<StreamRecordBatch>> cf;

        public BatchReadTask(RecordOffset startOffset, RecordOffset endOffset) {
            this.startOffset = DefaultRecordOffset.of(startOffset);
            this.endOffset = DefaultRecordOffset.of(endOffset);
            this.cf = new CompletableFuture<>();
        }

        @Override
        public String toString() {
            return "BatchReadTask{" +
                "startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                '}';
        }
    }

}
