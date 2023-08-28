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

package kafka.log.s3;

import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitCompactObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class MinorCompactTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinorCompactTask.class);
    private static final long NOOP_TIMESTAMP = -1L;
    private final long compactSizeThreshold;
    private final long maxCompactInterval;
    private final int streamSplitSizeThreshold;
    private final BlockingQueue<MinorCompactPart> waitingCompactRecords = new LinkedBlockingQueue<>();
    private final AtomicLong waitingCompactRecordsBytesSize = new AtomicLong();
    private volatile long lastCompactTimestamp = NOOP_TIMESTAMP;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("minor compact", true));

    public MinorCompactTask(long compactSizeThreshold, long maxCompactInterval, int streamSplitSizeThreshold, ObjectManager objectManager, S3Operator s3Operator) {
        this.compactSizeThreshold = compactSizeThreshold;
        this.maxCompactInterval = maxCompactInterval;
        this.streamSplitSizeThreshold = streamSplitSizeThreshold;
        // TODO: close
        schedule.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    public void tryCompact(MinorCompactPart part) {
        // TODO: back pressure
        if (lastCompactTimestamp == NOOP_TIMESTAMP) {
            lastCompactTimestamp = System.currentTimeMillis();
        }
        waitingCompactRecords.add(part);
        if (waitingCompactRecordsBytesSize.addAndGet(part.size) >= compactSizeThreshold) {
            schedule.execute(() -> tryCompact0(false));
        }
    }

    @Override
    public void run() {
        tryCompact0(false);
    }

    public void close() {
        try {
            schedule.submit(() -> tryCompact0(true)).get();
            schedule.shutdown();
        } catch (Throwable e) {
            LOGGER.error("minor compact fail", e);
        }
    }

    private void tryCompact0(boolean force) {
        long now = System.currentTimeMillis();
        boolean timeout = lastCompactTimestamp != NOOP_TIMESTAMP && (now - lastCompactTimestamp) >= maxCompactInterval;
        boolean sizeExceed = waitingCompactRecordsBytesSize.get() >= compactSizeThreshold;
        if (!force && !sizeExceed && !timeout) {
            return;
        }
        try {
            List<MinorCompactPart> parts = new ArrayList<>(waitingCompactRecords.size());

            waitingCompactRecords.drainTo(parts);
            lastCompactTimestamp = now;
            waitingCompactRecordsBytesSize.getAndAdd(-parts.stream().mapToLong(r -> r.size).sum());
            if (parts.isEmpty()) {
                return;
            }

            CommitCompactObjectRequest compactRequest = new CommitCompactObjectRequest();
            compactRequest.setCompactedObjectIds(parts.stream().map(p -> p.walObjectId).collect(Collectors.toList()));

            long objectId = objectManager.prepareObject(1, TimeUnit.SECONDS.toMillis(30)).get();
            ObjectWriter minorCompactObject = new ObjectWriter(objectId, s3Operator);

            List<CompletableFuture<StreamObject>> streamObjectCfList = new LinkedList<>();
            List<List<StreamRecordBatch>> streamRecordsList = sortAndSplit(parts);
            for (List<StreamRecordBatch> streamRecords : streamRecordsList) {
                long streamSize = streamRecords.stream().mapToLong(r -> r.getRecordBatch().rawPayload().remaining()).sum();
                if (streamSize >= streamSplitSizeThreshold) {
                    streamObjectCfList.add(writeStreamObject(streamRecords));
                } else {
                    for (StreamRecordBatch record : streamRecords) {
                        minorCompactObject.write(record);
                    }
                    long streamId = streamRecords.get(0).getStreamId();
                    long startOffset = streamRecords.get(0).getBaseOffset();
                    long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
                    compactRequest.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset));
                    // minor compact object block only contain single stream's data.
                    minorCompactObject.closeCurrentBlock();
                }
            }
            minorCompactObject.close().get();

            compactRequest.setObjectId(objectId);
            compactRequest.setObjectSize(minorCompactObject.size());

            CompletableFuture.allOf(streamObjectCfList.toArray(new CompletableFuture[0])).get();
            for (CompletableFuture<StreamObject> cf : streamObjectCfList) {
                compactRequest.addStreamObject(cf.get());
            }

            objectManager.commitMinorCompactObject(compactRequest).get();
        } catch (Throwable e) {
            //TODO: handle exception, only expect fail when quit.
            LOGGER.error("minor compact fail", e);
        }

    }

    private CompletableFuture<StreamObject> writeStreamObject(List<StreamRecordBatch> streamRecords) {
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30));
        return objectIdCf.thenCompose(objectId -> {
            ObjectWriter streamObjectWriter = new ObjectWriter(objectId, s3Operator);
            for (StreamRecordBatch record : streamRecords) {
                streamObjectWriter.write(record);
            }
            long streamId = streamRecords.get(0).getStreamId();
            long startOffset = streamRecords.get(0).getBaseOffset();
            long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
            StreamObject streamObject = new StreamObject();
            streamObject.setObjectId(objectId);
            streamObject.setStreamId(streamId);
            streamObject.setStartOffset(startOffset);
            streamObject.setEndOffset(endOffset);
            return streamObjectWriter.close().thenApply(nil -> {
                streamObject.setObjectSize(streamObjectWriter.size());
                return streamObject;
            });
        });
    }

    /**
     * Sort records and split them in (stream, epoch) dimension.
     * ex.
     * part0: s1-e0-m1 s1-e0-m2 s2-e0-m1 s2-e0-m2
     * part1: s1-e0-m3 s1-e0-m4
     * part2: s1-e1-m10 s1-e1-m11
     * after split:
     * list0: s1-e0-m1 s1-e0-m2 s1-e0-m3 s1-e0-m4
     * list1: s1-e1-m10 s1-e1-m11
     * list2: s2-e0-m1 s2-e0-m3
     */
    private List<List<StreamRecordBatch>> sortAndSplit(List<MinorCompactPart> parts) {
        int count = parts.stream().mapToInt(p -> p.records.size()).sum();
        // TODO: more efficient sort
        List<StreamRecordBatch> sortedList = new ArrayList<>(count);
        for (MinorCompactPart part : parts) {
            sortedList.addAll(part.records);
        }
        Collections.sort(sortedList);
        List<List<StreamRecordBatch>> streamRecordsList = new ArrayList<>(1024);
        long streamId = -1L;
        long epoch = -1L;
        List<StreamRecordBatch> streamRecords = null;
        for (StreamRecordBatch record : sortedList) {
            long recordStreamId = record.getStreamId();
            long recordEpoch = record.getEpoch();
            if (recordStreamId != streamId || recordEpoch != epoch) {
                if (streamRecords != null) {
                    streamRecordsList.add(streamRecords);
                }
                streamRecords = new LinkedList<>();
                streamId = recordStreamId;
                epoch = recordEpoch;
            }
            if (streamRecords != null) {
                streamRecords.add(record);
            }
        }
        if (streamRecords != null) {
            streamRecordsList.add(streamRecords);
        }
        return streamRecordsList;
    }


    static class MinorCompactPart {
        long walObjectId;
        List<StreamRecordBatch> records;
        long size;

        public MinorCompactPart(long walObjectId, List<StreamRecordBatch> records) {
            this.walObjectId = walObjectId;
            this.records = new ArrayList<>(records);
            this.size = records.stream().mapToLong(r -> r.getRecordBatch().rawPayload().remaining()).sum();
        }
    }
}
