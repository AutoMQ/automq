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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.util.WALUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_WITHOUT_CRC_SIZE;

public class RecoverIterator implements Iterator<RecoverResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverIterator.class);
    private final ObjectStorage objectStorage;
    private final int readAheadObjectSize;

    private final long trimOffset;
    private final List<WALObject> objectList;
    private final Queue<CompletableFuture<byte[]>> readAheadQueue;
    private final TreeMap<Long /* epoch startOffset */, Long /* epoch */> startOffset2Epoch = new TreeMap<>();

    private RecoverResult nextRecord = null;
    private int nextIndex = 0;
    private ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;

    public RecoverIterator(List<WALObject> objectList, ObjectStorage objectStorage,
        int readAheadObjectSize) {
        this.trimOffset = getTrimOffset(objectList, objectStorage);
        this.objectList = getContinuousFromTrimOffset(objectList, trimOffset);
        this.objectStorage = objectStorage;
        this.readAheadObjectSize = readAheadObjectSize;
        this.readAheadQueue = new ArrayDeque<>(readAheadObjectSize);

        long lastEpoch = -1L;
        for (WALObject object : objectList) {
            if (object.epoch() != lastEpoch) {
                startOffset2Epoch.put(object.startOffset(), object.epoch());
                lastEpoch = object.epoch();
            }
        }

        // Fill the read ahead queue.
        for (int i = 0; i < readAheadObjectSize; i++) {
            tryReadAhead();
        }
    }

    /**
     * Get the latest trim offset from the newest object.
     */
    private static long getTrimOffset(List<WALObject> objectList, ObjectStorage objectStorage) {
        if (objectList.isEmpty()) {
            return -1;
        }

        WALObject object = objectList.get(objectList.size() - 1);
        ObjectStorage.ReadOptions options = new ObjectStorage.ReadOptions()
            .throttleStrategy(ThrottleStrategy.BYPASS)
            .bucket(object.bucketId());
        ByteBuf buffer = objectStorage.rangeRead(options, object.path(), 0, Math.min(WALObjectHeader.MAX_WAL_HEADER_SIZE, object.length())).join();
        WALObjectHeader header = WALObjectHeader.unmarshal(buffer);
        buffer.release();
        return header.trimOffset();
    }

    // Visible for testing.
    static List<WALObject> getContinuousFromTrimOffset(List<WALObject> objectList, long trimOffset) {
        if (objectList.isEmpty()) {
            return Collections.emptyList();
        }

        int startIndex = objectList.size();
        for (int i = 0; i < objectList.size(); i++) {
            if (objectList.get(i).endOffset() > trimOffset) {
                startIndex = i;
                break;
            }
        }
        if (startIndex > 0) {
            for (int i = 0; i < startIndex; i++) {
                LOGGER.info("drop trimmed object: {}", objectList.get(i));
            }
        }
        if (startIndex >= objectList.size()) {
            return Collections.emptyList();
        }

        int endIndex = startIndex + 1;
        for (int i = startIndex + 1; i < objectList.size(); i++) {
            if (objectList.get(i).startOffset() != objectList.get(i - 1).endOffset()) {
                break;
            }
            endIndex = i + 1;
        }
        if (endIndex < objectList.size()) {
            for (int i = endIndex; i < objectList.size(); i++) {
                LOGGER.warn("drop discontinuous object: {}", objectList.get(i));
            }
        }

        return new ArrayList<>(objectList.subList(startIndex, endIndex));
    }

    @Override
    public boolean hasNext() {
        if (nextRecord != null) {
            return true;
        } else {
            while (hasNext0()) {
                RecoverResult record = next0();
                DefaultRecordOffset recordOffset = (DefaultRecordOffset) record.recordOffset();
                //noinspection DataFlowIssue
                if ((recordOffset.offset() <= trimOffset)
                    || (record.record().getStreamId() == -1L && record.record().getEpoch() == -1L)) {
                    record.record().release();
                    continue;
                }
                nextRecord = record;
                return true;
            }
            return false;
        }
    }

    private boolean hasNext0() {
        return dataBuffer.isReadable() || !readAheadQueue.isEmpty() || nextIndex < objectList.size();
    }

    private void loadNextBuffer() {
        // Please call hasNext() before calling loadNextBuffer().
        byte[] buffer = Objects.requireNonNull(readAheadQueue.poll()).join();
        dataBuffer = Unpooled.wrappedBuffer(buffer);

        // Check header
        WALObjectHeader header = WALObjectHeader.unmarshal(dataBuffer);
        dataBuffer.skipBytes(header.size());
    }

    private void tryReadAhead() {
        if (readAheadQueue.size() < readAheadObjectSize && nextIndex < objectList.size()) {
            WALObject object = objectList.get(nextIndex++);
            ObjectStorage.ReadOptions options = new ObjectStorage.ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(object.bucketId());
            CompletableFuture<byte[]> readFuture = objectStorage.rangeRead(options, object.path(), 0, object.length())
                .thenApply(buffer -> {
                    // Copy the result buffer and release it.
                    byte[] bytes = new byte[buffer.readableBytes()];
                    buffer.readBytes(bytes);
                    buffer.release();
                    return bytes;
                });
            readAheadQueue.add(readFuture);
        }
    }

    @Override
    public RecoverResult next() {
        if (nextRecord != null) {
            RecoverResult rst = nextRecord;
            nextRecord = null;
            return rst;
        }
        if (hasNext()) {
            return nextRecord;
        } else {
            return null;
        }
    }

    public RecoverResult next0() {
        // If there is no more data to read, return null.
        if (!dataBuffer.isReadable()) {
            loadNextBuffer();
        }

        // TODO: simple the code without strict batch

        // Try to read next object.
        tryReadAhead();

        ByteBuf recordHeaderBuf = dataBuffer.readBytes(RECORD_HEADER_SIZE);
        RecordHeader header = new RecordHeader(recordHeaderBuf);

        if (header.getRecordHeaderCRC() != WALUtil.crc32(recordHeaderBuf, RECORD_HEADER_WITHOUT_CRC_SIZE)) {
            recordHeaderBuf.release();
            throw new IllegalStateException("Record header crc check failed.");
        }
        recordHeaderBuf.release();

        if (header.getMagicCode() != RecordHeader.RECORD_HEADER_DATA_MAGIC_CODE) {
            throw new IllegalStateException("Invalid magic code in record header.");
        }

        int length = header.getRecordBodyLength();

        ByteBuf recordBuf = ByteBufAlloc.byteBuffer(length);
        if (dataBuffer.readableBytes() < length) {
            // Read the remain data and release the buffer.
            dataBuffer.readBytes(recordBuf, dataBuffer.readableBytes());
            dataBuffer.release();

            // Read from next buffer.
            if (!hasNext()) {
                throw new IllegalStateException("[Bug] There is a record part but no more data to read.");
            }
            loadNextBuffer();
            dataBuffer.readBytes(recordBuf, length - recordBuf.readableBytes());
        } else {
            dataBuffer.readBytes(recordBuf, length);
        }

        if (!dataBuffer.isReadable()) {
            dataBuffer.release();
        }

        if (header.getRecordBodyCRC() != WALUtil.crc32(recordBuf)) {
            recordBuf.release();
            throw new IllegalStateException("Record body crc check failed.");
        }

        long offset = header.getRecordBodyOffset() - RECORD_HEADER_SIZE;
        int size = recordBuf.readableBytes() + RECORD_HEADER_SIZE;

        return new RecoverResultImpl(StreamRecordBatchCodec.decode(recordBuf), DefaultRecordOffset.of(getEpoch(offset), offset, size));
    }

    private long getEpoch(long offset) {
        Map.Entry<Long, Long> entry = startOffset2Epoch.floorEntry(offset);
        if (entry == null) {
            LOGGER.error("[BUG] Cannot find any epoch for offset {}, startOffset2epoch={}", offset, startOffset2Epoch);
            throw new IllegalStateException("[BUG] Cannot find any epoch for offset");
        }
        return entry.getValue();
    }
}
