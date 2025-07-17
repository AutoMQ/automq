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
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.ReservationService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.AppendResultImpl;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.ByteBufAlloc.S3_WAL;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_WITHOUT_CRC_SIZE;

public class ObjectWALService implements WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(ObjectWALService.class);
    private static final ByteBufSeqAlloc BYTE_BUF_ALLOC = new ByteBufSeqAlloc(S3_WAL, 8);

    protected ObjectStorage objectStorage;
    protected ObjectWALConfig config;

    protected ReservationService reservationService;
    protected RecordAccumulator accumulator;

    public ObjectWALService(Time time, ObjectStorage objectStorage, ObjectWALConfig config) {
        this.objectStorage = objectStorage;
        this.config = config;

        this.reservationService = new ObjectReservationService(config.clusterId(), objectStorage, objectStorage.bucketId());
        this.accumulator = new RecordAccumulator(time, objectStorage, reservationService, config);
    }

    // Visible for testing.
    RecordAccumulator accumulator() {
        return accumulator;
    }

    @Override
    public WriteAheadLog start() throws IOException {
        log.info("Start S3 WAL.");
        accumulator.start();
        return this;
    }

    @Override
    public void shutdownGracefully() {
        log.info("Shutdown S3 WAL.");
        accumulator.close();
    }

    @Override
    public WALMetadata metadata() {
        return new WALMetadata(config.nodeId(), config.epoch());
    }

    @Override
    public AppendResult append(TraceContext context, ByteBuf data, int crc) throws OverCapacityException {
        ByteBuf header = BYTE_BUF_ALLOC.byteBuffer(RECORD_HEADER_SIZE);
        assert header.refCnt() == 1;

        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        try {
            final long recordSize = RECORD_HEADER_SIZE + data.readableBytes();

            long expectedWriteOffset = accumulator.append(recordSize, start -> {
                CompositeByteBuf recordByteBuf = ByteBufAlloc.compositeByteBuffer();
                Record record = WALUtil.generateRecord(data, header, 0, start);
                recordByteBuf.addComponents(true, record.header(), record.body());
                return recordByteBuf;
            }, appendResultFuture);

            return new AppendResultImpl(expectedWriteOffset, appendResultFuture);
        } catch (Exception e) {
            // Make sure the header buffer and data buffer is released.
            if (header.refCnt() > 0) {
                header.release();
            } else {
                log.error("[Bug] The header buffer is already released.", e);
            }

            if (data.refCnt() > 0) {
                data.release();
            } else {
                log.error("[Bug] The data buffer is already released.", e);
            }

            // Complete the future with exception, ensure the whenComplete method is executed.
            appendResultFuture.completeExceptionally(e);
            Throwable cause = ExceptionUtils.getRootCause(e);
            if (cause instanceof OverCapacityException) {
                if (((OverCapacityException) cause).error()) {
                    log.warn("Append record to S3 WAL failed, due to accumulator is full.", e);
                } else {
                    log.warn("S3 WAL accumulator is full, try to trigger an upload and trim the WAL", e);
                }

                throw new OverCapacityException("Append record to S3 WAL failed, due to accumulator is full: " + cause.getMessage());
            } else {
                log.error("Append record to S3 WAL failed, due to unrecoverable exception.", e);
                return new AppendResultImpl(-1, appendResultFuture);
            }
        }
    }

    @Override
    public Iterator<RecoverResult> recover() {
        try {
            return new RecoverIterator(accumulator.objectList(), objectStorage, config.readAheadObjectCount());
        } catch (WALFencedException e) {
            log.error("Recover S3 WAL failed, due to unrecoverable exception.", e);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    throw new RuntimeIOException(e);
                }

                @Override
                public RecoverResult next() {
                    throw new RuntimeIOException(e);
                }
            };
        }
    }

    @Override
    public CompletableFuture<Void> reset() {
        log.info("Reset S3 WAL");
        try {
            return accumulator.reset();
        } catch (Throwable e) {
            log.error("Reset S3 WAL failed, due to unrecoverable exception.", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        log.info("Trim S3 WAL to offset: {}", offset);
        try {
            return accumulator.trim(offset);
        } catch (Throwable e) {
            log.error("Trim S3 WAL failed, due to unrecoverable exception.", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    public static class RecoverIterator implements Iterator<RecoverResult> {
        private final ObjectStorage objectStorage;
        private final int readAheadObjectSize;

        private final List<RecordAccumulator.WALObject> objectList;
        private final Queue<CompletableFuture<byte[]>> readAheadQueue;

        private int nextIndex = 0;
        private ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;

        public RecoverIterator(List<RecordAccumulator.WALObject> objectList, ObjectStorage objectStorage,
            int readAheadObjectSize) {
            long trimOffset = getTrimOffset(objectList, objectStorage);
            this.objectList = getContinuousFromTrimOffset(objectList, trimOffset);
            this.objectStorage = objectStorage;
            this.readAheadObjectSize = readAheadObjectSize;
            this.readAheadQueue = new ArrayDeque<>(readAheadObjectSize);

            // Fill the read ahead queue.
            for (int i = 0; i < readAheadObjectSize; i++) {
                tryReadAhead();
            }
        }

        /**
         * Get the latest trim offset from the newest object.
         */
        private static long getTrimOffset(List<RecordAccumulator.WALObject> objectList, ObjectStorage objectStorage) {
            if (objectList.isEmpty()) {
                return -1;
            }

            RecordAccumulator.WALObject object = objectList.get(objectList.size() - 1);
            ObjectStorage.ReadOptions options = new ObjectStorage.ReadOptions()
                .throttleStrategy(ThrottleStrategy.BYPASS)
                .bucket(object.bucketId());
            ByteBuf buffer = objectStorage.rangeRead(options, object.path(), 0, object.length()).join();
            WALObjectHeader header = WALObjectHeader.unmarshal(buffer);
            buffer.release();
            return header.trimOffset();
        }

        // Visible for testing.
        static List<RecordAccumulator.WALObject> getContinuousFromTrimOffset(List<RecordAccumulator.WALObject> objectList, long trimOffset) {
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
                    log.warn("drop trimmed object: {}", objectList.get(i));
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
                    log.warn("drop discontinuous object: {}", objectList.get(i));
                }
            }

            return new ArrayList<>(objectList.subList(startIndex, endIndex));
        }


        @Override
        public boolean hasNext() {
            return dataBuffer.isReadable() || !readAheadQueue.isEmpty() || nextIndex < objectList.size();
        }

        private void loadNextBuffer(boolean skipStickyRecord) {
            // Please call hasNext() before calling loadNextBuffer().
            byte[] buffer = Objects.requireNonNull(readAheadQueue.poll()).join();
            dataBuffer = Unpooled.wrappedBuffer(buffer);

            // Check header
            WALObjectHeader header = WALObjectHeader.unmarshal(dataBuffer);
            dataBuffer.skipBytes(header.size());

            if (skipStickyRecord && header.stickyRecordLength() != 0) {
                dataBuffer.skipBytes((int) header.stickyRecordLength());
            }
        }

        private void tryReadAhead() {
            if (readAheadQueue.size() < readAheadObjectSize && nextIndex < objectList.size()) {
                RecordAccumulator.WALObject object = objectList.get(nextIndex++);
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
            // If there is no more data to read, return null.
            if (!hasNext()) {
                return null;
            }

            if (!dataBuffer.isReadable()) {
                loadNextBuffer(true);
            }

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
                loadNextBuffer(false);
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

            return new RecoverResultImpl(recordBuf, header.getRecordBodyOffset() - RECORD_HEADER_SIZE);
        }
    }
}
