/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.AppendResultImpl;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_WITHOUT_CRC_SIZE;

public class ObjectWALService implements WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(ObjectWALService.class);

    protected ObjectStorage objectStorage;
    protected ObjectWALConfig config;

    protected RecordAccumulator accumulator;

    public ObjectWALService(Time time, ObjectStorage objectStorage, ObjectWALConfig config) {
        this.objectStorage = objectStorage;
        this.config = config;

        this.accumulator = new RecordAccumulator(time, objectStorage, config);
    }

    // Visible for testing.
    protected RecordAccumulator accumulator() {
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
        final long recordSize = RECORD_HEADER_SIZE + data.readableBytes();
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        long expectedWriteOffset = accumulator.append(recordSize, start -> WALUtil.generateRecord(data, crc, start), appendResultFuture);

        return new AppendResultImpl(expectedWriteOffset, appendResultFuture);
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return new RecoverIterator(accumulator.objectList(), objectStorage, config.readAheadObjectCount());
    }

    @Override
    public CompletableFuture<Void> reset() {
        return trim(Long.MAX_VALUE);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        log.info("Trim S3 WAL to offset: {}", offset);
        return accumulator.trim(offset);
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
            this.objectList = objectList;
            this.objectStorage = objectStorage;
            this.readAheadObjectSize = readAheadObjectSize;
            this.readAheadQueue = new ArrayDeque<>(readAheadObjectSize);

            // Fill the read ahead queue.
            for (int i = 0; i < readAheadObjectSize; i++) {
                tryReadAhead();
            }
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
            dataBuffer.skipBytes(WALObjectHeader.WAL_HEADER_SIZE);

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
            RecordHeader header = RecordHeader.unmarshal(recordHeaderBuf);

            if (header.getRecordHeaderCRC() != WALUtil.crc32(recordHeaderBuf, RECORD_HEADER_WITHOUT_CRC_SIZE)) {
                recordHeaderBuf.release();
                throw new IllegalStateException("Record header crc check failed.");
            }
            recordHeaderBuf.release();

            if (header.getMagicCode() != RecordHeader.RECORD_HEADER_MAGIC_CODE) {
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

            return new RecoverResultImpl(recordBuf, header.getRecordBodyCRC());
        }
    }
}
