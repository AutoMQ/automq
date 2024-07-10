/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.object;

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
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;

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
        return new Iterator<>() {
            // TODO: Recover only the objects with the latest epoch.
            private List<RecordAccumulator.WALObject> objectList = accumulator.objectList();
            private int nextIndex = 0;
            private ByteBuf record = PooledByteBufAllocator.DEFAULT.buffer();

            @Override
            public boolean hasNext() {
                return record.isReadable() || nextIndex < objectList.size();
            }

            @Override
            public RecoverResult next() {
                if (!hasNext()) {
                    return null;
                }

                // TODO: Read ahead
                if (!record.isReadable()) {
                    RecordAccumulator.WALObject object = objectList.get(nextIndex++);
                    record = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();

                    // Check header
                    WALObjectHeader.unmarshal(record);
                    record.skipBytes(WALObjectHeader.WAL_HEADER_SIZE);
                }

                ByteBuf recordHeaderBuf = record.readBytes(RECORD_HEADER_SIZE);
                RecordHeader header = RecordHeader.unmarshal(recordHeaderBuf);
                recordHeaderBuf.release();

                int length = header.getRecordBodyLength();
                ByteBuf recordBuf = record.readBytes(length);

                if (!record.isReadable()) {
                    record.release();
                }

                return new RecoverResultImpl(recordBuf, header.getRecordBodyCRC());
            }
        };
    }

    @Override
    public CompletableFuture<Void> reset() {
        return trim(accumulator.nextOffset() - 1);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        return accumulator.trim(offset);
    }
}
