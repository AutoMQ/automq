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

package com.automq.stream.s3.wal.impl.s3;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.AppendResultImpl;
import com.automq.stream.s3.wal.impl.RecoverResultImpl;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_MAGIC_CODE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;

public class S3WALService implements WriteAheadLog {
    // TODO: Accumulator: calculate offset and cache records
    private final RecordAccumulator accumulator;

    // TODO: Uploader: upload to S3
    private final ObjectStorage objectStorage;

    // TODO: Mode: Write or Recover

    // TODO: MetricsManager: record metrics

    public S3WALService(ObjectStorage objectStorage) {
        this.accumulator = new RecordAccumulator(objectStorage);
        this.objectStorage = objectStorage;
    }

    @Override
    public WriteAheadLog start() throws IOException {
        return null;
    }

    @Override
    public void shutdownGracefully() {

    }

    @Override
    public WALMetadata metadata() {
        return null;
    }

    private ByteBuf recordHeader(ByteBuf body, int crc, long start) {
        return new RecordHeader()
            .setMagicCode(RECORD_HEADER_MAGIC_CODE)
            .setRecordBodyLength(body.readableBytes())
            .setRecordBodyOffset(start + RECORD_HEADER_SIZE)
            .setRecordBodyCRC(crc)
            .marshal();
    }

    private ByteBuf record(ByteBuf body, int crc, long start) {
        CompositeByteBuf record = ByteBufAlloc.compositeByteBuffer();
        crc = 0 == crc ? WALUtil.crc32(body) : crc;
        record.addComponents(true, recordHeader(body, crc, start), body);
        return record;
    }

    @Override
    public AppendResult append(TraceContext context, ByteBuf data, int crc) throws OverCapacityException {
        // TODO: check wal mode
        final long recordSize = RECORD_HEADER_SIZE + data.readableBytes();
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        long expectedWriteOffset = accumulator.append(recordSize, start -> record(data, crc, start), appendResultFuture);

        return new AppendResultImpl(expectedWriteOffset, appendResultFuture);
    }

    @Override
    public Iterator<RecoverResult> recover() {
        // TODO: check wal mode
        return new Iterator<>() {
            private List<RecordAccumulator.WALObject> objectList = accumulator.objectList();
            private int nextIndex = 0;
            private ByteBuf record = PooledByteBufAllocator.DEFAULT.buffer();

            @Override
            public boolean hasNext() {
                return record.readableBytes() > 0 || nextIndex < objectList.size();
            }

            @Override
            public RecoverResult next() {
                // TODO: Read ahead
                if (hasNext()) {
                    // TODO: Read from trim offset.
                    RecordAccumulator.WALObject object = objectList.get(nextIndex++);
                    record = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();
                } else {
                    return null;
                }

                ByteBuf headerBuf = record.readBytes(RECORD_HEADER_SIZE);
                RecordHeader header = RecordHeader.unmarshal(headerBuf);
                headerBuf.release();

                int length = header.getRecordBodyLength();
                ByteBuf bodyBuf = record.readBytes(length);
                return new RecoverResultImpl(bodyBuf, header.getRecordBodyCRC());
            }
        };
    }

    @Override
    public CompletableFuture<Void> reset() {
        return trim(accumulator.nextOffset());
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        // TODO: write trim offset to s3 and then clean up expired objects asynchronously.
        return null;
    }
}
