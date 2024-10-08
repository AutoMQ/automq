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

package com.automq.stream.s3.wal.impl;

import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MemoryWriteAheadLog implements WriteAheadLog {
    private final AtomicLong offsetAlloc = new AtomicLong();
    private final ConcurrentSkipListMap<Long, ByteBuf> dataMap = new ConcurrentSkipListMap<>();
    private volatile boolean full = false;

    public boolean full() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    public WriteAheadLog start() throws IOException {
        return this;
    }

    @Override
    public void shutdownGracefully() {

    }

    @Override
    public WALMetadata metadata() {
        return new WALMetadata(0, 0);
    }

    @Override
    public AppendResult append(TraceContext traceContext, ByteBuf data, int crc) throws OverCapacityException {
        if (full) {
            data.release();
            throw new OverCapacityException("MemoryWriteAheadLog is full");
        }
        int dataLength = data.readableBytes();
        long offset = offsetAlloc.getAndAdd(RecordHeader.RECORD_HEADER_SIZE + dataLength);

        ByteBuf buffer = Unpooled.buffer(dataLength);
        buffer.writeBytes(data);
        data.release();
        dataMap.put(offset, buffer);

        return new AppendResult() {
            @Override
            public long recordOffset() {
                return offset;
            }

            @Override
            public CompletableFuture<CallbackResult> future() {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return dataMap.entrySet()
            .stream()
            .map(e -> (RecoverResult) new RecoverResult() {
                @Override
                public ByteBuf record() {
                    return e.getValue();
                }

                @Override
                public long recordOffset() {
                    return e.getKey();
                }
            })
            .collect(Collectors.toList())
            .iterator();
    }

    @Override
    public CompletableFuture<Void> reset() {
        dataMap.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        dataMap.headMap(offset)
            .forEach((key, value) -> {
                dataMap.remove(key);
                value.release();
            });
        return CompletableFuture.completedFuture(null);
    }
}
