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
