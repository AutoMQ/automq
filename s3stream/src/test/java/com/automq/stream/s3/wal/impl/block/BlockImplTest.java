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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.common.Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class BlockImplTest {

    static final int BODY_SIZE = 42;
    static final int RECORD_SIZE = RECORD_HEADER_SIZE + BODY_SIZE;
    static final Block.RecordSupplier RECORD_SUPPLIER = (offset, header) -> {
        header.writerIndex(header.capacity());
        ByteBuf body = ByteBufAlloc.byteBuffer(BODY_SIZE);
        body.writerIndex(body.capacity());
        return new Record(header, body);
    };

    private Block block;

    @AfterEach
    void tearDown() {
        if (block != null) {
            block.release();
        }
    }

    @Test
    public void testAddRecord() {
        block = new BlockImpl(0, Long.MAX_VALUE, Long.MAX_VALUE);
        long offset;
        offset = addRecord(block);
        assertEquals(0, offset);
        offset = addRecord(block);
        assertEquals(RECORD_SIZE, offset);
        assertNonEmptyBlock(block, 2);
    }

    @Test
    public void testExceedMaxSize() {
        block = new BlockImpl(0, 1, Long.MAX_VALUE);
        long offset;
        offset = addRecord(block);
        assertEquals(-1, offset, "Should return -1 when exceed max size");
        assertEmptyBlock(block);
    }

    @Test
    public void testExceedSoftLimit() {
        block = new BlockImpl(0, Long.MAX_VALUE, RECORD_SIZE);
        long offset;
        offset = addRecord(block);
        assertEquals(0, offset);
        offset = addRecord(block);
        assertEquals(-1, offset, "Should return -1 when exceed soft limit");
        assertNonEmptyBlock(block, 1);
    }

    @Test
    public void testOnlyOneRecordExceedSoftLimit() {
        block = new BlockImpl(0, Long.MAX_VALUE, 1);
        long offset;
        offset = addRecord(block);
        assertEquals(0, offset, "Should not fail when there is no record before, even exceed soft limit");
        assertNonEmptyBlock(block, 1);
    }

    @Test
    public void testFutures() {
        block = new BlockImpl(0, Long.MAX_VALUE, Long.MAX_VALUE);
        CompletableFuture<AppendResult.CallbackResult> future1 = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> future2 = new CompletableFuture<>();
        block.addRecord(RECORD_SIZE, RECORD_SUPPLIER, future1);
        block.addRecord(RECORD_SIZE, RECORD_SUPPLIER, future2);
        assertEquals(2, block.futures().size());
        assertTrue(block.futures().contains(future1));
        assertTrue(block.futures().contains(future2));
    }

    @Test
    public void testCallDataTwice() {
        block = new BlockImpl(0, Long.MAX_VALUE, Long.MAX_VALUE);
        addRecord(block);
        ByteBuf data1 = block.data();
        ByteBuf data2 = block.data();
        assertSame(data1, data2, "Should return the same data");
    }

    @Test
    public void testRelease() {
        block = new BlockImpl(0, Long.MAX_VALUE, Long.MAX_VALUE);

        ByteBuf body = ByteBufAlloc.byteBuffer(BODY_SIZE);
        body.writerIndex(body.capacity());

        Block.RecordSupplier recordSupplier = (offset, header) -> {
            header.writerIndex(header.capacity());
            return new Record(header, body);
        };
        block.addRecord(RECORD_SIZE, recordSupplier, new CompletableFuture<>());

        ByteBuf data = block.data();
        block.release();

        assertEquals(0, data.refCnt(), "Should release data");
        assertEquals(0, body.refCnt(), "Should release body");

        // avoid double release
        block = null;
    }

    private static long addRecord(Block block) {
        return block.addRecord(RECORD_SIZE, RECORD_SUPPLIER, new CompletableFuture<>());
    }

    private static void assertEmptyBlock(Block block) {
        assertEquals(0, block.size());
        assertTrue(block.futures().isEmpty());
        assertEquals(0, block.data().readableBytes());
    }

    private static void assertNonEmptyBlock(Block block, int recordCount) {
        assertEquals((long) recordCount * RECORD_SIZE, block.size());
        assertEquals(recordCount, block.futures().size());
        assertEquals(recordCount * RECORD_SIZE, block.data().readableBytes());
    }
}
