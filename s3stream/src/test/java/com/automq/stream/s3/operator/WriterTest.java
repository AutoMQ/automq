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

package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies capacity-aware component writes without allocating large buffers.
 */
@Tag("S3Unit")
class WriterTest {
    /**
     * Components exceeding int in total are written separately, with ownership and failures preserved.
     */
    @Test
    void testWriteComponentsSplitsAtBufferCapacity() {
        Writer writer = mock(Writer.class);
        List<ByteBuf> writes = new ArrayList<>();
        CompletableFuture<Void> firstCf = new CompletableFuture<>();
        CompletableFuture<Void> secondCf = new CompletableFuture<>();
        when(writer.write(any())).thenAnswer(invocation -> {
            writes.add(invocation.getArgument(0));
            return writes.size() == 1 ? firstCf : secondCf;
        });
        ByteBuf first = sizedBuffer(1 << 30);
        ByteBuf second = sizedBuffer(1 << 30);
        ByteBuf footer = sizedBuffer(48);
        try {
            CompletableFuture<Void> cf = Writer.writeComponents(writer, first, second, footer);
            assertEquals(2, writes.size());
            assertEquals(1 << 30, writes.get(0).readableBytes());
            assertEquals((1 << 30) + 48, writes.get(1).readableBytes());
            secondCf.complete(null);
            assertFalse(cf.isDone());
            firstCf.completeExceptionally(new IllegalStateException("upload failed"));
            assertThrows(CompletionException.class, cf::join);
        } finally {
            writes.forEach(ByteBuf::release);
        }
        verify(first, times(1)).release();
        verify(second, times(1)).release();
        verify(footer, times(1)).release();
    }

    /**
     * A writer throwing after accepting the first part leaves no unsubmitted component references behind.
     */
    @Test
    void testWriteComponentsReleasesUnsubmittedBuffersOnFailure() {
        Writer writer = mock(Writer.class);
        ByteBuf first = sizedBuffer(1 << 30);
        ByteBuf second = sizedBuffer(1 << 30);
        ByteBuf footer = sizedBuffer(48);
        when(writer.write(any())).thenAnswer(invocation -> {
            ((ByteBuf) invocation.getArgument(0)).release();
            throw new IllegalStateException("write failed");
        });
        assertThrows(IllegalStateException.class, () -> Writer.writeComponents(writer, first, second, footer));
        verify(first, times(1)).release();
        verify(second, times(1)).release();
        verify(footer, times(1)).release();
    }

    private static ByteBuf sizedBuffer(int size) {
        ByteBuf buffer = spy(Unpooled.buffer(1).writeByte(0));
        doReturn(size).when(buffer).readableBytes();
        doReturn(size).when(buffer).writerIndex();
        doReturn(size).when(buffer).capacity();
        return buffer;
    }
}
