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

package com.automq.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedSizeByteBufPoolTest {

    private static final int BUFFER_SIZE = 42;

    private FixedSizeByteBufPool pool;

    @BeforeEach
    void setUp() {
        pool = new FixedSizeByteBufPool(BUFFER_SIZE, 2);
    }

    @Test
    void testGetBufferFromEmptyPool() {
        ByteBuf buffer = pool.get();
        assertNotNull(buffer, "Buffer should not be null");
        assertEquals(BUFFER_SIZE, buffer.capacity(), "Buffer size should be " + BUFFER_SIZE);
    }

    @Test
    void testReleaseAndReuseBuffer() {
        ByteBuf buffer = pool.get();
        assertNotNull(buffer, "Buffer should not be null");
        assertEquals(BUFFER_SIZE, buffer.capacity(), "Buffer size should be " + BUFFER_SIZE);

        buffer.writeInt(42);
        pool.release(buffer);

        ByteBuf buffer2 = pool.get();
        assertSame(buffer, buffer2, "Buffer should be reused from the pool");
        assertEquals(0, buffer2.readableBytes(), "Buffer should be cleared on release");
        assertEquals(BUFFER_SIZE, buffer2.writableBytes(), "Buffer should be cleared on release");
    }

    @Test
    void testReleaseBufferWithDifferentSizeThrowsAssertionError() {
        ByteBuf buffer = Unpooled.buffer(128); // Different size
        assertThrows(AssertionError.class, () -> pool.release(buffer), "Should throw AssertionError for different buffer size");
    }

    @Test
    void testMultipleBuffers() {
        ByteBuf buffer1 = pool.get();
        ByteBuf buffer2 = pool.get();

        assertNotSame(buffer1, buffer2, "Different calls to get() should return different buffers");

        pool.release(buffer1);
        pool.release(buffer2);

        ByteBuf buffer3 = pool.get();
        ByteBuf buffer4 = pool.get();

        assertTrue(buffer3 == buffer1 || buffer3 == buffer2, "Buffer3 should be one of the released buffers");
        assertTrue(buffer4 == buffer1 || buffer4 == buffer2, "Buffer4 should be one of the released buffers");
        assertNotSame(buffer3, buffer4, "Buffer3 and Buffer4 should not be the same after release");
    }

    @Test
    void testOverCapacity() {
        ByteBuf buffer1 = pool.get();
        ByteBuf buffer2 = pool.get();
        ByteBuf buffer3 = pool.get();

        pool.release(buffer1);
        pool.release(buffer2);
        pool.release(buffer3);

        ByteBuf buffer4 = pool.get();
        ByteBuf buffer5 = pool.get();
        ByteBuf buffer6 = pool.get();

        assertSame(buffer4, buffer1, "Buffer4 should be reused from the pool");
        assertSame(buffer5, buffer2, "Buffer5 should be reused from the pool");
        assertNotSame(buffer6, buffer3, "Buffer6 should not be reused from the pool");
    }
}
