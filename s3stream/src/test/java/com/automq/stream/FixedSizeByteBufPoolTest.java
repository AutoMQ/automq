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

package com.automq.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        pool = new FixedSizeByteBufPool(BUFFER_SIZE);
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
}
