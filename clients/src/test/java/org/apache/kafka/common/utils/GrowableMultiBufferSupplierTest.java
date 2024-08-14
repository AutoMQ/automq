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

package org.apache.kafka.common.utils;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class GrowableMultiBufferSupplierTest {

    private BufferSupplier.GrowableMultiBufferSupplier bufferSupplier;

    @BeforeEach
    void setUp() {
        bufferSupplier = new BufferSupplier.GrowableMultiBufferSupplier();
    }

    @Test
    void testGetWhenNoBuffersAvailable() {
        ByteBuffer result = bufferSupplier.get(10);
        assertEquals(10, result.capacity());
    }

    @Test
    void testGetWithSufficientCapacity() {
        ByteBuffer buffer = bufferSupplier.get(10);
        bufferSupplier.release(buffer);

        ByteBuffer result = bufferSupplier.get(5);
        assertSame(buffer, result);
        assertEquals(0, result.position());
        assertEquals(10, result.capacity());
    }

    @Test
    void testGetWithInsufficientCapacity() {
        ByteBuffer buffer = bufferSupplier.get(10);
        bufferSupplier.release(buffer);

        ByteBuffer result = bufferSupplier.get(15);
        assertNotSame(buffer, result);
        assertEquals(15, result.capacity());
    }

    @Test
    void testGetAndReleaseMultipleBuffers() {
        ByteBuffer buffer1 = bufferSupplier.get(10);
        ByteBuffer buffer2 = bufferSupplier.get(15);
        bufferSupplier.release(buffer1);
        bufferSupplier.release(buffer2);

        ByteBuffer result1 = bufferSupplier.get(5);
        ByteBuffer result2 = bufferSupplier.get(10);
        assertSame(buffer1, result1);
        assertSame(buffer2, result2);
        assertEquals(0, result1.position());
        assertEquals(0, result2.position());
    }

    @Test
    void testRelease() {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put((byte) 1);
        bufferSupplier.release(buffer);

        ByteBuffer result = bufferSupplier.get(5);
        assertSame(buffer, result);
        assertEquals(0, result.position());
    }
}
