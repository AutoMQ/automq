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

package org.apache.kafka.common.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

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

        ByteBuffer result2 = bufferSupplier.get(5);
        ByteBuffer result1 = bufferSupplier.get(10);
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
