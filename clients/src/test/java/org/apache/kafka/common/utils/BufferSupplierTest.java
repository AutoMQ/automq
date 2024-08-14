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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BufferSupplierTest {

    @Test
    public void testNettyBuffer() {
        BufferSupplier.NettyBufferSupplier supplier = new BufferSupplier.NettyBufferSupplier();

        ByteBuffer buffer = supplier.get(1024);
        assertEquals(0, buffer.position());
        assertEquals(1024, buffer.capacity());
        assertEquals(1024, buffer.limit());
        assertEquals(1, supplier.bufferMap.size());

        // make sure modifying the buffer doesn't affect the supplier
        buffer.putInt(1);
        supplier.release(buffer);
        assertEquals(0, supplier.bufferMap.size());
    }
}
