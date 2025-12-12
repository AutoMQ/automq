/*
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
package org.apache.kafka.common.network;

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NetworkReceiveTest {

    @BeforeEach
    public void setUp() throws Exception {
        clearSizeBufferPool();
    }

    @Test
    public void testBytesRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");
        assertEquals(0, receive.bytesRead());

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(128);
            return 4;
        }).thenReturn(0);

        assertEquals(4, receive.readFrom(channel));
        assertEquals(4, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(68, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(132, receive.bytesRead());
        assertTrue(receive.complete());
        
        receive.close();
    }

    /**
     * Test that size buffers are reused from the pool after close
     * @throws Exception 
     */
    @Test
    public void testSizeBufferPoolReuse() throws Exception {
        int initialPoolSize = getSizeBufferPoolSize();
        assertEquals(0, initialPoolSize);

        NetworkReceive receive1 = new NetworkReceive("test-1");
        receive1.close();

        int poolSizeAfterClose = getSizeBufferPoolSize();
        assertEquals(1, poolSizeAfterClose, "Size buffer should be returned to pool");

        NetworkReceive receive2 = new NetworkReceive("test-2");
        int poolSizeAfterReuse = getSizeBufferPoolSize();
        assertEquals(0, poolSizeAfterReuse, "Buffer should be reused from pool");
        
        receive2.close();
    }

    /**
     * Test that close() properly releases resources
     * @throws Exception 
     */
    @Test
    public void testCloseReleasesBuffers() throws Exception {
        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);
        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer buf = bufferCaptor.getValue();
            if (buf.capacity() == 4) {
                buf.putInt(1024);
                return 4;
            }
            return 0;
        });

        NetworkReceive receive = new NetworkReceive(1024, "test-close");
        receive.readFrom(channel);
        assertNotNull(receive.payload());

        receive.close();
        assertEquals(1, getSizeBufferPoolSize(), "Size buffer should be released to pool");
    }

    // Helper method
    private int getSizeBufferPoolSize() throws Exception {
        Field field = NetworkReceive.class.getDeclaredField("SIZE_BUFFER_POOL");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Queue<ByteBuffer> pool = (Queue<ByteBuffer>) field.get(null);
        return pool.size();
    }

    private void clearSizeBufferPool() throws Exception {
        Field field = NetworkReceive.class.getDeclaredField("SIZE_BUFFER_POOL");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Queue<ByteBuffer> pool = (Queue<ByteBuffer>) field.get(null);
        pool.clear();
    }
}