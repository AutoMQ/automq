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

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NetworkReceiveTest {

    @Test
    public void testBytesRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);

        Mockito.when(channel.read(captor.capture()))
            .thenAnswer(invocation -> {
                ByteBuffer buf = captor.getValue();
                if (buf.remaining() == 4) {
                    buf.putInt(128);
                    return 4;
                }
                int toWrite = Math.min(buf.remaining(), 64);
                buf.put(TestUtils.randomBytes(toWrite));
                return toWrite;
            });

        int totalRead = 0;
        while (!receive.complete()) {
            totalRead += receive.readFrom(channel);
        }

        assertEquals(132, receive.bytesRead());
        assertEquals(132, totalRead);
        assertTrue(receive.complete());

        receive.close();
    }

    @Test
    public void testZeroSizePayload() throws IOException {
        NetworkReceive receive = new NetworkReceive("zero");

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);

        Mockito.when(channel.read(captor.capture()))
            .thenAnswer(invocation -> {
                ByteBuffer buf = captor.getValue();
                if (buf.remaining() == 4) {
                    buf.putInt(0);
                    return 4;
                }
                return 0;
            });

        receive.readFrom(channel);

        assertTrue(receive.complete());
        assertEquals(0, receive.payload().remaining());

        receive.close();
    }

    @Test
    public void testWithMemoryPoolNone() throws IOException {
        NetworkReceive receive =
            new NetworkReceive(1024, "none", MemoryPool.NONE);

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);

        Mockito.when(channel.read(captor.capture()))
            .thenAnswer(invocation -> {
                ByteBuffer buf = captor.getValue();
                if (buf.remaining() == 4) {
                    buf.putInt(256);
                    return 4;
                }
                buf.put(TestUtils.randomBytes(buf.remaining()));
                return buf.remaining();
            });

        while (!receive.complete()) {
            receive.readFrom(channel);
        }

        assertTrue(receive.complete());
        receive.close();
    }
}
