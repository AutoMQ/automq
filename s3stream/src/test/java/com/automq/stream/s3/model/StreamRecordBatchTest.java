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

package com.automq.stream.s3.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamRecordBatchTest {

    @Test
    public void testOf() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
        StreamRecordBatch record = StreamRecordBatch.of(1L, 2L, 3L, 4, payloadBuf);
        assertEquals(1, record.getStreamId());
        assertEquals(2, record.getEpoch());
        assertEquals(3, record.getBaseOffset());
        assertEquals(4, record.getCount());
        assertEquals(payload.length, record.size());
        assertEquals(0, payloadBuf.refCnt());
        byte[] realPayload = new byte[payload.length];
        record.getPayload().readBytes(realPayload);
        assertArrayEquals(payload, realPayload);
        record.release();
        assertEquals(0, record.encoded.refCnt());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testParse(boolean duplicated) {
        CompositeByteBuf buf = Unpooled.compositeBuffer();
        for (int i = 0; i < 10; i++) {
            ByteBuf payloadBuf = Unpooled.wrappedBuffer(("hello" + i).getBytes(StandardCharsets.UTF_8));
            StreamRecordBatch record = StreamRecordBatch.of(1L, 2L, 3L + i, 4, payloadBuf);
            buf.addComponent(true, record.encoded());
        }
        for (int i = 0; i < 10; i++) {
            StreamRecordBatch record = StreamRecordBatch.parse(buf, duplicated);
            assertEquals(3 + i, record.getBaseOffset());
            ByteBuf payloadBuf = record.getPayload();
            byte[] payload = new byte[payloadBuf.readableBytes()];
            payloadBuf.readBytes(payload);
            assertArrayEquals(("hello" + i).getBytes(StandardCharsets.UTF_8), payload);
            record.release();
            if (duplicated) {
                assertEquals(0, record.encoded.refCnt());
            }
        }
        assertEquals(0, buf.readableBytes());
        assertEquals(1, buf.refCnt());
        buf.release();
    }

}
