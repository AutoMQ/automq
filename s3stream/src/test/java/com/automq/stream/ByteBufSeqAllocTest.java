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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ByteBufSeqAllocTest {

    @Test
    public void testAlloc() {
        ByteBufSeqAlloc alloc = new ByteBufSeqAlloc(0, 1);

        AtomicReference<ByteBufSeqAlloc.HugeBuf> bufRef = alloc.hugeBufArray[Math.abs(Thread.currentThread().hashCode() % alloc.hugeBufArray.length)];

        ByteBuf buf1 = alloc.alloc(12);
        buf1.writeLong(1);
        buf1.writeInt(2);

        ByteBuf buf2 = alloc.alloc(20);
        buf2.writeLong(3);
        buf2.writeInt(4);
        buf2.writeLong(5);

        ByteBuf buf3 = alloc.alloc(ByteBufSeqAlloc.HUGE_BUF_SIZE - 12 - 20 - 4);

        ByteBuf oldHugeBuf = bufRef.get().buf;

        ByteBuf buf4 = alloc.alloc(16);
        buf4.writeLong(6);
        buf4.writeLong(7);

        assertNotSame(oldHugeBuf, bufRef.get().buf);

        assertEquals(1, buf1.readLong());
        assertEquals(2, buf1.readInt());
        assertEquals(3, buf2.readLong());
        assertEquals(4, buf2.readInt());
        assertEquals(5, buf2.readLong());
        assertEquals(6, buf4.readLong());
        assertEquals(7, buf4.readLong());

        buf1.release();
        buf2.release();
        buf3.release();
        buf4.release();
        assertEquals(0, oldHugeBuf.refCnt());
        assertEquals(1, bufRef.get().buf.refCnt());

        ByteBuf oldHugeBuf2 = bufRef.get().buf;

        alloc.alloc(ByteBufSeqAlloc.HUGE_BUF_SIZE - 12).release();
        alloc.alloc(12).release();
        assertEquals(0, oldHugeBuf2.refCnt());
    }

}
