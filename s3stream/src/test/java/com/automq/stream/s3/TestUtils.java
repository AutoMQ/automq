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

package com.automq.stream.s3;

import java.util.Random;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class TestUtils {

    public static ByteBuf random(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        // In the most test cases, the generated ByteBuf will be released after write to S3.
        // To give the ByteBuf a chance to assert in the unit tests, we just retain it here.
        // Since the retained ByteBuf is unpooled, it will be released by the GC.
        return Unpooled.wrappedBuffer(bytes).retain();
    }

    public static ByteBuf randomPooled(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(size);
        buf.writeBytes(bytes);
        return buf;
    }

    public static String tempFilePath() {
        return System.getProperty("java.io.tmpdir") + "/kos-" + UUID.randomUUID();
    }
}
