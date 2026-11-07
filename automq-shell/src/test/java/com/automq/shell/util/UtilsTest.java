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

package com.automq.shell.util;

import com.automq.stream.s3.ByteBufAlloc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.buffer.ByteBuf;

@Timeout(60)
@Tag("S3Unit")
public class UtilsTest {

    @Test
    public void testCompression() {
        String testStr = "This is a test string";
        ByteBuf input = ByteBufAlloc.byteBuffer(testStr.length());
        input.writeBytes(testStr.getBytes());
        try {
            ByteBuf compressed = Utils.compress(input);
            ByteBuf decompressed = Utils.decompress(compressed);
            String decompressedStr = decompressed.toString(io.netty.util.CharsetUtil.UTF_8);
            System.out.printf("Original: %s, Decompressed: %s\n", testStr, decompressedStr);
            Assertions.assertEquals(testStr, decompressedStr);
        } catch (Exception e) {
            Assertions.fail("Exception occurred during compression/decompression: " + e.getMessage());
        }
    }
}
