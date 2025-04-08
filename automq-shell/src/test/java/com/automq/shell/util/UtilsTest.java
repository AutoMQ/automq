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
