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

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.TestUtils;
import io.github.bucket4j.Bucket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class WALChannelTest {
    public static final String TEST_BLOCK_DEVICE_KEY = "WAL_TEST_BLOCK_DEVICE";

    long gbPerSeconds = 1024 * 1024 * 1024L;
    long mbPerSeconds = 1024 * 1024;

    WALChannel walChannel;

    @BeforeEach
    void setUp() {
        walChannel = WALChannel.builder(String.format("%s/WALChannelUnitTest.data", TestUtils.tempFilePath())).direct(false).capacity(1024 * 1024 * 20).build();
        try {
            walChannel.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        walChannel.close();
    }

    ByteBuffer createRandomTextByteBuffer(int size) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);

        for (int i = 0; i < size; i++) {
            byteBuffer.put("ABCDEFGH".getBytes()[i % 8]);
        }

        return byteBuffer.flip();
    }

    @Test
    void testWriteAndRead() throws IOException {
        ByteBuf data = TestUtils.random(1024 * 3);
        for (int i = 0; i < 100; i++) {
            try {
                walChannel.write(data, (long) i * data.readableBytes());
                walChannel.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final String content = "Hello World";
        walChannel.write(Unpooled.wrappedBuffer(content.getBytes()), 100);
        walChannel.flush();

        ByteBuf readBuffer = Unpooled.buffer(content.length());
        int read = walChannel.read(readBuffer, 100);

        String readString = new String(readBuffer.array());
        System.out.println(new String(readBuffer.array()));
        System.out.println(read);

        assert read == content.length();
        assert readString.equals(content);
    }

    @Test
    void testWALBandwidthLimit() throws InterruptedException {
        Bucket bucket = WALBlockDeviceChannel.buildBandWidthRateLimiter(150 * mbPerSeconds, 4096);
        TimeUnit.MILLISECONDS.sleep(10);
        Assertions.assertFalse(bucket.tryConsume((150 * mbPerSeconds >> WALBlockDeviceChannel.BANDWIDTH_RATE_LIMIT_SCALE_FACTOR) + 2));
        Assertions.assertTrue(bucket.tryConsume(150 * mbPerSeconds >> WALBlockDeviceChannel.BANDWIDTH_RATE_LIMIT_SCALE_FACTOR));
    }

    @Test
    void testWALBandwidthLimitConfig() {
        try {
            WALBlockDeviceChannel.validRefillPerMs(
                150 * mbPerSeconds,
                512);

            WALBlockDeviceChannel.validRefillPerMs(
                150 * mbPerSeconds,
                4096);

            WALBlockDeviceChannel.validRefillPerMs(
                3 * gbPerSeconds,
                512);

            WALBlockDeviceChannel.validRefillPerMs(
                3 * gbPerSeconds,
                4096);

            WALBlockDeviceChannel.validRefillPerMs(
                WALBlockDeviceChannel.MAX_RATE_LIMIT_BYTES_PER_MS * 1000L,
                512);

            WALBlockDeviceChannel.validRefillPerMs(
                WALBlockDeviceChannel.MAX_RATE_LIMIT_BYTES_PER_MS * 1000L,
                4096);

        } catch (Exception e) {
            Assertions.fail("should not throw exception", e);
        }

        boolean exceptionThrows = false;
        try {
            WALBlockDeviceChannel.validRefillPerMs(Long.MAX_VALUE, 4096);
        } catch (Exception e) {
            // expected.
            exceptionThrows = true;
        }

        assertTrue(exceptionThrows);

    }
}
