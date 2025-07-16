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

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.utils.CommandResult;
import com.automq.stream.utils.CommandUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;

public class WALUtil {
    public static final String BLOCK_SIZE_PROPERTY = "automq.ebswal.blocksize";
    public static final int BLOCK_SIZE = Integer.parseInt(System.getProperty(
        BLOCK_SIZE_PROPERTY,
        "4096"
    ));

    private static final Logger LOGGER = LoggerFactory.getLogger(WALUtil.class);

    public static Record generateRecord(ByteBuf body, ByteBuf emptyHeader, int crc, long start) {
        crc = 0 == crc ? WALUtil.crc32(body) : crc;
        ByteBuf header = new RecordHeader(start, body.readableBytes(), crc).marshal(emptyHeader);
        return new Record(header, body);
    }

    public static Record generatePaddingRecord(ByteBuf emptyHeader, long start, int length) {
        int bodyLength = length - RECORD_HEADER_SIZE;

        ByteBuf header = new RecordHeader(start, bodyLength).marshal(emptyHeader);

        ByteBuf body = ByteBufAlloc.byteBuffer(bodyLength);
        body.writeZero(bodyLength);

        return new Record(header, body);
    }

    /**
     * Get CRC32 of the given ByteBuf from current reader index to the end.
     * This method will not change the reader index of the given ByteBuf.
     */
    public static int crc32(ByteBuf buf) {
        return crc32(buf, buf.readableBytes());
    }

    /**
     * Get CRC32 of the given ByteBuf from current reader index to the given length.
     * This method will not change the reader index of the given ByteBuf.
     */
    public static int crc32(ByteBuf buf, int length) {
        CRC32 crc32 = new CRC32();
        ByteBuf slice = buf.slice(buf.readerIndex(), length);
        for (ByteBuffer buffer : slice.nioBuffers()) {
            crc32.update(buffer);
        }
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static long recordOffsetToPosition(long offset, long physicalCapacity, long headerSize) {
        long capacity = physicalCapacity - headerSize;
        return offset % capacity + headerSize;
    }

    public static long calculateCycle(long offset, long physicalCapacity, long headerSize) {
        long capacity = physicalCapacity - headerSize;
        return offset / capacity;
    }

    public static long bytesToBlocks(long bytes) {
        return bytes % BLOCK_SIZE == 0 ? bytes / BLOCK_SIZE : bytes / BLOCK_SIZE + 1;
    }

    public static long alignLargeByBlockSize(long offset) {
        return offset % BLOCK_SIZE == 0 ? offset : offset + BLOCK_SIZE - offset % BLOCK_SIZE;
    }

    public static long alignNextBlock(long offset) {
        return offset % BLOCK_SIZE == 0 ? offset + BLOCK_SIZE : offset + BLOCK_SIZE - offset % BLOCK_SIZE;
    }

    public static long alignSmallByBlockSize(long offset) {
        return offset % BLOCK_SIZE == 0 ? offset : offset - offset % BLOCK_SIZE;
    }

    public static boolean isAligned(long offset) {
        return offset % BLOCK_SIZE == 0;
    }

    /**
     * Create a file with the given path and length.
     * Note {@code path} must NOT exist.
     */
    public static void createFile(String path, long length) throws IOException {
        File file = new File(path);
        assert !file.exists();

        File parent = file.getParentFile();
        if (null != parent && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("mkdirs " + parent + " fail");
        }
        if (!file.createNewFile()) {
            throw new IOException("create " + path + " fail");
        }
        if (!file.setReadable(true)) {
            throw new IOException("set " + path + " readable fail");
        }
        if (!file.setWritable(true)) {
            throw new IOException("set " + path + " writable fail");
        }

        // set length
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(length);
        }
    }

    /**
     * Get the capacity of the given block device.
     */
    public static long getBlockDeviceCapacity(String path) throws ExecutionException {
        String[] cmd = new String[] {
            "lsblk",
            "--bytes",
            "--nodeps",
            "--output", "SIZE",
            "--noheadings",
            "--raw",
            path
        };
        CommandResult result = CommandUtils.run(cmd);
        if (!result.success()) {
            throw new ExecutionException("get block device capacity fail: " + result, null);
        }
        return Long.parseLong(result.stdout().trim());
    }

    /**
     * Check if the given path is a block device.
     * If the path does not exist, it returns false
     */
    public static boolean isBlockDevice(String path) {
        if (!new File(path).exists()) {
            return false;
        }

        POSIX posix;
        try {
            posix = POSIXFactory.getNativePOSIX();
        } catch (Exception e) {
            LOGGER.warn("Failed to get native POSIX, fallback to check by prefix", e);
            return isBlockDeviceByPrefix(path);
        }

        try {
            return posix.stat(path).isBlockDev();
        } catch (Exception e) {
            // In some OS (like Windows), the isBlockDev() method may throw an IllegalStateException.
            LOGGER.warn("Failed to check if {} is a block device, fallback to check by prefix", path, e);
            return isBlockDeviceByPrefix(path);
        }
    }

    private static boolean isBlockDeviceByPrefix(String path) {
        return path.startsWith("/dev/");
    }
}
