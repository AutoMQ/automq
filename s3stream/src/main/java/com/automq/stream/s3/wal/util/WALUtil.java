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

package com.automq.stream.s3.wal.util;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class WALUtil {
    public static final int BLOCK_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.blocksize",
            "4096"
    ));

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

    public static long recordOffsetToPosition(long offset, long recordSectionCapacity, long headerSize) {
        return offset % recordSectionCapacity + headerSize;
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
}
