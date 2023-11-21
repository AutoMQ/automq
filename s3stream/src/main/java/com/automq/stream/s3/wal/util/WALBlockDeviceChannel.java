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

import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectIOLib;
import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectIOUtils;
import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectRandomAccessFile;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class WALBlockDeviceChannel implements WALChannel {
    // TODO: move these to config
    @Deprecated
    private static final int PRE_ALLOCATED_BYTE_BUFFER_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.preAllocatedByteBufferSize",
            String.valueOf(1024 * 1024 * 2)
    ));
    @Deprecated
    private static final int PRE_ALLOCATED_BYTE_BUFFER_MAX_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.preAllocatedByteBufferMaxSize",
            String.valueOf(1024 * 1024 * 16)
    ));

    final String blockDevicePath;
    final long capacity;
    final DirectIOLib directIOLib;
    /**
     * 0 means allocate on demand
     */
    final int initTempBufferSize;
    /**
     * 0 means no limit
     */
    final int maxTempBufferSize;

    DirectRandomAccessFile randomAccessFile;

    ThreadLocal<ByteBuffer> threadLocalByteBuffer = new ThreadLocal<>() {
        @Override
        protected ByteBuffer initialValue() {
            return DirectIOUtils.allocateForDirectIO(directIOLib, initTempBufferSize);
        }
    };

    public WALBlockDeviceChannel(String blockDevicePath, long blockDeviceCapacityWant) {
        this(blockDevicePath, blockDeviceCapacityWant, 0, 0);
    }

    public WALBlockDeviceChannel(String blockDevicePath, long blockDeviceCapacityWant, int initTempBufferSize, int maxTempBufferSize) {
        this.blockDevicePath = blockDevicePath;
        // We cannot get the actual capacity of the block device here, so we just use the capacity we want
        // And it's the caller's responsibility to make sure the capacity is right
        // FIXME: in recovery mode, `capacity` will be set to CAPACITY_NOT_SET here. It should be corrected after recovery.
        this.capacity = blockDeviceCapacityWant;
        if (!WALUtil.isAligned(blockDeviceCapacityWant)) {
            throw new RuntimeException("wal capacity must be aligned by block size when using block device");
        }
        this.initTempBufferSize = initTempBufferSize;
        this.maxTempBufferSize = maxTempBufferSize;

        DirectIOLib lib = DirectIOLib.getLibForPath(blockDevicePath);
        if (null == lib || !DirectIOLib.binit) {
            throw new RuntimeException("O_DIRECT not supported");
        } else {
            this.directIOLib = lib;
        }
    }

    @Override
    public void open() throws IOException {
        if (!blockDevicePath.startsWith(WALChannelBuilder.DEVICE_PREFIX)) {
            // If the block device path is not a device, we create a file with the capacity we want
            // This is ONLY for test purpose, so we don't check the capacity of the file
            try (RandomAccessFile raf = new RandomAccessFile(blockDevicePath, "rw")) {
                raf.setLength(capacity);
            }
        }

        randomAccessFile = new DirectRandomAccessFile(new File(blockDevicePath), "rw");
    }

    @Override
    public void close() {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ignored) {
        }
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public String path() {
        return blockDevicePath;
    }

    private ByteBuffer getBuffer(int alignedSize) {
        assert WALUtil.isAligned(alignedSize);

        ByteBuffer currentBuf = threadLocalByteBuffer.get();
        if (alignedSize <= currentBuf.capacity()) {
            return currentBuf;
        }
        if (maxTempBufferSize > 0 && alignedSize > maxTempBufferSize) {
            throw new RuntimeException("too large write size");
        }

        ByteBuffer newBuf = DirectIOUtils.allocateForDirectIO(directIOLib, alignedSize);
        threadLocalByteBuffer.set(newBuf);
        DirectIOUtils.releaseDirectBuffer(currentBuf);
        return newBuf;
    }

    @Override
    public void write(ByteBuf src, long position) throws IOException {
        assert WALUtil.isAligned(position);

        int alignedSize = (int) WALUtil.alignLargeByBlockSize(src.readableBytes());
        assert position + alignedSize <= capacity();
        ByteBuffer tmpBuf = getBuffer(alignedSize);
        tmpBuf.clear();

        for (ByteBuffer buffer : src.nioBuffers()) {
            tmpBuf.put(buffer);
        }
        tmpBuf.position(0).limit(alignedSize);

        write(tmpBuf, position);
    }

    private int write(ByteBuffer src, long position) throws IOException {
        assert WALUtil.isAligned(src.remaining());

        int bytesWritten = 0;
        while (src.hasRemaining()) {
            int written = randomAccessFile.write(src, position + bytesWritten);
            // kdio will throw an exception rather than return -1, so we don't need to check for -1
            bytesWritten += written;
        }
        return bytesWritten;
    }

    @Override
    public void flush() {
    }

    @Override
    public int read(ByteBuf dst, long position) throws IOException {
        long start = position;
        long end = position + dst.writableBytes();
        long alignedStart = WALUtil.alignSmallByBlockSize(start);
        long alignedEnd = WALUtil.alignLargeByBlockSize(end);
        int alignedSize = (int) (alignedEnd - alignedStart);
        assert alignedEnd <= capacity();

        ByteBuffer tmpBuf = getBuffer(alignedSize);
        tmpBuf.position(0).limit(alignedSize);

        read(tmpBuf, alignedStart);
        tmpBuf.position((int) (start - alignedStart)).limit((int) (end - alignedStart));

        dst.writeBytes(tmpBuf);
        return (int) (end - start);
    }

    private int read(ByteBuffer dst, long position) throws IOException {
        int bytesRead = 0;
        while (dst.hasRemaining()) {
            int read = randomAccessFile.read(dst, position + bytesRead);
            // kdio will throw an exception rather than return -1, so we don't need to check for -1
            bytesRead += read;
        }
        return bytesRead;
    }
}
