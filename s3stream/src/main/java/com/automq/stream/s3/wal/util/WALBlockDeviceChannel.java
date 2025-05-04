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

import com.automq.stream.s3.wal.exception.WALCapacityMismatchException;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;
import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectIOLib;
import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectIOUtils;
import com.automq.stream.thirdparty.moe.cnkirito.kdio.DirectRandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;
import static com.automq.stream.s3.wal.util.WALUtil.isBlockDevice;

public class WALBlockDeviceChannel extends AbstractWALChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALBlockDeviceChannel.class);
    private static final String CHECK_DIRECT_IO_AVAILABLE_FORMAT = "%s.check_direct_io_available";
    final String path;
    final long capacityWant;
    final boolean recoveryMode;
    final DirectIOLib directIOLib;
    /**
     * 0 means allocate on demand
     */
    final int initTempBufferSize;
    /**
     * 0 means no limit
     */
    final int maxTempBufferSize;
    /**
     * Flag indicating whether unaligned write is allowed.
     * Currently, it is only allowed when testing.
     */
    public boolean unalignedWrite = false;

    long capacityFact = 0;
    DirectRandomAccessFile randomAccessFile;

    FastThreadLocal<ByteBuffer> threadLocalByteBuffer = new FastThreadLocal<>() {
        @Override
        protected ByteBuffer initialValue() {
            return DirectIOUtils.allocateForDirectIO(directIOLib, initTempBufferSize);
        }
    };

    public WALBlockDeviceChannel(String path, long capacityWant) {
        this(path, capacityWant, 0, 0, false);
    }

    public WALBlockDeviceChannel(String path, long capacityWant, int initTempBufferSize, int maxTempBufferSize,
        boolean recoveryMode) {
        this.path = path;
        this.recoveryMode = recoveryMode;
        if (recoveryMode) {
            this.capacityWant = CAPACITY_NOT_SET;
        } else {
            assert capacityWant > 0;
            this.capacityWant = capacityWant;
            if (!WALUtil.isAligned(capacityWant)) {
                throw new RuntimeException("wal capacity must be aligned by block size when using block device");
            }
        }
        this.initTempBufferSize = initTempBufferSize;
        this.maxTempBufferSize = maxTempBufferSize;

        DirectIOLib lib = DirectIOLib.getLibForPath(path);
        if (null == lib) {
            throw new RuntimeException("O_DIRECT not supported");
        }
        int blockSize = lib.blockSize();
        if (WALUtil.BLOCK_SIZE % blockSize != 0) {
            throw new RuntimeException(String.format("block size %d is not a multiple of %d, update it by jvm option: -D%s=%d",
                WALUtil.BLOCK_SIZE, blockSize, WALUtil.BLOCK_SIZE_PROPERTY, blockSize));
        }
        this.directIOLib = lib;
    }

    /**
     * Check whether the {@link WALBlockDeviceChannel} is available for the given path.
     *
     * @return null if available, otherwise the reason why it's not available
     */
    public static String checkAvailable(String path) {
        if (!DirectIOLib.binit) {
            return "O_DIRECT not supported";
        }
        if (!DirectIOUtils.allocatorAvailable()) {
            return "java.nio.DirectByteBuffer.<init>(long, int) not available." +
                " Add --add-opens=java.base/java.nio=ALL-UNNAMED and -Dio.netty.tryReflectionSetAccessible=true to JVM options may fix this.";
        }
        if (!isBlockDevice(path)) {
            String reason = tryOpenFileWithDirectIO(String.format(CHECK_DIRECT_IO_AVAILABLE_FORMAT, path));
            if (null != reason) {
                return "O_DIRECT not supported by the file system, path: " + path + ", reason: " + reason;
            }
        }
        return null;
    }

    /**
     * Try to open a file with O_DIRECT flag to check whether the file system supports O_DIRECT.
     * The file will be deleted after the test.
     *
     * @return null if the file is opened successfully, otherwise the reason why it's not available
     */
    private static String tryOpenFileWithDirectIO(String path) {
        File file = new File(path);
        try {
            DirectRandomAccessFile randomAccessFile = new DirectRandomAccessFile(file, "rw");
            randomAccessFile.close();
            return null;
        } catch (IOException e) {
            return e.getMessage();
        } finally {
            // the file may be created in {@link DirectRandomAccessFile(File, String)}, so delete it
            file.delete();
        }
    }

    @Override
    public void open(CapacityReader reader) throws IOException {
        if (!isBlockDevice(path)) {
            openAndCheckFile();
        } else {
            try {
                long capacity = WALUtil.getBlockDeviceCapacity(path);
                if (!recoveryMode && capacityWant > capacity) {
                    // the real capacity of the block device is smaller than requested
                    throw new WALCapacityMismatchException(path, capacityWant, capacity);
                }
            } catch (ExecutionException e) {
                LOGGER.warn("failed to get the real capacity of the block device {}, just skip checking", path, e);
            }
            // We could not get the real capacity of the WAL in block device, so we just use the `capacityWant` as the capacity here
            // It will be checked and updated in `checkCapacity` later
            capacityFact = capacityWant;
        }

        randomAccessFile = new DirectRandomAccessFile(new File(path), "rw");

        checkCapacity(reader);
    }

    /**
     * Create the file and set length if not exists, and check the file size if exists.
     */
    private void openAndCheckFile() throws IOException {
        File file = new File(path);
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IOException(path + " is not a file");
            }
            capacityFact = file.length();
            if (!recoveryMode && capacityFact != capacityWant) {
                // the file exists but not the same size as requested
                throw new WALCapacityMismatchException(path, capacityWant, capacityFact);
            }
        } else {
            // the file does not exist
            if (recoveryMode) {
                throw new WALNotInitializedException("try to open an uninitialized WAL in recovery mode: file not exists. path: " + path);
            }
            WALUtil.createFile(path, capacityWant);
            capacityFact = capacityWant;
        }
    }

    private void checkCapacity(CapacityReader reader) throws IOException {
        if (null == reader) {
            return;
        }
        Long capacity = reader.capacity(this);
        if (null == capacity) {
            if (recoveryMode) {
                throw new WALNotInitializedException("try to open an uninitialized WAL in recovery mode: empty header. path: " + path);
            }
        } else if (capacityFact == CAPACITY_NOT_SET) {
            // recovery mode on block device
            capacityFact = capacity;
        } else if (capacityFact != capacity) {
            throw new WALCapacityMismatchException(path, capacityFact, capacity);
        }
        assert capacityFact != CAPACITY_NOT_SET;
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
        return capacityFact;
    }

    @Override
    public String path() {
        return path;
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
    public void doWrite(ByteBuf src, long position) throws IOException {
        if (unalignedWrite) {
            // unaligned write, just used for testing
            unalignedWrite(src, position);
            return;
        }
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

    private void unalignedWrite(ByteBuf src, long position) throws IOException {
        long start = position;
        long end = position + src.readableBytes();
        long alignedStart = WALUtil.alignSmallByBlockSize(start);
        long alignedEnd = WALUtil.alignLargeByBlockSize(end);
        int alignedSize = (int) (alignedEnd - alignedStart);

        // read the data in the range [alignedStart, alignedEnd) to tmpBuf
        ByteBuffer tmpBuf = getBuffer(alignedSize);
        tmpBuf.position(0).limit(alignedSize);
        read(tmpBuf, alignedStart);

        // overwrite the data in the range [start, end) in tmpBuf
        for (ByteBuffer buffer : src.nioBuffers()) {
            tmpBuf.position((int) (start - alignedStart));
            start += buffer.remaining();
            tmpBuf.put(buffer);
        }
        tmpBuf.position(0).limit(alignedSize);

        // write it
        write(tmpBuf, alignedStart);
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
    public void doFlush() {
    }

    @Override
    public int doRead(ByteBuf dst, long position, int length) throws IOException {
        long start = position;
        length = Math.min(length, dst.writableBytes());
        long end = position + length;
        long alignedStart = WALUtil.alignSmallByBlockSize(start);
        long alignedEnd = WALUtil.alignLargeByBlockSize(end);
        int alignedSize = (int) (alignedEnd - alignedStart);
        // capacity may be CAPACITY_NOT_SET only when we call {@link CapacityReader#capacity} in recovery mode
        assert CAPACITY_NOT_SET == capacity() || alignedEnd <= capacity();

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

    @Override
    public boolean useDirectIO() {
        return true;
    }
}
