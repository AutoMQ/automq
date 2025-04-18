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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;

public class WALFileChannel extends AbstractWALChannel {
    final String filePath;
    final long fileCapacityWant;
    /**
     * When set to true, the file should exist and the file size does not need to be verified.
     */
    final boolean recoveryMode;
    long fileCapacityFact = 0;
    RandomAccessFile randomAccessFile;
    FileChannel fileChannel;

    public WALFileChannel(String filePath, long fileCapacityWant, boolean recoveryMode) {
        this.filePath = filePath;
        this.recoveryMode = recoveryMode;
        if (recoveryMode) {
            this.fileCapacityWant = CAPACITY_NOT_SET;
        } else {
            assert fileCapacityWant > 0;
            this.fileCapacityWant = fileCapacityWant;
        }
    }

    @Override
    public void open(CapacityReader reader) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IOException(filePath + " is not a file");
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileCapacityFact = randomAccessFile.length();
            if (!recoveryMode && fileCapacityFact != fileCapacityWant) {
                // the file exists but not the same size as requested
                throw new WALCapacityMismatchException(filePath, fileCapacityWant, fileCapacityFact);
            }
        } else {
            // the file does not exist
            if (recoveryMode) {
                throw new WALNotInitializedException("try to open an uninitialized WAL in recovery mode: file not exists: " + filePath);
            }
            WALUtil.createFile(filePath, fileCapacityWant);
            randomAccessFile = new RandomAccessFile(filePath, "rw");
            fileCapacityFact = fileCapacityWant;
        }

        fileChannel = randomAccessFile.getChannel();

        checkCapacity(reader);
    }

    private void checkCapacity(CapacityReader reader) throws IOException {
        if (null == reader) {
            return;
        }
        Long capacity = reader.capacity(this);
        if (null == capacity) {
            if (recoveryMode) {
                throw new WALNotInitializedException("try to open an uninitialized WAL in recovery mode: empty header. path: " + filePath);
            }
        } else if (fileCapacityFact != capacity) {
            throw new WALCapacityMismatchException(filePath, fileCapacityFact, capacity);
        }
        assert fileCapacityFact != CAPACITY_NOT_SET;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public long capacity() {
        return fileCapacityFact;
    }

    @Override
    public String path() {
        return filePath;
    }

    @Override
    public void doWrite(ByteBuf src, long position) throws IOException {
        assert src.readableBytes() + position <= capacity();
        ByteBuffer[] nioBuffers = src.nioBuffers();
        for (ByteBuffer nioBuffer : nioBuffers) {
            int bytesWritten = write(nioBuffer, position);
            position += bytesWritten;
        }
    }

    @Override
    public void doFlush() throws IOException {
        fileChannel.force(false);
    }

    @Override
    public int doRead(ByteBuf dst, long position, int length) throws IOException {
        length = Math.min(length, dst.writableBytes());
        assert position + length <= capacity();
        int bytesRead = 0;
        while (dst.isWritable()) {
            int read = dst.writeBytes(fileChannel, position + bytesRead, length);
            if (read == -1) {
                // EOF
                break;
            }
            bytesRead += read;
        }
        return bytesRead;
    }

    private int write(ByteBuffer src, long position) throws IOException {
        int bytesWritten = 0;
        while (src.hasRemaining()) {
            int written = fileChannel.write(src, position + bytesWritten);
            if (written == -1) {
                throw new IOException("write -1");
            }
            bytesWritten += written;
        }
        return bytesWritten;
    }

    @Override
    public boolean useDirectIO() {
        return false;
    }
}
