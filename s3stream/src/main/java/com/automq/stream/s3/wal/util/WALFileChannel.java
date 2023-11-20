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

import com.automq.stream.s3.wal.WALNotInitializedException;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WALFileChannel implements WALChannel {
    final String filePath;
    final long fileCapacityWant;
    long fileCapacityFact = 0;
    boolean readOnly;
    RandomAccessFile randomAccessFile;
    FileChannel fileChannel;

    public WALFileChannel(String filePath, long fileCapacityWant, boolean readOnly) {
        this.filePath = filePath;
        this.fileCapacityWant = fileCapacityWant;
        this.readOnly = readOnly;
    }

    @Override
    public void open() throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileCapacityFact = randomAccessFile.length();
            if (!readOnly && fileCapacityFact != fileCapacityWant) {
                throw new IOException("file " + filePath + " capacity " + fileCapacityFact + " not equal to requested " + fileCapacityWant);
            }
        } else if (!readOnly) {
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IOException("mkdirs " + file.getParentFile() + " fail");
                }
            }
            if (!file.createNewFile()) {
                throw new IOException("create " + filePath + " fail");
            }
            if (!file.setWritable(true)) {
                throw new IOException("set " + filePath + " writable fail");
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(fileCapacityWant);
            fileCapacityFact = fileCapacityWant;
        } else {
            throw new WALNotInitializedException("read only open uninitialized WAL " + filePath);
        }

        fileChannel = randomAccessFile.getChannel();
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
    public void write(ByteBuf src, long position) throws IOException {
        assert src.readableBytes() + position <= capacity();
        ByteBuffer[] nioBuffers = src.nioBuffers();
        for (ByteBuffer nioBuffer : nioBuffers) {
            int bytesWritten = write(nioBuffer, position);
            position += bytesWritten;
        }
    }

    @Override
    public void flush() throws IOException {
        fileChannel.force(false);
    }

    @Override
    public int read(ByteBuf dst, long position) throws IOException {
        assert dst.writableBytes() + position <= capacity();
        int bytesRead = 0;
        while (dst.isWritable()) {
            int read = dst.writeBytes(fileChannel, position + bytesRead, dst.writableBytes());
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
}
