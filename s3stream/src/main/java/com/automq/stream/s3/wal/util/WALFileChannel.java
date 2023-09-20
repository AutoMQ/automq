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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WALFileChannel implements WALChannel {
    final String filePath;
    final long fileCapacityWant;
    long fileCapacityFact = 0;
    RandomAccessFile randomAccessFile;
    FileChannel fileChannel;

    public WALFileChannel(String filePath, long fileCapacityWant) {
        this.filePath = filePath;
        this.fileCapacityWant = fileCapacityWant;
    }

    @Override
    public void open() throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileCapacityFact = randomAccessFile.length();
            if (fileCapacityFact != fileCapacityWant) {
                throw new IOException("file " + filePath + " capacity " + fileCapacityFact + " not equal to requested " + fileCapacityWant);
            }
        } else {
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
        }

        fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public void close() {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ignored) {
        }

        try {
            if (fileChannel != null) {
                fileChannel.force(true);
                fileChannel.close();
            }
        } catch (IOException ignored) {
        }
    }

    @Override
    public long capacity() {
        return fileCapacityFact;
    }

    @Override
    public void write(ByteBuffer src, long position) throws IOException {
        int remaining = src.limit();
        int writen = 0;
        do {
            ByteBuffer slice = src.slice().position(writen).limit(remaining);
            int write = fileChannel.write(slice, position + writen);
            if (write == -1) {
                throw new IOException("write -1");
            }
            remaining = remaining - write;
            writen = writen + write;
        } while (remaining > 0);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int bytesRead = 0;
        while (dst.hasRemaining()) {
            int read = fileChannel.read(dst, position + bytesRead);
            if (read == -1) {
                break;
            }
            bytesRead += read;
        }
        return bytesRead;
    }
}
