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
import java.nio.ByteBuffer;

public class WALBlockDeviceChannel implements WALChannel {
    // TODO: move these to config
    private static final int PREALLOCATED_BYTE_BUFFER_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.preallocatedByteBufferSize",
            String.valueOf(1024 * 1024 * 2)
    ));
    private static final int PREALLOCATED_BYTE_BUFFER_MAX_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.preallocatedByteBufferMaxSize",
            String.valueOf(1024 * 1024 * 16)
    ));

    final String blockDevicePath;
    final long capacityWant;
    final DirectIOLib directIOLib;

    long capacityFact = 0;

    DirectRandomAccessFile randomAccessFile;

    ThreadLocal<ByteBuffer> threadLocalByteBuffer = new ThreadLocal<>() {
        @Override
        protected ByteBuffer initialValue() {
            return DirectIOUtils.allocateForDirectIO(directIOLib, PREALLOCATED_BYTE_BUFFER_SIZE);
        }
    };

    public WALBlockDeviceChannel(String blockDevicePath, long blockDeviceCapacityWant) {
        this.blockDevicePath = blockDevicePath;
        this.capacityWant = blockDeviceCapacityWant;
        DirectIOLib lib = DirectIOLib.getLibForPath(blockDevicePath);
        if (null == lib || !DirectIOLib.binit) {
            throw new RuntimeException("O_DIRECT not supported");
        } else {
            this.directIOLib = lib;
        }
    }

    @Override
    public void open() throws IOException {
        randomAccessFile = new DirectRandomAccessFile(new File(blockDevicePath), "rw");
        capacityFact = randomAccessFile.length();
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

    private void makeThreadLocalBytebufferMatchDirectIO(int inputBufferDirectIOAlignedSize) {
        ByteBuffer byteBufferWrite = threadLocalByteBuffer.get();
        if (inputBufferDirectIOAlignedSize > byteBufferWrite.capacity()) {
            if (inputBufferDirectIOAlignedSize <= PREALLOCATED_BYTE_BUFFER_MAX_SIZE) {
                threadLocalByteBuffer.set(ByteBuffer.allocateDirect(inputBufferDirectIOAlignedSize));
            } else {
                throw new RuntimeException("too large write size");
            }
        }
    }

    @Override
    public void write(ByteBuf buf, long position) throws IOException {
        ByteBuffer src = buf.nioBuffer();
        int bufferDirectIOAlignedSize = (int) WALUtil.alignLargeByBlockSize(src.limit());

        makeThreadLocalBytebufferMatchDirectIO(bufferDirectIOAlignedSize);


        ByteBuffer byteBufferWrite = threadLocalByteBuffer.get();
        byteBufferWrite.position(0).put(src);
        byteBufferWrite.position(0).limit(bufferDirectIOAlignedSize);

        int remaining = byteBufferWrite.limit();
        int written = 0;
        do {
            ByteBuffer slice = byteBufferWrite.slice().position(written).limit(remaining);
            // FIXME: make sure the position is aligned
            int write = randomAccessFile.write(slice, position + written);
            if (write == -1) {
                throw new IOException("write -1");
            } else if (write % WALUtil.BLOCK_SIZE != 0) {
                // Should not happen. If it happens, it means that the system does not support direct IO
                write -= write % WALUtil.BLOCK_SIZE;
            }
            written += write;
            remaining -= write;
        } while (remaining > 0);
    }

    @Override
    public void flush() {
        // TODO
    }

    @Override
    public int read(ByteBuf buf, long position) throws IOException {
        ByteBuffer dst = buf.nioBuffer();
        // FIXME: a small dst will lead to a zero size read
        int bufferDirectIOAlignedSize = (int) WALUtil.alignSmallByBlockSize(dst.capacity());

        makeThreadLocalBytebufferMatchDirectIO(bufferDirectIOAlignedSize);

        ByteBuffer tlDirectBuffer = threadLocalByteBuffer.get();
        tlDirectBuffer.position(0).limit(bufferDirectIOAlignedSize);

        int bytesRead = 0;
        while (tlDirectBuffer.hasRemaining()) {
            // FIXME: make sure the position is aligned
            int read = randomAccessFile.read(tlDirectBuffer, position + bytesRead);
            if (read == -1) {
                break;
            }
            bytesRead += read;
        }

        tlDirectBuffer.position(0).limit(bytesRead);
        // FIXME: newPosition is wrong
        dst.position(0).put(tlDirectBuffer);
        dst.position(0).limit(bytesRead);
        return bytesRead;
    }
}
