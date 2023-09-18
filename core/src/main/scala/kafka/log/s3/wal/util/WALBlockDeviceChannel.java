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

package kafka.log.s3.wal.util;

import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class WALBlockDeviceChannel implements WALChannel {
    private static final int BLOCK_SIZE = Integer.parseInt(System.getProperty(
            "automq.ebswal.blocksize",
            "4096"
    ));
    private static final DirectIOLib DIRECT_IO_LIB = DirectIOLib.getLibForPath("/");
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

    long capacityFact = 0;

    DirectRandomAccessFile randomAccessFile;

    ThreadLocal<ByteBuffer> threadLocalByteBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(PREALLOCATED_BYTE_BUFFER_SIZE);
        }
    };

    public WALBlockDeviceChannel(String blockDevicePath, long blockDeviceCapacityWant) {
        this.blockDevicePath = blockDevicePath;
        this.capacityWant = blockDeviceCapacityWant;
    }

    @Override
    public void open() throws IOException {
        if (DirectIOLib.binit) {
            randomAccessFile = new DirectRandomAccessFile(new File(blockDevicePath), "rw");
            capacityFact = randomAccessFile.length();
        } else {
            throw new RuntimeException("your system do not support direct io");
        }
    }

    @Override
    public void close() {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (Throwable ignored) {
        }
    }

    @Override
    public long capacity() {
        return capacityFact;
    }

    private void makeThreadLocalBytebufferMatchDirectIO(int inputBufferDirectIOAlignedSize) {
        ByteBuffer byteBufferWrite = threadLocalByteBuffer.get();
        if (inputBufferDirectIOAlignedSize > byteBufferWrite.capacity()) {
            if (inputBufferDirectIOAlignedSize <= PREALLOCATED_BYTE_BUFFER_MAX_SIZE)
                threadLocalByteBuffer.set(ByteBuffer.allocateDirect(inputBufferDirectIOAlignedSize));
            else throw new RuntimeException("too large write size");
        }
    }

    /**
     * @param src
     * @param position
     * @throws IOException
     */
    @Override
    public void write(ByteBuffer src, long position) throws IOException {
        int bufferDirectIOAlignedSize = (int) WALUtil.alignLargeByBlockSize(src.limit());

        makeThreadLocalBytebufferMatchDirectIO(bufferDirectIOAlignedSize);


        ByteBuffer byteBufferWrite = threadLocalByteBuffer.get();
        byteBufferWrite.position(0).put(src);
        byteBufferWrite.position(0).limit(bufferDirectIOAlignedSize);

        int remaining = byteBufferWrite.limit();
        int writen = 0;
        do {
            ByteBuffer slice = byteBufferWrite.slice().position(writen).limit(remaining);
            int write = randomAccessFile.write(slice, position + writen);
            if (write == -1) {
                throw new IOException("write -1");
            } else if (write % BLOCK_SIZE != 0) {
                // 理论上不会发生。如果发生，说明系统不支持direct io
                write -= write % BLOCK_SIZE;
            }
            writen += write;
            remaining -= write;
        } while (remaining > 0);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int bufferDirectIOAlignedSize = (int) WALUtil.alignSmallByBlockSize(dst.capacity());

        makeThreadLocalBytebufferMatchDirectIO(bufferDirectIOAlignedSize);

        ByteBuffer tlDirectBuffer = threadLocalByteBuffer.get();
        tlDirectBuffer.position(0).limit(bufferDirectIOAlignedSize);
        int count = randomAccessFile.read(tlDirectBuffer, position);
        if (count == -1) {
            throw new IOException("read -1");
        }

        tlDirectBuffer.position(0).limit(count);
        dst.position(0).put(tlDirectBuffer);
        dst.position(0).limit(count);
        return count;
    }
}
