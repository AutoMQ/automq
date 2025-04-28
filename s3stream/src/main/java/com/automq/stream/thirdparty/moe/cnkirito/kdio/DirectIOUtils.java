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
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.nio.ByteBuffer;

import io.netty.util.internal.PlatformDependent;

public class DirectIOUtils {

    /**
     * Allocate <tt>capacity</tt> bytes of native memory for use as a buffer, and
     * return a {@link ByteBuffer} which gives an interface to this memory. The
     * memory is allocated with
     * {@link DirectIOLib#posix_memalign(PointerByReference, NativeLong, NativeLong) DirectIOLib#posix_memalign()}
     * to ensure that the buffer can be used with <tt>O_DIRECT</tt>.
     * *
     *
     * @param capacity The requested number of bytes to allocate
     * @return A new JnaMemAlignedBuffer of <tt>capacity</tt> bytes aligned in native memory.
     */
    public static ByteBuffer allocateForDirectIO(DirectIOLib lib, int capacity) {
        if (capacity % lib.blockSize() > 0) {
            throw new IllegalArgumentException("Capacity (" + capacity + ") must be a multiple"
                + "of the block size (" + lib.blockSize() + ")");
        }
        NativeLong blockSize = new NativeLong(lib.blockSize());
        PointerByReference pointerToPointer = new PointerByReference();

        // align memory for use with O_DIRECT
        DirectIOLib.posix_memalign(pointerToPointer, blockSize, new NativeLong(capacity));
        return wrapPointer(Pointer.nativeValue(pointerToPointer.getValue()), capacity);
    }

    /**
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    public static ByteBuffer wrapPointer(long ptr, int len) {
        ByteBuffer buf = PlatformDependent.directBuffer(ptr, len);

        assert buf.isDirect();
        return buf;
    }

    public static boolean allocatorAvailable() {
        return PlatformDependent.hasDirectBufferNoCleanerConstructor();
    }

    /**
     * Release the memory of the buffer.
     */
    public static void releaseDirectBuffer(ByteBuffer buffer) {
        assert buffer.isDirect();
        PlatformDependent.freeDirectBuffer(buffer);
    }
}
