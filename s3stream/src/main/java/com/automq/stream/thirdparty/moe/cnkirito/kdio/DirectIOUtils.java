/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteBuffer;

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
