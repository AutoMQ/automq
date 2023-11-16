/**
 * Copyright 2019 xujingfeng (kirito.moe@foxmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;

public interface DirectChannel extends Channel {
    /**
     * Writes from the <tt>src</tt> buffer into this channel at <tt>position</tt>.
     *
     * @param src
     *        The {@link ByteBuffer} to write from
     *
     * @param position
     *        The position within the file at which to start writing
     *
     * @return How many bytes were written from <tt>src</tt> into the file
     * @throws IOException
     */
    int write(ByteBuffer src, long position) throws IOException;

    /**
     * Reads from this channel into the <tt>dst</tt> buffer from <tt>position</tt>.
     *
     * @param dst
     *        The {@link ByteBuffer} to read into
     *
     * @param position
     *        The position within the file at which to start reading
     *
     * @return How many bytes were placed into <tt>dst</tt>
     * @throws IOException
     */
    int read(ByteBuffer dst, long position) throws IOException;

    /**
     * @return The file size for this channel
     */
    long size();

    /**
     * @return <tt>true</tt> if this channel is read only, <tt>false</tt> otherwise
     */
    boolean isReadOnly();

    /**
     * Truncates this file's length to <tt>fileLength</tt>.
     *
     * @param fileLength The length to which to truncate
     *
     * @return This UnsafeByteAlignedChannel
     *
     * @throws IOException
     */
    DirectChannel truncate(long fileLength) throws IOException;

    /**
     * @return The file descriptor for this channel
     */
    int getFD();
}
