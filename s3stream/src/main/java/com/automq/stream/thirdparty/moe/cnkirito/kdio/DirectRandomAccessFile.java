/**
 * Copyright (C) 2014 Stephen Macke (smacke@cs.stanford.edu)
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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Class to emulate the behavior of {@link RandomAccessFile}, but using direct I/O.
 *
 */
public class DirectRandomAccessFile implements Closeable {

    private final DirectChannel channel;


    /**
     * @param file The file to open
     *
     * @param mode Either "rw" or "r", depending on whether this file is read only
     *
     * @throws IOException
     */
    public DirectRandomAccessFile(File file, String mode)
            throws IOException {

        boolean readOnly = false;
        if ("r".equals(mode)) {
            readOnly = true;
        } else if (!"rw".equals(mode)) {
            throw new IllegalArgumentException("only r and rw modes supported");
        }

        if (readOnly && !file.isFile()) {
            throw new FileNotFoundException("couldn't find file " + file);
        }

        this.channel = DirectChannelImpl.getChannel(file, readOnly);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }


    public int write(ByteBuffer src, long position) throws IOException {
        return channel.write(src, position);
    }

    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    /**
     * @return The current position in the file
     */
    public long getFilePointer() {
        return channel.getFD();
    }

    /**
     * @return The current length of the file
     */
    public long length() {
        return channel.size();
    }

}
