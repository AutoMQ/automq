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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;

public class DirectChannelImpl implements DirectChannel {
    private final DirectIOLib lib;
    private final int fd;
    private boolean isOpen;
    private long fileLength;
    private final boolean isReadOnly;

    private DirectChannelImpl(DirectIOLib lib, int fd, long fileLength, boolean readOnly) {
        this.lib = lib;
        this.fd = fd;
        this.isOpen = true;
        this.isReadOnly = readOnly;
        this.fileLength = fileLength;
    }

    public static DirectChannel getChannel(File file, boolean readOnly) throws IOException {
        DirectIOLib lib = DirectIOLib.getLibForPath(file.toString());
        if (null == lib) {
            throw new IOException("No DirectIOLib found for path " + file);
        }
        return getChannel(lib, file, readOnly);
    }

    public static DirectChannel getChannel(DirectIOLib lib, File file, boolean readOnly) throws IOException {
        int fd = lib.oDirectOpen(file.toString(), readOnly);
        long length = file.length();
        return new DirectChannelImpl(lib, fd, length, readOnly);
    }

    private void ensureOpen() throws ClosedChannelException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
    }

    private void ensureWritable() {
        if (isReadOnly()) {
            throw new NonWritableChannelException();
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        ensureOpen();
        return lib.pread(fd, dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        ensureOpen();
        ensureWritable();
        assert src.position() == lib.blockStart(src.position());

        int written = lib.pwrite(fd, src, position);

        // update file length if we wrote past it
        fileLength = Math.max(position + written, fileLength);
        return written;
    }

    @Override
    public DirectChannel truncate(final long length) throws IOException {
        ensureOpen();
        ensureWritable();
        if (DirectIOLib.ftruncate(fd, length) < 0) {
            throw new IOException("Error during truncate on descriptor " + fd + ": " +
                    DirectIOLib.getLastError());
        }
        fileLength = length;
        return this;
    }

    @Override
    public long size() {
        return fileLength;
    }

    @Override
    public int getFD() {
        return fd;
    }


    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public void close() throws IOException {
        if (!isOpen()) {
            return;
        }
        isOpen = false;
        if (lib.close(fd) < 0) {
            throw new IOException("Error closing file with descriptor " + fd + ": " +
                    DirectIOLib.getLastError());
        }
    }
}
