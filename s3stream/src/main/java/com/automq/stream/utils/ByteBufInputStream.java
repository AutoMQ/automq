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
package com.automq.stream.utils;

import io.netty.buffer.ByteBuf;
import java.io.InputStream;

/**
 * A byte buffer backed input inputStream
 */
public final class ByteBufInputStream extends InputStream {
    private final ByteBuf buffer;

    public ByteBufInputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public int read() {
        if (buffer.readableBytes() == 0) {
            return -1;
        }
        return buffer.readByte() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) {
        if (len == 0) {
            return 0;
        }
        if (buffer.readableBytes() == 0) {
            return -1;
        }

        len = Math.min(len, buffer.readableBytes());
        buffer.readBytes(bytes, off, len);
        return len;
    }
}
