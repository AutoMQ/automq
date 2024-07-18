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

package kafka.log.streamaspect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MetaKeyValue {
    public static final byte MAGIC_V0 = 0;

    private final String key;
    private final ByteBuffer value;

    private MetaKeyValue(String key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public static MetaKeyValue of(String key, ByteBuffer value) {
        return new MetaKeyValue(key, value);
    }

    public static MetaKeyValue decode(ByteBuffer buf) throws IllegalArgumentException {
        // version, version = 0
        byte magic = buf.get();
        if (magic != MAGIC_V0) {
            throw new IllegalArgumentException("unsupported magic: " + magic);
        }
        // key, short
        int keyLength = buf.getInt();
        byte[] keyBytes = new byte[keyLength];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        // value
        ByteBuffer value = buf.duplicate();
        return MetaKeyValue.of(key, value);
    }

    public static ByteBuffer encode(MetaKeyValue kv) {
        byte[] keyBytes = kv.key.getBytes(StandardCharsets.UTF_8);
        // MetaKeyValue encoded format =>
        //  magic => 1 byte
        // keyLength => 4 bytes
        //  value => bytes
        int length = 1 // magic length
                + 4 // key length
                + keyBytes.length // key payload
                + kv.value.remaining(); // value payload
        ByteBuf buf = Unpooled.buffer(length);
        buf.writeByte(MAGIC_V0);
        buf.writeInt(keyBytes.length);
        buf.writeBytes(keyBytes);
        buf.writeBytes(kv.value.duplicate());
        return buf.nioBuffer();
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value.duplicate();
    }
}
