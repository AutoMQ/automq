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

package kafka.log.es;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

public class MetaKeyValue {
    public static final byte MAGIC_V0 = 0;

    private final short key;
    private final ByteBuffer value;

    private MetaKeyValue(short key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public static MetaKeyValue of(short key, ByteBuffer value) {
        return new MetaKeyValue(key, value);
    }

    public static MetaKeyValue decode(ByteBuffer buf) throws IllegalArgumentException {
        // version, version = 0
        byte magic = buf.get();
        if (magic != MAGIC_V0) {
            throw new IllegalArgumentException("unsupported magic: " + magic);
        }
        // key, short
        short key = buf.getShort();
        // value
        ByteBuffer value = buf.slice();
        return MetaKeyValue.of(key, value);
    }

    public static ByteBuffer encode(MetaKeyValue kv) {
        // MetaKeyValue encoded format =>
        //  magic => 1 byte
        //  key => 2 bytes
        //  value => bytes
        int length = 1 // magic length
                + 2 // key length
                + kv.value.remaining(); // value length
        ByteBuf buf = Unpooled.buffer(length);
        buf.writeByte(MAGIC_V0);
        buf.writeShort(kv.key);
        buf.writeBytes(kv.value);
        return buf.nioBuffer();
    }

    public short getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value.duplicate();
    }
}
