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

package org.apache.kafka.common.protocol;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    void readArray(byte[] arr);

    /**
     * Read a Kafka-delimited string from a byte buffer.  The UTF-8 string
     * length is stored in a two-byte short.  If the length is negative, the
     * string is null.
     */
    default String readNullableString() {
        int length = readShort();
        if (length < 0) {
            return null;
        }
        byte[] arr = new byte[length];
        readArray(arr);
        return new String(arr, StandardCharsets.UTF_8);
    }

    /**
     * Read a Kafka-delimited array from a byte buffer.  The array length is
     * stored in a four-byte short.
     */
    default byte[] readNullableBytes() {
        int length = readInt();
        if (length < 0) {
            return null;
        }
        byte[] arr = new byte[length];
        readArray(arr);
        return arr;
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    default UUID readUUID() {
        return new UUID(readLong(), readLong());
    }
}
