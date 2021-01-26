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
package org.apache.kafka.common.record;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a memory record set which is not necessarily offset-aligned
 */
public class UnalignedMemoryRecords implements UnalignedRecords {

    private final ByteBuffer buffer;

    public UnalignedMemoryRecords(ByteBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public int sizeInBytes() {
        return buffer.remaining();
    }

    @Override
    public long writeTo(TransferableChannel channel, long position, int length) throws IOException {
        if (position > Integer.MAX_VALUE)
            throw new IllegalArgumentException("position should not be greater than Integer.MAX_VALUE: " + position);
        if (position + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());
        return Utils.tryWriteTo(channel, (int) position, length, buffer);
    }

}
