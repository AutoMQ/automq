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

package com.automq.stream.s3.wal.impl;

import com.automq.stream.s3.wal.RecordOffset;

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultRecordOffset implements RecordOffset {
    private static final byte MAGIC = (byte) 0xA8;
    private final long epoch;
    private final long offset;
    private final int size;

    private DefaultRecordOffset(long epoch, long offset, int size) {
        this.epoch = epoch;
        this.offset = offset;
        this.size = size;
    }

    public static DefaultRecordOffset of(long epoch, long recordOffset, int recordSize) {
        return new DefaultRecordOffset(epoch, recordOffset, recordSize);
    }

    public static DefaultRecordOffset of(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }
        return new DefaultRecordOffset(buf.readLong(), buf.readLong(), buf.readInt());
    }

    public static DefaultRecordOffset of(RecordOffset recordOffset) {
        if (recordOffset instanceof DefaultRecordOffset) {
            return (DefaultRecordOffset) recordOffset;
        }
        return of(recordOffset.buffer());
    }

    public long epoch() {
        return epoch;
    }

    public long offset() {
        return offset;
    }

    public int size() {
        return size;
    }

    @Override
    public ByteBuf buffer() {
        ByteBuf buffer = Unpooled.buffer(1 + 8 + 4);
        buffer.writeByte(MAGIC);
        buffer.writeLong(epoch);
        buffer.writeLong(this.offset);
        buffer.writeInt(this.size);
        return buffer;
    }

    @Override
    public String toString() {
        return "DefaultRecordOffset{" +
            "epoch=" + epoch +
            ", offset=" + offset +
            ", size=" + size +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        DefaultRecordOffset offset1 = (DefaultRecordOffset) o;
        return epoch == offset1.epoch && offset == offset1.offset && size == offset1.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, offset, size);
    }
}
