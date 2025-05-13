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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.exception.UnmarshalException;

import io.netty.buffer.ByteBuf;
import java.util.Map;

public class WALObjectHeader {
    // TODO: find usage and fix
    public static final int WAL_HEADER_MAGIC_CODE_V0 = 0x12345678;
    // TODO: find usage and fix
    public static final int WAL_HEADER_SIZE_V0 = 4 // magic code
                                              + 8 // start offset
                                              + 8 // body length
                                              + 8 // sticky record length
                                              + 4 // node id
                                              + 8; // node epoch
    public static final int WAL_HEADER_MAGIC_CODE_V1 = 0xEDCBA987;
    public static final int WAL_HEADER_SIZE_V1 = WAL_HEADER_SIZE_V0
                                              + 8; // trim offset
    private static final Map<Integer, Integer> WAL_HEADER_SIZES = Map.of(
            WAL_HEADER_MAGIC_CODE_V0, WAL_HEADER_SIZE_V0,
            WAL_HEADER_MAGIC_CODE_V1, WAL_HEADER_SIZE_V1
    );

    private final int magicCode0;
    private final long startOffset1;
    private final long length2;
    private final long stickyRecordLength3;
    private final int nodeId4;
    private final long epoch5;
    private final long trimOffset6;

    public WALObjectHeader(long startOffset, long length, long stickyRecordLength, int nodeId, long epoch) {
        this.magicCode0 = WAL_HEADER_MAGIC_CODE_V0;
        this.startOffset1 = startOffset;
        this.length2 = length;
        this.stickyRecordLength3 = stickyRecordLength;
        this.nodeId4 = nodeId;
        this.epoch5 = epoch;
        this.trimOffset6 = -1;
    }

    public WALObjectHeader(long startOffset, long length, long stickyRecordLength, int nodeId, long epoch, long trimOffset) {
        this.magicCode0 = WAL_HEADER_MAGIC_CODE_V1;
        this.startOffset1 = startOffset;
        this.length2 = length;
        this.stickyRecordLength3 = stickyRecordLength;
        this.nodeId4 = nodeId;
        this.epoch5 = epoch;
        this.trimOffset6 = trimOffset;
    }

    public static WALObjectHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        buf.markReaderIndex();

        if (buf.readableBytes() < 4) {
            throw new UnmarshalException(String.format("Insufficient bytes to read magic code, Recovered: [%d] expect: [%d]", buf.readableBytes(), 4));
        }

        int magicCode = buf.readInt();
        if (!WAL_HEADER_SIZES.containsKey(magicCode)) {
            throw new UnmarshalException(String.format("WALHeader magic code not match, Recovered: [%d] expect: [%s]", magicCode, WAL_HEADER_SIZES.keySet()));
        }
        if (buf.readableBytes() < WAL_HEADER_SIZES.get(magicCode)) {
            throw new UnmarshalException(String.format("WALHeader does not have enough bytes, Recovered: [%d] expect: [%d]", buf.readableBytes(), WAL_HEADER_SIZES.get(magicCode)));
        }

        WALObjectHeader header = null;
        if (magicCode == WAL_HEADER_MAGIC_CODE_V1) {
            header = new WALObjectHeader(buf.readLong(), buf.readLong(), buf.readLong(), buf.readInt(), buf.readLong(), buf.readLong());
        } else if (magicCode == WAL_HEADER_MAGIC_CODE_V0) {
            header = new WALObjectHeader(buf.readLong(), buf.readLong(), buf.readLong(), buf.readInt(), buf.readLong());
        }

        buf.resetReaderIndex();
        return header;
    }

    public ByteBuf marshal() {
        if (magicCode0 == WAL_HEADER_MAGIC_CODE_V1) {
            return marshalV1();
        } else if (magicCode0 == WAL_HEADER_MAGIC_CODE_V0) {
            return marshalV0();
        } else {
            throw new IllegalStateException("Invalid magic code: " + magicCode0);
        }
    }

    private ByteBuf marshalV0() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE_V0);
        buf.writeInt(magicCode0);
        buf.writeLong(startOffset1);
        buf.writeLong(length2);
        buf.writeLong(stickyRecordLength3);
        buf.writeInt(nodeId4);
        buf.writeLong(epoch5);
        return buf;
    }

    private ByteBuf marshalV1() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE_V1);
        buf.writeInt(magicCode0);
        buf.writeLong(startOffset1);
        buf.writeLong(length2);
        buf.writeLong(stickyRecordLength3);
        buf.writeInt(nodeId4);
        buf.writeLong(epoch5);
        buf.writeLong(trimOffset6);
        return buf;
    }

    public int magicCode() {
        return magicCode0;
    }

    public long startOffset() {
        return startOffset1;
    }

    public long length() {
        return length2;
    }

    public long stickyRecordLength() {
        return stickyRecordLength3;
    }

    public int nodeId() {
        return nodeId4;
    }

    public long epoch() {
        return epoch5;
    }

    public long trimOffset() {
        return trimOffset6;
    }
}
