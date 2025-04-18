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

public class WALObjectHeader {
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345678;
    public static final int WAL_HEADER_SIZE = 4 // magic code
                                              + 8 // start offset
                                              + 8 // body length
                                              + 8 // sticky record length
                                              + 4 // node id
                                              + 8; // node epoch

    private int magicCode0 = WAL_HEADER_MAGIC_CODE;
    private long startOffset1;
    private long length2;
    private long stickyRecordLength3;
    private int nodeId4;
    private long epoch5;

    public WALObjectHeader() {
    }

    public WALObjectHeader(long startOffset, long length, long stickyRecordLength, int nodeId, long epoch) {
        this.startOffset1 = startOffset;
        this.length2 = length;
        this.stickyRecordLength3 = stickyRecordLength;
        this.nodeId4 = nodeId;
        this.epoch5 = epoch;
    }

    public static WALObjectHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        if (buf.readableBytes() < WAL_HEADER_SIZE) {
            throw new UnmarshalException(String.format("WALHeader does not have enough bytes, Recovered: [%d] expect: [%d]", buf.readableBytes(), WAL_HEADER_SIZE));
        }

        WALObjectHeader walObjectHeader = new WALObjectHeader();
        buf.markReaderIndex();
        walObjectHeader.magicCode0 = buf.readInt();
        if (walObjectHeader.magicCode0 != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("WALHeader magic code not match, Recovered: [%d] expect: [%d]", walObjectHeader.magicCode0, WAL_HEADER_MAGIC_CODE));
        }

        walObjectHeader.startOffset1 = buf.readLong();
        walObjectHeader.length2 = buf.readLong();
        walObjectHeader.stickyRecordLength3 = buf.readLong();
        walObjectHeader.nodeId4 = buf.readInt();
        walObjectHeader.epoch5 = buf.readLong();
        buf.resetReaderIndex();

        return walObjectHeader;
    }

    public ByteBuf marshal() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
        buf.writeInt(magicCode0);
        buf.writeLong(startOffset1);
        buf.writeLong(length2);
        buf.writeLong(stickyRecordLength3);
        buf.writeInt(nodeId4);
        buf.writeLong(epoch5);
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
}
