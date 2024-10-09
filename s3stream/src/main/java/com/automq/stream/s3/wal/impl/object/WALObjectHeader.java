/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
