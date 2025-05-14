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

import java.util.Map;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

public class WALObjectHeader {
    // Visible for testing.
    static final int WAL_HEADER_MAGIC_CODE_V0 = 0x12345678;
    static final int WAL_HEADER_SIZE_V0 = 4  // magic code
                                        + 8  // start offset
                                        + 8  // body length
                                        + 8  // sticky record length
                                        + 4  // node id
                                        + 8; // node epoch
    static final int WAL_HEADER_MAGIC_CODE_V1 = 0xEDCBA987;
    static final int WAL_HEADER_SIZE_V1 = WAL_HEADER_SIZE_V0
                                        + 8; // trim offset

    private static final Map<Integer, Integer> WAL_HEADER_SIZES = Map.of(
            WAL_HEADER_MAGIC_CODE_V0, WAL_HEADER_SIZE_V0,
            WAL_HEADER_MAGIC_CODE_V1, WAL_HEADER_SIZE_V1
    );

    public static final int DEFAULT_WAL_MAGIC_CODE = WAL_HEADER_MAGIC_CODE_V1;
    public static final int DEFAULT_WAL_HEADER_SIZE = WAL_HEADER_SIZE_V1;

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

    /**
     * In the historical version V0, the endOffset of each WAL object is calculated directly from the path and size of the object.
     * This method is used to be compatible with this case.
     *
     * @param startOffset the start offset of the WAL object, get from the path
     * @param length the size of the WAL object
     * @return the end offset of the WAL object
     */
    public static long calculateEndOffsetV0(long startOffset, long length) {
        return startOffset + length - WAL_HEADER_SIZE_V0;
    }

    public static WALObjectHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        buf.markReaderIndex();

        int size = buf.readableBytes();
        if (size < 4) {
            throw new UnmarshalException(String.format("Insufficient bytes to read magic code, Recovered: [%d] expect: [%d]", size, 4));
        }

        int magicCode = buf.readInt();
        if (!WAL_HEADER_SIZES.containsKey(magicCode)) {
            throw new UnmarshalException(String.format("WALHeader magic code not match, Recovered: [%d] expect: [%s]", magicCode, WAL_HEADER_SIZES.keySet()));
        }
        if (size < WAL_HEADER_SIZES.get(magicCode)) {
            throw new UnmarshalException(String.format("WALHeader does not have enough bytes, Recovered: [%d] expect: [%d]", size, WAL_HEADER_SIZES.get(magicCode)));
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

    public int size() {
        return WAL_HEADER_SIZES.get(magicCode0);
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WALObjectHeader))
            return false;
        WALObjectHeader header = (WALObjectHeader) o;
        return magicCode0 == header.magicCode0 && startOffset1 == header.startOffset1 && length2 == header.length2 && stickyRecordLength3 == header.stickyRecordLength3 && nodeId4 == header.nodeId4 && epoch5 == header.epoch5 && trimOffset6 == header.trimOffset6;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magicCode0, startOffset1, length2, stickyRecordLength3, nodeId4, epoch5, trimOffset6);
    }
}
