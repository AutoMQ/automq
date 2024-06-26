/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.common;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;

public class RecordHeader {
    public static final int RECORD_HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    public static final int RECORD_HEADER_WITHOUT_CRC_SIZE = RECORD_HEADER_SIZE - 4;
    public static final int RECORD_HEADER_MAGIC_CODE = 0x87654321;

    private int magicCode0 = RECORD_HEADER_MAGIC_CODE;
    private int recordBodyLength1;
    private long recordBodyOffset2;
    private int recordBodyCRC3;
    private int recordHeaderCRC4;

    public static RecordHeader unmarshal(ByteBuf byteBuf) {
        RecordHeader recordHeader = new RecordHeader();
        byteBuf.markReaderIndex();
        recordHeader.magicCode0 = byteBuf.readInt();
        recordHeader.recordBodyLength1 = byteBuf.readInt();
        recordHeader.recordBodyOffset2 = byteBuf.readLong();
        recordHeader.recordBodyCRC3 = byteBuf.readInt();
        recordHeader.recordHeaderCRC4 = byteBuf.readInt();
        byteBuf.resetReaderIndex();
        return recordHeader;
    }

    public int getMagicCode() {
        return magicCode0;
    }

    public RecordHeader setMagicCode(int magicCode) {
        this.magicCode0 = magicCode;
        return this;
    }

    public int getRecordBodyLength() {
        return recordBodyLength1;
    }

    public RecordHeader setRecordBodyLength(int recordBodyLength) {
        this.recordBodyLength1 = recordBodyLength;
        return this;
    }

    public long getRecordBodyOffset() {
        return recordBodyOffset2;
    }

    public RecordHeader setRecordBodyOffset(long recordBodyOffset) {
        this.recordBodyOffset2 = recordBodyOffset;
        return this;
    }

    public int getRecordBodyCRC() {
        return recordBodyCRC3;
    }

    public RecordHeader setRecordBodyCRC(int recordBodyCRC) {
        this.recordBodyCRC3 = recordBodyCRC;
        return this;
    }

    public int getRecordHeaderCRC() {
        return recordHeaderCRC4;
    }

    @Override
    public String toString() {
        return "RecordHeaderCoreData{" +
               "magicCode=" + magicCode0 +
               ", recordBodyLength=" + recordBodyLength1 +
               ", recordBodyOffset=" + recordBodyOffset2 +
               ", recordBodyCRC=" + recordBodyCRC3 +
               ", recordHeaderCRC=" + recordHeaderCRC4 +
               '}';
    }

    private ByteBuf marshalHeaderExceptCRC() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        buf.writeInt(magicCode0);
        buf.writeInt(recordBodyLength1);
        buf.writeLong(recordBodyOffset2);
        buf.writeInt(recordBodyCRC3);
        return buf;
    }

    public ByteBuf marshal() {
        ByteBuf buf = marshalHeaderExceptCRC();
        buf.writeInt(WALUtil.crc32(buf, RECORD_HEADER_WITHOUT_CRC_SIZE));
        return buf;
    }
}
