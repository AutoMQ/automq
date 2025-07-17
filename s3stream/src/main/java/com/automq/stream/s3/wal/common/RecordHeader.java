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

package com.automq.stream.s3.wal.common;

import com.automq.stream.s3.wal.util.WALUtil;

import io.netty.buffer.ByteBuf;

public class RecordHeader {
    public static final int RECORD_HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    public static final int RECORD_HEADER_WITHOUT_CRC_SIZE = RECORD_HEADER_SIZE - 4;
    public static final int RECORD_HEADER_DATA_MAGIC_CODE = 0x87654321;
    /**
     * Magic code for record header indicating that the record body is empty (used for padding).
     */
    public static final int RECORD_HEADER_EMPTY_MAGIC_CODE = 0x76543210;

    private final int magicCode0;
    private final int recordBodyLength1;
    private final long recordBodyOffset2;
    private final int recordBodyCRC3;
    private int recordHeaderCRC4;

    public RecordHeader(long offset, int length, int crc) {
        this.magicCode0 = RECORD_HEADER_DATA_MAGIC_CODE;
        this.recordBodyLength1 = length;
        this.recordBodyOffset2 = offset + RECORD_HEADER_SIZE;
        this.recordBodyCRC3 = crc;
    }

    public RecordHeader(long offset, int length) {
        this.magicCode0 = RECORD_HEADER_EMPTY_MAGIC_CODE;
        this.recordBodyLength1 = length;
        this.recordBodyOffset2 = offset + RECORD_HEADER_SIZE;
        this.recordBodyCRC3 = 0;
    }

    public RecordHeader(ByteBuf byteBuf) {
        byteBuf.markReaderIndex();
        this.magicCode0 = byteBuf.readInt();
        this.recordBodyLength1 = byteBuf.readInt();
        this.recordBodyOffset2 = byteBuf.readLong();
        this.recordBodyCRC3 = byteBuf.readInt();
        this.recordHeaderCRC4 = byteBuf.readInt();
        byteBuf.resetReaderIndex();
    }

    public int getMagicCode() {
        return magicCode0;
    }

    public int getRecordBodyLength() {
        return recordBodyLength1;
    }

    public long getRecordBodyOffset() {
        return recordBodyOffset2;
    }

    public int getRecordBodyCRC() {
        return recordBodyCRC3;
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

    private ByteBuf marshalHeaderExceptCRC(ByteBuf buf) {
        buf.writeInt(magicCode0);
        buf.writeInt(recordBodyLength1);
        buf.writeLong(recordBodyOffset2);
        buf.writeInt(recordBodyCRC3);
        return buf;
    }

    public ByteBuf marshal(ByteBuf emptyBuf) {
        assert emptyBuf.writableBytes() == RECORD_HEADER_SIZE;
        ByteBuf buf = marshalHeaderExceptCRC(emptyBuf);
        buf.writeInt(WALUtil.crc32(buf, RECORD_HEADER_WITHOUT_CRC_SIZE));
        return buf;
    }
}
