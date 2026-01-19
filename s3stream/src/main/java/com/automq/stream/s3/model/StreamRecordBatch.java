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

package com.automq.stream.s3.model;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufSupplier;
import com.automq.stream.utils.biniarysearch.ComparableItem;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.ByteBufAlloc.DECODE_RECORD;
import static com.automq.stream.s3.ByteBufAlloc.ENCODE_RECORD;
import static com.automq.stream.s3.StreamRecordBatchCodec.BASE_OFFSET_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.EPOCH_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.HEADER_SIZE;
import static com.automq.stream.s3.StreamRecordBatchCodec.LAST_OFFSET_DELTA_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.MAGIC_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.MAGIC_V0;
import static com.automq.stream.s3.StreamRecordBatchCodec.PAYLOAD_LENGTH_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.PAYLOAD_POS;
import static com.automq.stream.s3.StreamRecordBatchCodec.STREAM_ID_POS;

public class StreamRecordBatch implements Comparable<StreamRecordBatch>, ComparableItem<Long> {
    private static final int OBJECT_OVERHEAD = 48 /* fields */ + 48 /* ByteBuf payload */ + 48 /* ByteBuf encoded */;
    private static final ByteBufSeqAlloc ENCODE_ALLOC = new ByteBufSeqAlloc(ENCODE_RECORD, 8);
    private static final ByteBufSeqAlloc DECODE_ALLOC = new ByteBufSeqAlloc(DECODE_RECORD, 8);
    // Cache the frequently used fields
    private final long baseOffset;
    private final int count;

    final ByteBuf encoded;

    private StreamRecordBatch(ByteBuf encoded) {
        this.encoded = encoded;
        this.baseOffset = encoded.getLong(encoded.readerIndex() + BASE_OFFSET_POS);
        this.count = encoded.getInt(encoded.readerIndex() + LAST_OFFSET_DELTA_POS);
    }

    public ByteBuf encoded() {
        return encoded.slice();
    }

    public long getStreamId() {
        return encoded.getLong(encoded.readerIndex() + STREAM_ID_POS);
    }

    public long getEpoch() {
        return encoded.getLong(encoded.readerIndex() + EPOCH_POS);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getLastOffset() {
        long baseOffset = getBaseOffset();
        int count = getCount();
        if (count > 0) {
            return baseOffset + count;
        } else {
            // link record
            return baseOffset - count;
        }
    }

    public int getCount() {
        return count;
    }

    public ByteBuf getPayload() {
        return encoded.slice(encoded.readerIndex() + PAYLOAD_POS, encoded.readableBytes() - HEADER_SIZE);
    }

    public int size() {
        return encoded.getInt(encoded.readerIndex() + PAYLOAD_LENGTH_POS);
    }

    public int occupiedSize() {
        return size() + OBJECT_OVERHEAD;
    }

    public void retain() {
        encoded.retain();
    }

    public void release() {
        encoded.release();
    }

    @Override
    public int compareTo(StreamRecordBatch o) {
        int rst = Long.compare(getStreamId(), o.getStreamId());
        if (rst != 0) {
            return rst;
        }
        return Long.compare(getBaseOffset(), o.getBaseOffset());
    }

    @Override
    public String toString() {
        return "StreamRecordBatch{" +
            "streamId=" + getStreamId() +
            ", epoch=" + getEpoch() +
            ", baseOffset=" + getBaseOffset() +
            ", count=" + getCount() +
            ", size=" + size() + '}';
    }

    @Override
    public boolean isLessThan(Long value) {
        return getLastOffset() <= value;
    }

    @Override
    public boolean isGreaterThan(Long value) {
        return getBaseOffset() > value;
    }

    public static StreamRecordBatch of(long streamId, long epoch, long baseOffset, int count, ByteBuffer payload) {
        return of(streamId, epoch, baseOffset, count, Unpooled.wrappedBuffer(payload), ENCODE_ALLOC);
    }

    public static StreamRecordBatch of(long streamId, long epoch, long baseOffset, int count, ByteBuffer payload, ByteBufSupplier alloc) {
        return of(streamId, epoch, baseOffset, count, Unpooled.wrappedBuffer(payload), alloc);
    }

    /**
     * StreamRecordBatch.of expects take the owner of the payload.
     * The payload will be copied to the new StreamRecordBatch and released.
     */
    public static StreamRecordBatch of(long streamId, long epoch, long baseOffset, int count, ByteBuf payload) {
        return of(streamId, epoch, baseOffset, count, payload, ENCODE_ALLOC);
    }

    /**
     * StreamRecordBatch.of expects take the owner of the payload.
     * The payload will be copied to the new StreamRecordBatch and released.
     */
    public static StreamRecordBatch of(long streamId, long epoch, long baseOffset, int count, ByteBuf payload,
        ByteBufSupplier alloc) {
        int totalLength = HEADER_SIZE + payload.readableBytes();
        ByteBuf buf = alloc.alloc(totalLength);
        buf.writeByte(MAGIC_V0);
        buf.writeLong(streamId);
        buf.writeLong(epoch);
        buf.writeLong(baseOffset);
        buf.writeInt(count);
        buf.writeInt(payload.readableBytes());
        buf.writeBytes(payload);
        payload.release();
        return new StreamRecordBatch(buf);
    }

    public static StreamRecordBatch parse(ByteBuf buf, boolean duplicated) {
        return parse(buf, duplicated, DECODE_ALLOC);
    }

    /**
     * Won't release the input ByteBuf.
     * - If duplicated is true, the returned StreamRecordBatch has its own copy of the data.
     * - If duplicated is false, the returned StreamRecordBatch shares and retains the data buffer with the input.
     */
    public static StreamRecordBatch parse(ByteBuf buf, boolean duplicated, ByteBufSeqAlloc alloc) {
        int readerIndex = buf.readerIndex();
        byte magic = buf.getByte(readerIndex + MAGIC_POS);
        if (magic != MAGIC_V0) {
            throw new RuntimeException("Invalid magic byte " + magic);
        }
        int payloadSize = buf.getInt(readerIndex + PAYLOAD_LENGTH_POS);
        int encodedSize = PAYLOAD_POS + payloadSize;
        if (duplicated) {
            ByteBuf encoded = alloc.alloc(encodedSize);
            buf.readBytes(encoded, encodedSize);
            return new StreamRecordBatch(encoded);
        } else {
            ByteBuf encoded = buf.retainedSlice(readerIndex, encodedSize);
            buf.skipBytes(encodedSize);
            return new StreamRecordBatch(encoded);
        }
    }
}
