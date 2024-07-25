/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.model.StreamRecordBatch;
import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.ENCODE_RECORD;

public class StreamRecordBatchCodec {
    public static final byte MAGIC_V0 = 0x22;
    public static final int HEADER_SIZE =
        1 // magic
            + 8 // streamId
            + 8 // epoch
            + 8 // baseOffset
            + 4 // lastOffsetDelta
            + 4; // payload length
    private static final ByteBufSeqAlloc ENCODE_ALLOC = new ByteBufSeqAlloc(ENCODE_RECORD, 8);

    public static ByteBuf encode(StreamRecordBatch streamRecord) {
        int totalLength = HEADER_SIZE + streamRecord.size(); // payload
        // use sequential allocator to avoid memory fragmentation
        ByteBuf buf = ENCODE_ALLOC.byteBuffer(totalLength);
        buf.writeByte(MAGIC_V0);
        buf.writeLong(streamRecord.getStreamId());
        buf.writeLong(streamRecord.getEpoch());
        buf.writeLong(streamRecord.getBaseOffset());
        buf.writeInt(streamRecord.getCount());
        buf.writeInt(streamRecord.size());
        buf.writeBytes(streamRecord.getPayload().duplicate());
        return buf;
    }

    /**
     * Decode a stream record batch from a byte buffer and move the reader index.
     * The returned stream record batch does NOT share the payload buffer with the input buffer.
     */
    public static StreamRecordBatch duplicateDecode(ByteBuf buf) {
        byte magic = buf.readByte(); // magic
        if (magic != MAGIC_V0) {
            throw new RuntimeException("Invalid magic byte " + magic);
        }
        long streamId = buf.readLong();
        long epoch = buf.readLong();
        long baseOffset = buf.readLong();
        int lastOffsetDelta = buf.readInt();
        int payloadLength = buf.readInt();
        ByteBuf payload = ByteBufAlloc.byteBuffer(payloadLength, ByteBufAlloc.DECODE_RECORD);
        buf.readBytes(payload);
        return new StreamRecordBatch(streamId, epoch, baseOffset, lastOffsetDelta, payload);
    }

    /**
     * Decode a stream record batch from a byte buffer and move the reader index.
     * The returned stream record batch shares the payload buffer with the input buffer.
     */
    public static StreamRecordBatch decode(ByteBuf buf) {
        buf.readByte(); // magic
        long streamId = buf.readLong();
        long epoch = buf.readLong();
        long baseOffset = buf.readLong();
        int lastOffsetDelta = buf.readInt();
        int payloadLength = buf.readInt();
        ByteBuf payload = buf.slice(buf.readerIndex(), payloadLength);
        buf.skipBytes(payloadLength);
        return new StreamRecordBatch(streamId, epoch, baseOffset, lastOffsetDelta, payload);
    }
}
