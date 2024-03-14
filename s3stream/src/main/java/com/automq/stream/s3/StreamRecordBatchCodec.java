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

package com.automq.stream.s3;

import com.automq.stream.s3.model.StreamRecordBatch;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.IOException;

public class StreamRecordBatchCodec {
    public static final byte MAGIC_V0 = 0x22;

    public static ByteBuf encode(StreamRecordBatch streamRecord) {
        int totalLength = 1 // magic
                + 8 // streamId
                + 8 // epoch
                + 8 // baseOffset
                + 4 // lastOffsetDelta
                + 4 // payload length
                + streamRecord.size(); // payload

        ByteBuf buf = DirectByteBufAlloc.byteBuffer(totalLength);
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
     */
    public static StreamRecordBatch decode(DataInputStream in) {
        try {
            byte magic = in.readByte(); // magic
            if (magic != MAGIC_V0) {
                throw new RuntimeException("Invalid magic byte " + magic);
            }
            long streamId = in.readLong();
            long epoch = in.readLong();
            long baseOffset = in.readLong();
            int lastOffsetDelta = in.readInt();
            int payloadLength = in.readInt();
            byte[] payloadBytes = new byte[payloadLength];
            in.readFully(payloadBytes);
            ByteBuf payload = DirectByteBufAlloc.byteBuffer(payloadLength);
            payload.writeBytes(payloadBytes);
            return new StreamRecordBatch(streamId, epoch, baseOffset, lastOffsetDelta, payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
