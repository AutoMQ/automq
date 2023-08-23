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

package kafka.log.s3;

import com.automq.elasticstream.client.DefaultRecordBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import kafka.log.s3.model.StreamRecordBatch;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class StreamRecordBatchCodec {
    private static final byte MAGIC_V0 = 0x22;
    private static final PooledByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;

    public static ByteBuf encode(StreamRecordBatch streamRecord) {
        int totalLength = 1 // magic
                + 8 // streamId
                + 8 // epoch
                + 8 // baseOffset
                + 4 // lastOffsetDelta
                + 4 // payload length
                + streamRecord.getRecordBatch().rawPayload().remaining(); // payload

        ByteBuf buf = ALLOCATOR.heapBuffer(totalLength);
        buf.writeByte(MAGIC_V0);
        buf.writeLong(streamRecord.getStreamId());
        buf.writeLong(streamRecord.getEpoch());
        buf.writeLong(streamRecord.getBaseOffset());
        buf.writeInt(streamRecord.getRecordBatch().count());
        ByteBuffer payload = streamRecord.getRecordBatch().rawPayload().duplicate();
        buf.writeInt(payload.remaining());
        buf.writeBytes(payload);
        return buf;
    }

    /**
     * Decode a stream record batch from a byte buffer and move the reader index.
     */
    public static StreamRecordBatch decode(DataInputStream in) {
        try {
            in.readByte(); // magic
            long streamId = in.readLong();
            long epoch = in.readLong();
            long baseOffset = in.readLong();
            int lastOffsetDelta = in.readInt();
            int payloadLength = in.readInt();
            ByteBuffer payload = ByteBuffer.allocate(payloadLength);
            in.readFully(payload.array());
            DefaultRecordBatch defaultRecordBatch = new DefaultRecordBatch(lastOffsetDelta, 0, Collections.emptyMap(), payload);
            return new StreamRecordBatch(streamId, epoch, baseOffset, defaultRecordBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
