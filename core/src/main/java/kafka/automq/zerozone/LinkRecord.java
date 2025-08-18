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

package kafka.automq.zerozone;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class LinkRecord {
    private static final byte MAGIC_V0 = (byte) 0x00;
    private final long lastOffset;
    private final TimestampType timestampType;
    private final long maxTimestamp;
    private final int partitionLeaderEpoch;
    private final ChannelOffset channelOffset;

    public LinkRecord(long lastOffset, TimestampType timestampType, long maxTimestamp, int partitionLeaderEpoch,
        ChannelOffset channelOffset) {
        this.lastOffset = lastOffset;
        this.timestampType = timestampType;
        this.maxTimestamp = maxTimestamp;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.channelOffset = channelOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public int partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public ChannelOffset channelOffset() {
        return channelOffset;
    }

    public static ByteBuf encode(ChannelOffset channelOffset, MemoryRecords memoryRecords) {
        // The MemoryRecords only contains one RecordBatch, cause of produce only send one RecordBatch per partition.
        if (channelOffset == null) {
            return null;
        }
        MutableRecordBatch recordBatch = memoryRecords.batches().iterator().next();
        long offset = recordBatch.lastOffset();
        long timestamp = recordBatch.maxTimestamp();
        int partitionLeaderEpoch = recordBatch.partitionLeaderEpoch();

        ByteBuf buffer = Unpooled.buffer(1 /* magic */ + 8 /* lastOffset */ + 4 /* timestampType */ + 8 /* maxTimestamp */
            + 4 /* partitionLeaderEpoch */ + channelOffset.byteBuf().readableBytes());
        buffer.writeByte(MAGIC_V0);
        buffer.writeLong(offset);
        buffer.writeInt(recordBatch.timestampType().id);
        buffer.writeLong(timestamp);
        buffer.writeInt(partitionLeaderEpoch);
        buffer.writeBytes(channelOffset.byteBuf().slice());

        return buffer;
    }

    public static LinkRecord decode(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC_V0) {
            throw new UnsupportedOperationException("Unsupported magic: " + magic);
        }
        long lastOffset = buf.readLong();
        TimestampType timestampType = TimestampType.forId(buf.readInt());
        long maxTimestamp = buf.readLong();
        int partitionLeaderEpoch = buf.readInt();
        ByteBuf channelOffset = Unpooled.buffer(buf.readableBytes());
        buf.readBytes(channelOffset);
        return new LinkRecord(lastOffset, timestampType, maxTimestamp, partitionLeaderEpoch, ChannelOffset.of(channelOffset));
    }
}
