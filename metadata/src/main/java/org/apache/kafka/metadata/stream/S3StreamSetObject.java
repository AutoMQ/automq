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

package org.apache.kafka.metadata.stream;

import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class S3StreamSetObject implements Comparable<S3StreamSetObject> {
    public static final byte MAGIC = 0x01;
    public static final byte ZSTD_COMPRESSED = 1 << 1;
    private static final int COMPRESSION_THRESHOLD = 50;

    private final long objectId;
    private final int nodeId;
    private final byte[] ranges;

    /**
     * The order id of the object. Sort by this field to get the order of the objects which contains logically increasing streams.
     * <p>
     * When compact a batch of objects to a compacted object, this compacted object's order id will be assigned the value <code>first object's order
     * id in this batch</code>
     */
    private final long orderId;
    private final long dataTimeInMs;

    // Only used for testing
    public S3StreamSetObject(long objectId, int nodeId, final List<StreamOffsetRange> streamOffsetRanges, long orderId) {
        this(objectId, nodeId, sortAndEncode(streamOffsetRanges), orderId, S3StreamConstant.INVALID_TS);
    }

    public S3StreamSetObject(long objectId, int nodeId, final List<StreamOffsetRange> streamOffsetRanges, long orderId, long dateTimeInMs) {
        this(objectId, nodeId, sortAndEncode(streamOffsetRanges), orderId, dateTimeInMs);
    }

    public S3StreamSetObject(long objectId, int nodeId, byte[] ranges, long orderId, long dataTimeInMs) {
        this.orderId = orderId;
        this.objectId = objectId;
        this.nodeId = nodeId;
        this.ranges = ranges;
        this.dataTimeInMs = dataTimeInMs;
    }

    public List<StreamOffsetRange> offsetRangeList() {
        return decode(ranges);
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3StreamSetObjectRecord()
                .setObjectId(objectId)
                .setNodeId(nodeId)
                .setOrderId(orderId)
                .setDataTimeInMs(dataTimeInMs)
                .setRanges(ranges), (short) 0);
    }

    public static S3StreamSetObject of(S3StreamSetObjectRecord record) {
        return new S3StreamSetObject(record.objectId(), record.nodeId(),
                record.ranges(), record.orderId(), record.dataTimeInMs());
    }

    public Integer nodeId() {
        return nodeId;
    }

    public Long objectId() {
        return objectId;
    }

    public S3ObjectType objectType() {
        return S3ObjectType.STREAM_SET;
    }

    public long orderId() {
        return orderId;
    }

    public long dataTimeInMs() {
        return dataTimeInMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3StreamSetObject that = (S3StreamSetObject) o;
        return objectId == that.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId);
    }

    @Override
    public String toString() {
        return "S3StreamSetObject{" +
                "objectId=" + objectId +
                ", orderId=" + orderId +
                ", nodeId=" + nodeId +
                ", dataTimeInMs=" + dataTimeInMs +
                '}';
    }

    @Override
    public int compareTo(S3StreamSetObject o) {
        return Long.compare(this.orderId, o.orderId);
    }

    public static byte[] sortAndEncode(List<StreamOffsetRange> streamOffsetRanges) {
        streamOffsetRanges = new ArrayList<>(streamOffsetRanges);
        streamOffsetRanges.sort(Comparator.comparingLong(StreamOffsetRange::streamId));
        return encode(streamOffsetRanges);
    }

    public static byte[] encode(List<StreamOffsetRange> streamOffsetRanges) {
        boolean compressed = streamOffsetRanges.size() > COMPRESSION_THRESHOLD;
        int flag = 0;
        if (compressed) {
            flag = flag | ZSTD_COMPRESSED;
        }
        ByteBuf rangesBuf = Unpooled.buffer(streamOffsetRanges.size() * 20);
        streamOffsetRanges.forEach(r -> {
            rangesBuf.writeLong(r.streamId());
            rangesBuf.writeLong(r.startOffset());
            rangesBuf.writeInt((int) (r.endOffset() - r.startOffset()));
        });
        byte[] compressedBytes;
        if (compressed) {
            compressedBytes = Zstd.compress(rangesBuf.array());
        } else {
            compressedBytes = rangesBuf.array();
        }
        ByteBuf buf = Unpooled.buffer(1 /* magic */ + 1 /* flag */ + 4 /* origin size */ + compressedBytes.length);
        buf.writeByte(MAGIC);
        buf.writeByte(flag);
        buf.writeInt(rangesBuf.readableBytes());
        buf.writeBytes(compressedBytes);
        return buf.array();
    }

    public static List<StreamOffsetRange> decode(byte[] bytes) {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        byte flag = buf.readByte();
        int rangeBytesSize = buf.readInt();
        byte[] rangesBytes;
        boolean compressed = (flag & ZSTD_COMPRESSED) != 0;
        byte[] compressedBytes = new byte[buf.readableBytes()];
        buf.readBytes(compressedBytes);
        if (compressed) {
            rangesBytes = Zstd.decompress(compressedBytes, rangeBytesSize);
        } else {
            rangesBytes = compressedBytes;
        }
        List<StreamOffsetRange> ranges = new ArrayList<>(rangeBytesSize / 20);
        ByteBuf rangesBuf = Unpooled.wrappedBuffer(rangesBytes);
        while (rangesBuf.readableBytes() > 0) {
            long streamId = rangesBuf.readLong();
            long startOffset = rangesBuf.readLong();
            int count = rangesBuf.readInt();
            ranges.add(new StreamOffsetRange(streamId, startOffset, startOffset + count));
        }
        return ranges;
    }
}
