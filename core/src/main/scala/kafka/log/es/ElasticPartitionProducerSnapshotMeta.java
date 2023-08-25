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

package kafka.log.es;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * refers to one snapshot file
 */
public class ElasticPartitionProducerSnapshotMeta {
    /**
     * raw data of the snapshot
     */
    private byte[] rawSnapshotData;
    /**
     * the offset of the snapshot. Snapshot file name = offset + ".snapshot"
     */
    private long offset;

    public ElasticPartitionProducerSnapshotMeta(long offset, byte[] snapshot) {
        this.offset = offset;
        this.rawSnapshotData = snapshot;
    }

    public byte[] getRawSnapshotData() {
        return rawSnapshotData;
    }

    public void setRawSnapshotData(byte[] rawSnapshotData) {
        this.rawSnapshotData = rawSnapshotData;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean isEmpty() {
        return rawSnapshotData == null || rawSnapshotData.length == 0;
    }

    public ByteBuffer encode() {
        if (rawSnapshotData == null || rawSnapshotData.length == 0) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(offset);
            buffer.flip();
            return buffer;
        }

        ByteBuffer buffer = ByteBuffer.allocate(rawSnapshotData.length + 8);
        buffer.putLong(offset);
        buffer.put(rawSnapshotData);
        buffer.flip();
        return buffer;
    }

    public String fileName() {
        return offset + ".snapshot";
    }

    public static ElasticPartitionProducerSnapshotMeta decode(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() == 0) {
            return new ElasticPartitionProducerSnapshotMeta(-1, null);
        }

        long offset = buffer.getLong();
        byte[] snapshot = new byte[buffer.remaining()];
        buffer.get(snapshot);
        return new ElasticPartitionProducerSnapshotMeta(offset, snapshot);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ElasticPartitionProducerSnapshotMeta)) {
            return false;
        }
        ElasticPartitionProducerSnapshotMeta other = (ElasticPartitionProducerSnapshotMeta) o;
        return offset == other.offset && Arrays.equals(rawSnapshotData, other.rawSnapshotData);
    }

    @Override
    public int hashCode() {
        if (isEmpty()) {
            return 0;
        }
        return (int) (offset ^ (offset >>> 32)) + Arrays.hashCode(rawSnapshotData);
    }
}
