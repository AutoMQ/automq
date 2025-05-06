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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterRecord {
    private static final short MAGIC = 0x01;
    private final int nodeId;
    private final short bucketId;
    private final long objectId;
    private final int position;
    private final int size;

    public RouterRecord(int nodeId, short bucketId, long objectId, int position, int size) {
        this.nodeId = nodeId;
        this.bucketId = bucketId;
        this.objectId = objectId;
        this.position = position;
        this.size = size;
    }

    public int nodeId() {
        return nodeId;
    }

    public short bucketId() {
        return bucketId;
    }

    public long objectId() {
        return objectId;
    }

    public int position() {
        return position;
    }

    public int size() {
        return size;
    }

    public ByteBuf encode() {
        ByteBuf buf = Unpooled.buffer(1 /* magic */ + 4 /* nodeId */ + 2 /* bucketId */ + 8 /* objectId */ + 4 /* position */ + 4 /* size */);
        buf.writeByte(MAGIC);
        buf.writeInt(nodeId);
        buf.writeShort(bucketId);
        buf.writeLong(objectId);
        buf.writeInt(position);
        buf.writeInt(size);
        return buf;
    }

    @Override
    public String toString() {
        return "RouterRecord{" +
            "nodeId=" + nodeId +
            ", bucketId=" + bucketId +
            ", objectId=" + objectId +
            ", position=" + position +
            ", size=" + size +
            '}';
    }

    public static RouterRecord decode(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        int nodeId = buf.readInt();
        short bucketId = buf.readShort();
        long objectId = buf.readLong();
        int position = buf.readInt();
        int size = buf.readInt();
        return new RouterRecord(nodeId, bucketId, objectId, position, size);
    }

}
