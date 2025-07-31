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

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterRecordV2 {
    private static final short MAGIC = 0x02;
    private final int nodeId;
    private final List<ByteBuf> channelOffsets;

    public RouterRecordV2(int nodeId, List<ByteBuf> channelOffsets) {
        this.nodeId = nodeId;
        this.channelOffsets = channelOffsets;
    }

    public int nodeId() {
        return nodeId;
    }

    public List<ByteBuf> channelOffsets() {
        return channelOffsets;
    }

    public ByteBuf encode() {
        int size = 1 /* magic */ + 4 /* nodeId */ + channelOffsets.stream().mapToInt(buf -> buf.readableBytes() + 2).sum();
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeByte(MAGIC);
        buf.writeInt(nodeId);
        channelOffsets.forEach(channelOffset -> {
            buf.writeShort(channelOffset.readableBytes());
            buf.writeBytes(channelOffset.duplicate());
        });
        return buf;
    }

    public static RouterRecordV2 decode(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        int nodeId = buf.readInt();
        List<ByteBuf> channelOffsets = new ArrayList<>(buf.readableBytes() / 16);
        while (buf.readableBytes() > 0) {
            short size = buf.readShort();
            ByteBuf channelOffset = Unpooled.buffer(size);
            buf.readBytes(channelOffset);
            channelOffsets.add(channelOffset);
        }
        return new RouterRecordV2(nodeId, channelOffsets);
    }

}
