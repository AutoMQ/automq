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

public class ChannelOffset {
    private static final byte MAGIC = (byte) 0x86;
    private static final int CHANNEL_ID_INDEX = 1;
    private static final int ORDER_HINT_INDEX = 3;
    private static final int CHANNEL_OWNER_NODE_ID_INDEX = 5;
    private static final int CHANNEL_ATTRIBUTES = 9;
    private static final int WAL_RECORD_OFFSET_INDEX = 13;

    private final ByteBuf buf;

    private ChannelOffset(ByteBuf buf) {
        this.buf = buf;
    }

    public static ChannelOffset of(ByteBuf buf) {
        return new ChannelOffset(buf);
    }

    public static ChannelOffset of(short channelId, short orderHint, int channelOwnerNodeId, int attributes,
        ByteBuf walRecordOffset) {
        ByteBuf channelOffset = Unpooled.buffer(1 /* magic */ + 2 /* channelId */ + 2 /* orderHint */
            + 4 /* channelOwnerNodeId */ + 4 /* targetNodeId */ + walRecordOffset.readableBytes());
        channelOffset.writeByte(MAGIC);
        channelOffset.writeShort(channelId);
        channelOffset.writeShort(orderHint);
        channelOffset.writeInt(channelOwnerNodeId);
        channelOffset.writeInt(attributes);
        channelOffset.writeBytes(walRecordOffset.duplicate());
        return of(channelOffset);
    }

    public short channelId() {
        return buf.getShort(CHANNEL_ID_INDEX);
    }

    public short orderHint() {
        return buf.getShort(ORDER_HINT_INDEX);
    }

    public int channelOwnerNodeId() {
        return buf.getInt(CHANNEL_OWNER_NODE_ID_INDEX);
    }

    public int attributes() {
        return buf.getInt(CHANNEL_ATTRIBUTES);
    }

    public ByteBuf walRecordOffset() {
        return buf.slice(WAL_RECORD_OFFSET_INDEX, buf.readableBytes() - WAL_RECORD_OFFSET_INDEX);
    }

    public ByteBuf byteBuf() {
        return buf;
    }

    @Override
    public String toString() {
        return "ChannelOffset{" +
            "channelId=" + channelId() +
            ", orderHint=" + orderHint() +
            ", channelOwnerNodeId=" + channelOwnerNodeId() +
            ", attributes=" + attributes() +
            '}';
    }
}
