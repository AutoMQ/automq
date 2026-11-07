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

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ZoneRouterResponseCodec {
    public static final byte PRODUCE_RESPONSE_BLOCK_MAGIC = 0x01;

    public static ByteBuf encode(ProduceResponseData produceResponseData) {
        short version = 11;
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        int size = produceResponseData.size(objectSerializationCache, version);
        ByteBuf buf = Unpooled.buffer(1 /* magic */ + 2 /* version */ + size);
        buf.writeByte(PRODUCE_RESPONSE_BLOCK_MAGIC);
        buf.writeShort(version);
        produceResponseData.write(new ByteBufferAccessor(buf.nioBuffer(buf.writerIndex(), size)), objectSerializationCache, version);
        buf.writerIndex(buf.writerIndex() + size);
        return buf;
    }

    public static ProduceResponseData decode(ByteBuf buf) {
        byte magic = buf.readByte();
        if (magic != PRODUCE_RESPONSE_BLOCK_MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        short version = buf.readShort();
        ProduceResponseData produceResponseData = new ProduceResponseData();
        produceResponseData.read(new ByteBufferAccessor(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())), version);
        return produceResponseData;
    }

}
