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

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static kafka.automq.zerozone.ZoneRouterPack.FOOTER_SIZE;
import static kafka.automq.zerozone.ZoneRouterPack.PACK_MAGIC;
import static kafka.automq.zerozone.ZoneRouterPack.genObjectPath;

public class ZoneRouterPackWriter {
    private final String path;
    private final Writer writer;
    private final CompositeByteBuf dataBuf;

    public ZoneRouterPackWriter(int nodeId, long objectId, ObjectStorage objectStorage) {
        this.path = genObjectPath(nodeId, objectId);
        this.dataBuf = ByteBufAlloc.compositeByteBuffer();
        this.writer = objectStorage.writer(new ObjectStorage.WriteOptions().enableFastRetry(true), path);
    }

    public Position addProduceRequests(List<ZoneRouterProduceRequest> produceRequests) {
        int position = dataBuf.writerIndex();
        ByteBuf buf = encodeDataBlock(produceRequests);
        int size = buf.readableBytes();
        dataBuf.addComponent(true, buf);
        return new Position(position, size);
    }

    public short bucketId() {
        return writer.bucketId();
    }

    public ObjectStorage.ObjectPath objectPath() {
        return new ObjectStorage.ObjectPath(bucketId(), path);
    }

    public CompletableFuture<Void> close() {
        ByteBuf footer = ByteBufAlloc.byteBuffer(FOOTER_SIZE);
        footer.writeZero(40);
        footer.writeLong(PACK_MAGIC);
        dataBuf.addComponent(true, footer);
        writer.write(dataBuf);
        return writer.close();
    }

    public static ByteBuf encodeDataBlock(List<ZoneRouterProduceRequest> produceRequests) {
        int size = 1 /* magic */;
        List<ObjectSerializationCache> objectSerializationCaches = new ArrayList<>(produceRequests.size());
        List<Integer> dataSizes = new ArrayList<>(produceRequests.size());
        for (ZoneRouterProduceRequest produceRequest : produceRequests) {

            size += 2 /* api version */ + 2 /* flag */ + 4 /* data size */;

            ProduceRequestData data = produceRequest.data();
            ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
            objectSerializationCaches.add(objectSerializationCache);
            int dataSize = data.size(objectSerializationCache, produceRequest.apiVersion());
            dataSizes.add(dataSize);
            size += dataSize;
        }
        ByteBuf buf = ByteBufAlloc.byteBuffer(size);
        buf.writeByte(ZoneRouterPack.PRODUCE_DATA_BLOCK_MAGIC);
        for (int i = 0; i < produceRequests.size(); i++) {
            ZoneRouterProduceRequest produceRequest = produceRequests.get(i);
            int dataSize = dataSizes.get(i);
            ProduceRequestData data = produceRequest.data();
            ObjectSerializationCache objectSerializationCache = objectSerializationCaches.get(i);

            buf.writeShort(produceRequest.apiVersion());
            buf.writeShort(produceRequest.flag());
            buf.writeInt(dataSize);
            data.write(new ByteBufferAccessor(buf.nioBuffer(buf.writerIndex(), dataSize)), objectSerializationCache, produceRequest.apiVersion());
            buf.writerIndex(buf.writerIndex() + dataSize);
        }
        return buf;
    }

}
