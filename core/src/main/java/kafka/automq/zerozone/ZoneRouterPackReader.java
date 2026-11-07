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

import com.automq.stream.s3.operator.ObjectStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

import static kafka.automq.zerozone.ZoneRouterPack.PRODUCE_DATA_BLOCK_MAGIC;
import static kafka.automq.zerozone.ZoneRouterPack.genObjectPath;

public class ZoneRouterPackReader {
    private final short bucketId;
    private final String path;
    private final ObjectStorage objectStorage;

    public ZoneRouterPackReader(int nodeId, short bucketId, long objectId, ObjectStorage objectStorage) {
        this.path = genObjectPath(nodeId, objectId);
        this.bucketId = bucketId;
        this.objectStorage = objectStorage;
    }

    public CompletableFuture<List<ZoneRouterProduceRequest>> readProduceRequests(Position position) {
        return objectStorage
            .rangeRead(new ObjectStorage.ReadOptions().bucket(bucketId), path, position.position(), position.position() + position.size())
            .thenApply(buf -> {
                try {
                    return ZoneRouterPackReader.decodeDataBlock(buf);
                } finally {
                    buf.release();
                }
            });
    }

    /**
     * Caution: ProduceRequestData$PartitionProduceData.records is a slice of buf.
     */
    static List<ZoneRouterProduceRequest> decodeDataBlock(ByteBuf buf) {
        byte magic = buf.readByte();
        if (magic != PRODUCE_DATA_BLOCK_MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        List<ZoneRouterProduceRequest> requests = new ArrayList<>();
        while (buf.readableBytes() > 0) {
            short apiVersion = buf.readShort();
            short flag = buf.readShort();
            int dataSize = buf.readInt();
            ByteBuf dataBuf = buf.slice(buf.readerIndex(), dataSize);
            ProduceRequestData produceRequestData = new ProduceRequestData();
            produceRequestData.read(new ByteBufferAccessor(dataBuf.nioBuffer()), apiVersion);
            buf.skipBytes(dataSize);
            buf.retain();
            requests.add(new ZoneRouterProduceRequest(apiVersion, flag, produceRequestData, buf::release));
        }
        return requests;
    }

}