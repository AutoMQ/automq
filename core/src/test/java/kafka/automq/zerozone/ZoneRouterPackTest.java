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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

@Timeout(60)
@Tag("S3Unit")
public class ZoneRouterPackTest {

    @Test
    public void testDataBlockCodec() {
        for (short version : new short[] {3, 7, 11}) {
            List<ZoneRouterProduceRequest> produceRequests = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                ProduceRequestData requestData = new ProduceRequestData();
                requestData.setTransactionalId("trans" + i);
                requestData.setAcks((short) -1);
                ProduceRequestData.TopicProduceDataCollection produceData = new ProduceRequestData.TopicProduceDataCollection();
                ProduceRequestData.PartitionProduceData partitionProduceData = new ProduceRequestData.PartitionProduceData();
                partitionProduceData.setIndex(i);
                partitionProduceData.setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(("simplerecord" + i).getBytes(StandardCharsets.UTF_8))));
                produceData.add(
                    new ProduceRequestData.TopicProduceData()
                        .setName("topic")
                        .setPartitionData(List.of(partitionProduceData)));
                requestData.setTopicData(produceData);
                produceRequests.add(new ZoneRouterProduceRequest(version, new ZoneRouterProduceRequest.Flag().internalTopicsAllowed(true).value(), requestData));
            }
            ByteBuf buf = ZoneRouterPackWriter.encodeDataBlock(produceRequests);
            List<ZoneRouterProduceRequest> decoded = ZoneRouterPackReader.decodeDataBlock(buf);
            Assertions.assertEquals(produceRequests, decoded);
        }
    }

}
