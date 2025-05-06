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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class ZoneRouterResponseCodecTest {

    @Test
    public void testCodec() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> map = new HashMap<>();
        map.put(new TopicPartition("test", 1), new ProduceResponse.PartitionResponse(Errors.UNKNOWN_LEADER_EPOCH));
        ProduceResponseData data = new ProduceResponse(map).data();
        ProduceResponseData decoded = ZoneRouterResponseCodec.decode(ZoneRouterResponseCodec.encode(data));
        assertEquals(data, decoded);
    }

}
