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

package kafka.automq.failover;

import kafka.automq.utils.JsonUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class FailoverContextTest {

    @Test
    public void testEncodeDecodeV1() {
        FailoverContext context = new FailoverContext(111, 333, 222, "kraftWalConfigs");
        String encoded = JsonUtils.encode(context);
        FailoverContext decoded = JsonUtils.decode(encoded, FailoverContext.class);
        assertEquals("{\"n\":111,\"t\":222,\"e\":333,\"c\":\"kraftWalConfigs\"}", encoded);
        assertEquals(context.getNodeId(), decoded.getNodeId());
        assertEquals(context.getNodeEpoch(), decoded.getNodeEpoch());
        assertEquals(context.getTarget(), decoded.getTarget());
        assertEquals(context.getKraftWalConfigs(), decoded.getKraftWalConfigs());
    }

}
