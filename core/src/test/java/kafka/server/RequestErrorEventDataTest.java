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
package kafka.server;

import org.apache.kafka.clients.admin.RequestErrorEventData;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RequestErrorEventDataTest {

    @Test
    void roundTripSerialization() {
        RequestErrorEventData original = new RequestErrorEventData()
            .setApiKey(0)
            .setErrorCode(29)
            .setResource("my-topic")
            .setClientIps(List.of("10.0.0.1"))
            .setClientIds(List.of("producer-1"))
            .setRps(42.0);

        byte[] bytes = original.toByteArray();
        assertNotNull(bytes);

        RequestErrorEventData decoded = RequestErrorEventData.fromByteArray(bytes);
        assertEquals(0, decoded.apiKey());
        assertEquals(29, decoded.errorCode());
        assertEquals("my-topic", decoded.resource());
        assertEquals(List.of("10.0.0.1"), decoded.clientIps());
        assertEquals(List.of("producer-1"), decoded.clientIds());
        assertEquals(42.0, decoded.rps(), 0.001);
    }
}
