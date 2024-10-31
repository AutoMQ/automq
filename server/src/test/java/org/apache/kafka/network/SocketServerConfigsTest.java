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
package org.apache.kafka.network;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SocketServerConfigsTest {
    @Test
    public void testDefaultNameToSecurityProto() {
        Map<ListenerName, SecurityProtocol> expected = new HashMap<>();
        expected.put(new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT);
        expected.put(new ListenerName("SSL"), SecurityProtocol.SSL);
        expected.put(new ListenerName("SASL_PLAINTEXT"), SecurityProtocol.SASL_PLAINTEXT);
        expected.put(new ListenerName("SASL_SSL"), SecurityProtocol.SASL_SSL);
        assertEquals(expected, SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO);
    }

    @Test
    public void testListenerListToEndPointsWithEmptyString() {
        assertEquals(Arrays.asList(),
            SocketServerConfigs.listenerListToEndPoints("",
                SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithBlankString() {
        assertEquals(Arrays.asList(),
            SocketServerConfigs.listenerListToEndPoints(" ",
                SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithOneEndpoint() {
        assertEquals(Arrays.asList(
            new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "example.com", 8080)),
                SocketServerConfigs.listenerListToEndPoints("PLAINTEXT://example.com:8080",
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    // Regression test for KAFKA-3719
    @Test
    public void testListenerListToEndPointsWithUnderscores() {
        assertEquals(Arrays.asList(
            new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "example.com", 8080),
            new Endpoint("SSL", SecurityProtocol.SSL, "local_host", 8081)),
                SocketServerConfigs.listenerListToEndPoints("PLAINTEXT://example.com:8080,SSL://local_host:8081",
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithWildcard() {
        assertEquals(Arrays.asList(
            new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, null, 8080)),
                SocketServerConfigs.listenerListToEndPoints("PLAINTEXT://:8080",
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithIpV6() {
        assertEquals(Arrays.asList(
            new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "::1", 9092)),
                SocketServerConfigs.listenerListToEndPoints("PLAINTEXT://[::1]:9092",
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testAnotherListenerListToEndPointsWithIpV6() {
        assertEquals(Arrays.asList(
            new Endpoint("SASL_SSL", SecurityProtocol.SASL_SSL, "fe80::b1da:69ca:57f7:63d8%3", 9092)),
                SocketServerConfigs.listenerListToEndPoints("SASL_SSL://[fe80::b1da:69ca:57f7:63d8%3]:9092",
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testAnotherListenerListToEndPointsWithNonDefaultProtoMap() {
        Map<ListenerName, SecurityProtocol> map = new HashMap<>();
        map.put(new ListenerName("CONTROLLER"), SecurityProtocol.PLAINTEXT);
        assertEquals(Arrays.asList(
            new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "example.com", 9093)),
                SocketServerConfigs.listenerListToEndPoints("CONTROLLER://example.com:9093",
                    map));
    }
}
