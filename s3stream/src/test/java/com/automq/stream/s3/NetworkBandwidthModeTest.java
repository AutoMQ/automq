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

package com.automq.stream.s3;

import com.automq.stream.s3.network.NetworkBandwidthMode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
public class NetworkBandwidthModeTest {

    /**
     * Given a config string, when parsing network bandwidth mode, then only supported mode names are accepted.
     */
    @Test
    public void testParseNetworkBandwidthMode() {
        assertEquals(NetworkBandwidthMode.SEPARATE, NetworkBandwidthMode.parse("separate"));
        assertEquals(NetworkBandwidthMode.SHARED, NetworkBandwidthMode.parse("shared"));
        assertEquals(NetworkBandwidthMode.SHARED, NetworkBandwidthMode.parse(" SHARED "));
        assertThrows(IllegalArgumentException.class, () -> NetworkBandwidthMode.parse(null));
        assertThrows(IllegalArgumentException.class, () -> NetworkBandwidthMode.parse("mixed"));
    }
}
