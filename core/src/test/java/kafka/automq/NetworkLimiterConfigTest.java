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

package kafka.automq;

import kafka.log.stream.s3.ConfigUtils;
import kafka.server.KafkaConfig;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.network.NetworkBandwidthLimiterType;
import com.automq.stream.s3.network.NetworkBandwidthMode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
class NetworkLimiterConfigTest {
    private Map<String, Object> properties() {
        return new HashMap<>(Map.of("process.roles", "controller", "node.id", "2",
            "controller.listener.names", "PLAINTEXT", "controller.quorum.voters", "2@localhost:9092"));
    }

    /** Broker defaults retain legacy limiting and the existing refill interval. */
    @Test
    void defaults() {
        Config config = ConfigUtils.to(new KafkaConfig(properties()));
        assertEquals(NetworkBandwidthLimiterType.LEGACY, config.networkLimiterType());
        assertEquals(10, config.refillPeriodMs());
        assertEquals(config.networkBaselineBandwidth(), config.networkBurstBandwidth());
        assertEquals(60000, config.networkMaxBurstWindowMs());
    }

    /** Explicit broker properties reach the stream configuration without losing burst or mode settings. */
    @Test
    void smoothProperties() {
        Map<String, Object> properties = properties();
        properties.put("s3.network.limiter.type", "smooth");
        properties.put("s3.network.bandwidth.mode", "shared");
        properties.put("s3.network.baseline.bandwidth", "104857600");
        properties.put("s3.network.burst.bandwidth", "209715200");
        properties.put("s3.network.max.burst.window.ms", "100");
        properties.put("s3.network.refill.period.ms", "1");
        Config config = ConfigUtils.to(new KafkaConfig(properties));
        assertEquals(NetworkBandwidthLimiterType.SMOOTH, config.networkLimiterType());
        assertEquals(NetworkBandwidthMode.SHARED, config.networkBandwidthMode());
        assertEquals(104857600, config.networkBaselineBandwidth());
        assertEquals(209715200, config.networkBurstBandwidth());
        assertEquals(100, config.networkMaxBurstWindowMs());
        assertEquals(1, config.refillPeriodMs());
    }

    /** Invalid implementation names fail broker configuration instead of silently choosing legacy. */
    @Test
    void invalidType() {
        Map<String, Object> properties = properties();
        properties.put("s3.network.limiter.type", "other");
        assertThrows(org.apache.kafka.common.config.ConfigException.class, () -> new KafkaConfig(properties));
    }
}
