/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.operator.BucketURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Protects the broker startup boundary before object WALs can write incompatible keys. */
@Tag("S3Unit")
public class WALShardingConfigValidatorTest {
    /** Every older cluster level requires explicit legacy writes on both traditional and router WALs. */
    @ParameterizedTest
    @EnumSource(value = AutoMQVersion.class, names = "V6", mode = EnumSource.Mode.EXCLUDE)
    public void testOldClusterRequiresLegacyWrites(AutoMQVersion version) {
        for (String suffix : List.of("", "?shardMode=rw")) {
            assertThrows(ConfigException.class, () -> WALShardingConfigValidator.validate(
                config("0@s3://wal" + suffix, List.of()), version));
            assertThrows(ConfigException.class, () -> WALShardingConfigValidator.validate(
                config("0@s3://wal?shardMode=r", List.of(BucketURI.parse("1@s3://router" + suffix))), version));
        }
        assertDoesNotThrow(() -> WALShardingConfigValidator.validate(config("0@s3://wal?shardMode=r",
            List.of(BucketURI.parse("1@s3://router?shardMode=r"))), version));
    }

    /** Once reader support is finalized, the default sharded mode can start without URI overrides. */
    @Test
    public void testNewClusterAllowsDefaults() {
        assertDoesNotThrow(() -> WALShardingConfigValidator.validate(config("0@s3://wal",
            List.of(BucketURI.parse("1@s3://router"))), AutoMQVersion.V6));
    }

    /** Every writable router bucket is checked, while read-only buckets and local WALs are exempt. */
    @Test
    public void testReadOnlyAndLocalWALs() {
        List<BucketURI> buckets = List.of(BucketURI.parse("1@s3://old?mode=r"),
            BucketURI.parse("2@s3://active?shardMode=r"));
        assertDoesNotThrow(() -> WALShardingConfigValidator.validate(config("0@file:///tmp/wal", buckets), AutoMQVersion.V5));
        assertDoesNotThrow(() -> WALShardingConfigValidator.validate(config(null, buckets), AutoMQVersion.V5));
        assertThrows(ConfigException.class, () -> WALShardingConfigValidator.validate(config("0@file:///tmp/wal",
            List.of(buckets.get(1), BucketURI.parse("3@s3://new"))), AutoMQVersion.V5));
    }

    /** Startup errors explain the migration step without disclosing credentials from the URI. */
    @Test
    public void testActionableErrorWithoutCredentials() {
        ConfigException exception = assertThrows(ConfigException.class, () -> WALShardingConfigValidator.validate(
            config("0@s3://wal?secretKey=secret", List.of()), AutoMQVersion.V5));
        assertTrue(exception.getMessage().contains("s3.wal.path"));
        assertTrue(exception.getMessage().contains("shardMode=r"));
        assertTrue(exception.getMessage().contains("automq.version=7"));
        assertFalse(exception.getMessage().contains("secret"));
    }

    private static AutoMQConfig config(String wal, List<BucketURI> buckets) {
        AutoMQConfig config = mock(AutoMQConfig.class);
        when(config.walConfig()).thenReturn(wal);
        when(config.zoneRouterChannels()).thenReturn(Optional.of(buckets));
        return config;
    }
}
