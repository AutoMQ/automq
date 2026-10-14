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

package kafka.log.stream.s3.wal;

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Protects recovery and failover writers reopened from stored WAL URIs during a rolling upgrade. */
@Tag("S3Unit")
public class DefaultWalFactoryTest {
    /** Historical URIs default to legacy writes; explicit layout versions survive recovery and failover. */
    @ParameterizedTest
    @EnumSource(value = OpenMode.class, names = {"READ_WRITE", "FAILOVER"})
    public void testHistoricalURIRespectsLayoutVersion(OpenMode mode) {
        DefaultWalFactory factory = new DefaultWalFactory(1001, Map.of(), NetworkBandwidthLimiter.NOOP,
            NetworkBandwidthLimiter.NOOP);
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class,
            withSettings().defaultAnswer(RETURNS_SELF), (builder, context) -> when(builder.build()).thenReturn(mock(ObjectStorage.class)));
            MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
                (wal, context) -> configs.add((ObjectWALConfig) context.arguments().get(2)))) {
            WalFactory.BuildOptions options = WalFactory.BuildOptions.builder().nodeEpoch(42).openMode(mode).build();
            factory.build(IdURI.parse("7@s3://wal"), options);
            assertEquals(0, configs.get(0).version());
            factory.build(IdURI.parse("7@s3://wal?version=1"), options);
            assertEquals(1, configs.get(1).version());
            factory.build(IdURI.parse("7@s3://wal?version=0"), options);
            assertEquals(0, configs.get(2).version());
            assertEquals(3, builders.constructed().size());
            assertEquals(3, wals.constructed().size());
            configs.forEach(config -> assertEquals(mode, config.openMode()));
        }
    }

    /** An unsupported layout fails before allocating storage clients during startup or failover. */
    @Test
    public void testInvalidVersionDoesNotOpenStorage() {
        DefaultWalFactory factory = new DefaultWalFactory(1001, Map.of(), NetworkBandwidthLimiter.NOOP,
            NetworkBandwidthLimiter.NOOP);
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class)) {
            assertThrows(IllegalArgumentException.class, () -> factory.build(IdURI.parse("7@s3://wal?version=2"),
                WalFactory.BuildOptions.builder().nodeEpoch(42).openMode(OpenMode.FAILOVER).build()));
            assertEquals(0, builders.constructed().size());
        }
    }
}
