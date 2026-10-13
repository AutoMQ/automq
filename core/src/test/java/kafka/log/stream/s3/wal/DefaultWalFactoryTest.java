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

import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Protects recovery and failover writers reopened from stored WAL URIs during a rolling upgrade. */
@Tag("S3Unit")
public class DefaultWalFactoryTest {
    /** Stored URIs without a mode stay legacy before finalization and use sharding only after support is finalized. */
    @ParameterizedTest
    @EnumSource(value = OpenMode.class, names = {"READ_WRITE", "FAILOVER"})
    public void testHistoricalURIRespectsClusterCapability(OpenMode mode) {
        AtomicReference<AutoMQVersion> version = new AtomicReference<>(AutoMQVersion.V5);
        DefaultWalFactory factory = new DefaultWalFactory(1001, Map.of(), NetworkBandwidthLimiter.NOOP,
            NetworkBandwidthLimiter.NOOP, version::get);
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class,
            withSettings().defaultAnswer(RETURNS_SELF), (builder, context) -> when(builder.build()).thenReturn(mock(ObjectStorage.class)));
            MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
                (wal, context) -> configs.add((ObjectWALConfig) context.arguments().get(2)))) {
            WalFactory.BuildOptions options = WalFactory.BuildOptions.builder().nodeEpoch(42).openMode(mode).build();
            factory.build(IdURI.parse("7@s3://wal?shardCount=256"), options);
            assertEquals("r", configs.get(0).shardMode());
            version.set(AutoMQVersion.V6);
            factory.build(IdURI.parse("7@s3://wal?shardCount=256"), options);
            assertEquals("rw", configs.get(1).shardMode());
            factory.build(IdURI.parse("7@s3://wal?shardCount=256&shardMode=r"), options);
            assertEquals("r", configs.get(2).shardMode());
            assertEquals(3, builders.constructed().size());
            assertEquals(3, wals.constructed().size());
            configs.forEach(config -> {
                assertEquals(256, config.shardCount());
                assertEquals(mode, config.openMode());
            });
        }
    }
}
