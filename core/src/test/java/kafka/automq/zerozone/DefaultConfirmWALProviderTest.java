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

package kafka.automq.zerozone;

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Protects shard configuration propagation to remote readers of the traditional WAL. */
@Tag("S3Unit")
public class DefaultConfirmWALProviderTest {
    /** Confirmation readers use the owner's configured shard count and bucket ID. */
    @Test
    public void testReadOnlyUsesWALLayout() throws Exception {
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class,
            withSettings().defaultAnswer(RETURNS_SELF), (builder, context) -> when(builder.build()).thenReturn(mock(ObjectStorage.class)));
            MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
                (wal, context) -> configs.add((ObjectWALConfig) context.arguments().get(2)))) {
            DefaultConfirmWALProvider provider = new DefaultConfirmWALProvider("cluster");
            provider.readOnly("7@s3://wal?shardCount=256&shardMode=rw", 1001).get(5, TimeUnit.SECONDS);
            assertEquals(1, builders.constructed().size());
            assertEquals(1, wals.constructed().size());
            ObjectWALConfig config = configs.get(0);
            assertEquals(256, config.shardCount());
            assertEquals("rw", config.shardMode());
            assertEquals(7, config.bucketId());
            assertEquals("cluster", config.clusterId());
            assertEquals(1001, config.nodeId());
            assertEquals(OpenMode.READ_ONLY, config.openMode());
        }
    }

    /** Invalid layouts fail before constructing an object storage client or scheduling a reader. */
    @Test
    public void testInvalidLayoutDoesNotOpenStorage() {
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class)) {
            DefaultConfirmWALProvider provider = new DefaultConfirmWALProvider("cluster");
            assertThrows(IllegalArgumentException.class,
                () -> provider.readOnly("7@s3://wal?shardCount=0", 1001));
            assertEquals(0, builders.constructed().size());
        }
    }
}
