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

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;

/**
 * Verifies the configuration passed to router WALs without starting storage or background writers.
 */
@Tag("S3Unit")
public class DefaultRouterChannelProviderTest {
    /**
     * Explicit URI settings reach the local writer while remote readers keep their existing configuration.
     */
    @Test
    public void testReadWriteChannelUsesURIOptions() {
        BucketURI bucket = BucketURI.parse("7@s3://router/prefix?region=us-west-2&endpoint=https://example.com"
            + "&batchInterval=100&maxBytesInBatch=16777216&maxInflightUploadCount=100"
            + "&maxUnflushedBytes=536870912&readaheadDataSize=2097152&secretKey=test-secret");
        List<ObjectWALConfig> configs = channelConfigs(bucket);
        ObjectWALConfig writer = configs.get(0);

        assertEquals(100, writer.batchInterval());
        assertEquals(16 * 1024 * 1024L, writer.maxBytesInBatch());
        assertEquals(100, writer.maxInflightUploadCount());
        assertEquals(512 * 1024 * 1024L, writer.maxUnflushedBytes());
        assertEquals(2 * 1024 * 1024, writer.readaheadDataSize());
        assertEquals(7, writer.bucketId());
        assertEquals(42, writer.nodeId());
        assertEquals(123L, writer.epoch());
        assertEquals("cluster", writer.clusterId());
        assertEquals("rc", writer.type());
        assertEquals(OpenMode.READ_WRITE, writer.openMode());
        assertEquals("test-secret", IdURI.parse(writer.uri()).extensionString("secretKey"));
        assertFalse(writer.toString().contains("test-secret"));

        ObjectWALConfig reader = configs.get(1);
        assertEquals(OpenMode.READ_ONLY, reader.openMode());
        assertEquals(43, reader.nodeId());
        assertEquals("", reader.uri());
        assertDefaults(reader);
    }

    /**
     * Missing tuning options retain the existing writer defaults, including the batch and upload limits.
     */
    @Test
    public void testReadWriteChannelKeepsDefaults() {
        assertDefaults(channelConfigs(BucketURI.parse("7@s3://router?region=us-west-2")).get(0));
    }

    /**
     * Ambiguous tuning values fail through the shared WAL parser before a writer is constructed.
     */
    @Test
    public void testDuplicateTuningOptionFailsBeforeOpeningStorage() {
        DefaultRouterChannelProvider provider = new DefaultRouterChannelProvider(42, 123,
            BucketURI.parse("7@s3://router?batchInterval=100&batchInterval=200"), "cluster");
        try (MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class)) {
            assertThrows(IllegalArgumentException.class, provider::channel);
            assertEquals(0, wals.constructed().size());
        }
    }

    private static List<ObjectWALConfig> channelConfigs(BucketURI bucket) {
        DefaultRouterChannelProvider provider = spy(new DefaultRouterChannelProvider(42, 123, bucket, "cluster"));
        doReturn(mock(ObjectStorage.class)).when(provider).objectStorage();
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
            (wal, context) -> configs.add((ObjectWALConfig) context.arguments().get(2)));
            MockedConstruction<ObjectRouterChannel> channels = mockConstruction(ObjectRouterChannel.class)) {
            RouterChannel writer = provider.channel();
            assertSame(writer, provider.channel());
            assertSame(writer, provider.readOnlyChannel(42));
            RouterChannel reader = provider.readOnlyChannel(43);
            assertSame(reader, provider.readOnlyChannel(43));
            assertEquals(2, wals.constructed().size());
            assertEquals(2, channels.constructed().size());
        }
        return configs;
    }

    private static void assertDefaults(ObjectWALConfig config) {
        assertEquals(250, config.batchInterval());
        assertEquals(8 * 1024 * 1024L, config.maxBytesInBatch());
        assertEquals(50, config.maxInflightUploadCount());
        assertEquals(1024 * 1024 * 1024L, config.maxUnflushedBytes());
        assertEquals(100 * 1024 * 1024, config.readaheadDataSize());
    }
}
