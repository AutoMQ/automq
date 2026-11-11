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

import com.automq.stream.s3.cache.RouterWALReadCache;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Verifies the configuration passed to router WALs without starting storage or background writers.
 */
@Tag("S3Unit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class DefaultRouterChannelProviderTest {
    /**
     * Explicit URI settings reach both the local writer and remote readers, including the shard layout.
     */
    @Test
    public void testReadWriteChannelUsesURIOptions() {
        BucketURI bucket = BucketURI.parse("7@s3://router/prefix?region=us-west-2&endpoint=https://example.com"
            + "&batchInterval=100&maxBytesInBatch=16777216&maxInflightUploadCount=100"
            + "&maxUnflushedBytes=536870912&readaheadDataSize=2097152&secretKey=test-secret&version=1");
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
        assertEquals(S3RequestSource.ROUTER_WAL, writer.source());
        assertEquals(OpenMode.READ_WRITE, writer.openMode());
        assertEquals("test-secret", IdURI.parse(writer.uri()).extensionString("secretKey"));
        assertFalse(writer.toString().contains("test-secret"));
        assertEquals(1, writer.version());

        ObjectWALConfig reader = configs.get(1);
        assertEquals(OpenMode.READ_ONLY, reader.openMode());
        assertEquals(S3RequestSource.ROUTER_WAL, reader.source());
        assertEquals(43, reader.nodeId());
        assertEquals(writer.uri(), reader.uri());
        assertEquals(1, reader.version());
        assertEquals(writer.readaheadDataSize(), reader.readaheadDataSize());
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
            List.of(BucketURI.parse("7@s3://router?batchInterval=100&batchInterval=200")), "cluster");
        try (MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class)) {
            assertThrows(IllegalArgumentException.class, provider::channel);
            assertEquals(0, wals.constructed().size());
        }
    }

    /** Every bucket gets a writer and remote reader, and URI options are retained for each writer. */
    @Test
    public void testMultipleBuckets() {
        List<BucketURI> buckets = List.of(BucketURI.parse("0@s3://a?batchInterval=100&version=1"),
            BucketURI.parse("1@s3://b?batchInterval=200&mode=r&version=0"));
        DefaultRouterChannelProvider provider = spy(new DefaultRouterChannelProvider(42, 123, buckets, "cluster"));
        buckets.forEach(bucket -> doReturn(mock(ObjectStorage.class)).when(provider).objectStorage(bucket));
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
            (wal, context) -> {
                configs.add((ObjectWALConfig) context.arguments().get(2));
                assertEquals(true, ((ObjectWALConfig) context.arguments().get(2)).manualMode());
            })) {
            provider.channel();
            provider.readOnlyChannel(43);
            assertEquals(4, wals.constructed().size());
            configs.forEach(config -> assertEquals(S3RequestSource.ROUTER_WAL, config.source()));
            assertEquals(100, configs.get(0).batchInterval());
            assertEquals(200, configs.get(1).batchInterval());
            assertEquals(0, configs.get(0).bucketId());
            assertEquals(1, configs.get(1).bucketId());
            assertEquals(OpenMode.READ_ONLY, configs.get(2).openMode());
            assertEquals(OpenMode.READ_ONLY, configs.get(3).openMode());
            assertEquals(0, configs.get(2).bucketId());
            assertEquals(1, configs.get(3).bucketId());
            assertEquals(1, configs.get(0).version());
            assertEquals(0, configs.get(1).version());
            assertEquals(OpenMode.READ_ONLY, configs.get(1).openMode());
            assertEquals(1, configs.get(2).version());
            assertEquals(0, configs.get(3).version());
            provider.close();
        }
    }

    /** The RouterWAL override reaches shared S3 clients, while other clients keep the default. */
    @Test
    public void testReadMergeIntervalScope() {
        String property = "automq.router.wal.read.merge.interval.ms";
        String previous = System.getProperty(property);
        List<Integer> intervals = new ArrayList<>();
        BucketURI bucket = BucketURI.parse("7@s3://router?region=us-west-2");
        try (MockedConstruction<AwsObjectStorage> storages = mockConstruction(AwsObjectStorage.class,
            (storage, context) -> intervals.add((Integer) context.arguments().get(7)))) {
            System.setProperty(property, "20");
            DefaultRouterChannelProvider provider = new DefaultRouterChannelProvider(42, 123, List.of(bucket), "cluster");
            assertSame(provider.objectStorage(bucket), provider.objectStorage(bucket));
            ObjectStorageFactory.instance().builder(bucket).build();
            assertEquals(List.of(20, 5), intervals);
            assertEquals(2, storages.constructed().size());
            provider.close();
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    /** Invalid or nonpositive values fail during provider initialization. */
    @Test
    public void testInvalidReadMergeInterval() {
        String property = "automq.router.wal.read.merge.interval.ms";
        String previous = System.getProperty(property);
        try {
            for (String value : List.of("", "0", "-1", "+1", "1.5", "2147483648")) {
                System.setProperty(property, value);
                assertThrows(IllegalArgumentException.class,
                    () -> new DefaultRouterChannelProvider(42, 123, List.of(), "cluster"));
            }
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    /** An absent cache setting preserves the uncached path for local and remote WAL readers. */
    @Test
    public void testReadCacheDisabledByDefault() {
        String property = "automq.router.wal.read.cache.size.bytes";
        String previous = System.clearProperty(property);
        BucketURI bucket = BucketURI.parse("0@s3://router");
        try (MockedConstruction<RouterWALReadCache> caches = mockConstruction(RouterWALReadCache.class);
            MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
                (wal, context) -> assertNull(context.arguments().get(4)))) {
            DefaultRouterChannelProvider provider = spy(new DefaultRouterChannelProvider(42, 123, List.of(bucket), "cluster"));
            doReturn(mock(ObjectStorage.class)).when(provider).objectStorage(bucket);
            try {
                provider.channel();
                provider.readOnlyChannel(43);
                assertEquals(2, wals.constructed().size());
                assertEquals(0, caches.constructed().size());
            } finally {
                provider.close();
            }
        } finally {
            restoreProperty(property, previous);
        }
    }

    /** All buckets and reader channels share one budget, and provider shutdown closes it once. */
    @Test
    public void testSharedReadCacheLifecycle() {
        String property = "automq.router.wal.read.cache.size.bytes";
        String previous = System.getProperty(property);
        List<BucketURI> buckets = List.of(BucketURI.parse("0@s3://a"), BucketURI.parse("1@s3://b"));
        long budget = 1024 * 1024 * 1024L;
        try (MockedConstruction<RouterWALReadCache> caches = mockConstruction(RouterWALReadCache.class,
            (cache, context) -> assertEquals(budget, context.arguments().get(0)));
            MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
                (wal, context) -> assertSame(caches.constructed().get(0), context.arguments().get(4)))) {
            System.setProperty(property, Long.toString(budget));
            DefaultRouterChannelProvider provider = spy(new DefaultRouterChannelProvider(42, 123, buckets, "cluster"));
            buckets.forEach(bucket -> doReturn(mock(ObjectStorage.class)).when(provider).objectStorage(bucket));
            try {
                provider.channel();
                provider.readOnlyChannel(43);
                provider.readOnlyChannel(44);
                assertEquals(1, caches.constructed().size());
                assertEquals(6, wals.constructed().size());
            } finally {
                provider.close();
            }
            verify(caches.constructed().get(0)).close();
        } finally {
            restoreProperty(property, previous);
        }
    }

    /** Progressive delivery is an independent opt-in on the shared cache, with strict configuration validation. */
    @Test
    public void testProgressiveReadCacheSetting() {
        String sizeProperty = "automq.router.wal.read.cache.size.bytes";
        String progressProperty = "automq.router.wal.read.cache.progressive.enabled";
        String previousSize = System.getProperty(sizeProperty);
        String previousProgress = System.getProperty(progressProperty);
        System.setProperty(sizeProperty, Long.toString(RouterWALReadCache.BLOCK_SIZE));
        try {
            for (boolean enabled : new boolean[] {false, true}) {
                if (enabled) {
                    System.setProperty(progressProperty, "true");
                } else {
                    System.clearProperty(progressProperty);
                }
                try (MockedConstruction<RouterWALReadCache> caches = mockConstruction(RouterWALReadCache.class,
                    (cache, context) -> assertEquals(enabled, context.arguments().get(1)))) {
                    DefaultRouterChannelProvider provider = new DefaultRouterChannelProvider(42, 123, List.of(), "cluster");
                    provider.close();
                    assertEquals(1, caches.constructed().size());
                }
            }
            System.setProperty(progressProperty, "yes");
            assertThrows(IllegalArgumentException.class,
                () -> new DefaultRouterChannelProvider(42, 123, List.of(), "cluster"));
        } finally {
            restoreProperty(sizeProperty, previousSize);
            restoreProperty(progressProperty, previousProgress);
        }
    }

    /** Malformed budgets or budgets smaller than a block fail before any storage is opened. */
    @Test
    public void testInvalidReadCacheSize() {
        String property = "automq.router.wal.read.cache.size.bytes";
        String previous = System.getProperty(property);
        try {
            for (String value : List.of("", "-1", "+1", "1.5", "4194304", "8388607", "9223372036854775808")) {
                System.setProperty(property, value);
                assertThrows(IllegalArgumentException.class,
                    () -> new DefaultRouterChannelProvider(42, 123, List.of(), "cluster"));
            }
        } finally {
            restoreProperty(property, previous);
        }
    }

    /** The minimum enabled budget is exactly one 8 MiB block. */
    @Test
    public void testMinimumReadCacheSize() {
        String property = "automq.router.wal.read.cache.size.bytes";
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "8388608");
            DefaultRouterChannelProvider provider = new DefaultRouterChannelProvider(42, 123, List.of(), "cluster");
            try {
                assertEquals(8 * 1024 * 1024, RouterWALReadCache.BLOCK_SIZE);
            } finally {
                provider.close();
            }
        } finally {
            restoreProperty(property, previous);
        }
    }

    private static void restoreProperty(String property, String previous) {
        if (previous == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, previous);
        }
    }

    private static List<ObjectWALConfig> channelConfigs(BucketURI bucket) {
        DefaultRouterChannelProvider provider = spy(new DefaultRouterChannelProvider(42, 123, List.of(bucket), "cluster"));
        doReturn(mock(ObjectStorage.class)).when(provider).objectStorage(bucket);
        List<ObjectWALConfig> configs = new ArrayList<>();
        try (MockedConstruction<ObjectWALService> wals = mockConstruction(ObjectWALService.class,
            (wal, context) -> configs.add((ObjectWALConfig) context.arguments().get(2)))) {
            RouterChannel writer = provider.channel();
            assertSame(writer, provider.channel());
            assertSame(writer, provider.readOnlyChannel(42));
            RouterChannel reader = provider.readOnlyChannel(43);
            assertSame(reader, provider.readOnlyChannel(43));
            assertEquals(2, wals.constructed().size());
        }
        return configs;
    }

    private static void assertDefaults(ObjectWALConfig config) {
        assertEquals(250, config.batchInterval());
        assertEquals(8 * 1024 * 1024L, config.maxBytesInBatch());
        assertEquals(50, config.maxInflightUploadCount());
        assertEquals(1024 * 1024 * 1024L, config.maxUnflushedBytes());
        assertEquals(100 * 1024 * 1024, config.readaheadDataSize());
        assertEquals(0, config.version());
    }
}
