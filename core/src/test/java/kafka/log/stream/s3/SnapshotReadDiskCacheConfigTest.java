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

package kafka.log.stream.s3;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.cache.AsyncStreamRecordBatchDiskCache;
import com.automq.stream.s3.compact.CompactionManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Verifies disabled defaults, admission bounds and broker-to-storage configuration mapping. */
@Tag("S3Unit")
public class SnapshotReadDiskCacheConfigTest {
    @TempDir
    Path directory;

    /** No disk capacity or path is selected until the deployment config supplies them. */
    @Test
    public void testDisabledDefaults() {
        ConfigDef definition = new ConfigDef();
        AutoMQConfig.define(definition);
        Map<String, Object> defaults = definition.defaultValues();
        assertEquals(0L, defaults.get(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_SIZE_CONFIG));
        assertEquals("", defaults.get(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_PATH_CONFIG));
        assertEquals(100L, defaults.get(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_READ_TIMEOUT_MS_CONFIG));
        assertEquals(0L, new Config().snapshotReadCacheDiskSize());
    }

    /** Reject nonpositive worker, queue, index, byte-budget and deadline settings before startup. */
    @Test
    public void testInvalidBounds() {
        ConfigDef definition = new ConfigDef();
        AutoMQConfig.define(definition);
        for (String key : java.util.List.of(
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_MAX_ENTRIES_CONFIG,
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_THREADS_CONFIG,
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_QUEUE_CAPACITY_CONFIG,
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_MAX_PENDING_BYTES_CONFIG,
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_READ_TIMEOUT_MS_CONFIG)) {
            assertThrows(ConfigException.class, () -> definition.parse(Map.of(key, 0)));
        }
        assertThrows(ConfigException.class, () -> definition.parse(Map.of(
            AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_SIZE_CONFIG, -1)));
    }

    /** All configured disk limits reach the storage client without implicit capacity selection. */
    @Test
    public void testBrokerMapping() {
        Properties properties = kafka.utils.TestUtils.createDummyBrokerConfig();
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_PATH_CONFIG, "/tmp/snapshot-cache-test");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_SIZE_CONFIG, "8192");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_MAX_ENTRIES_CONFIG, "17");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_THREADS_CONFIG, "3");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_QUEUE_CAPACITY_CONFIG, "9");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_MAX_PENDING_BYTES_CONFIG, "65536");
        properties.put(AutoMQConfig.S3_SNAPSHOT_READ_CACHE_DISK_READ_TIMEOUT_MS_CONFIG, "25");
        Config config = ConfigUtils.to(KafkaConfig.fromProps(properties));
        assertEquals("/tmp/snapshot-cache-test", config.snapshotReadCacheDiskPath());
        assertEquals(8192, config.snapshotReadCacheDiskSize());
        assertEquals(17, config.snapshotReadCacheDiskMaxEntries());
        assertEquals(3, config.snapshotReadCacheDiskThreads());
        assertEquals(9, config.snapshotReadCacheDiskQueueCapacity());
        assertEquals(65536, config.snapshotReadCacheDiskMaxPendingBytes());
        assertEquals(25, config.snapshotReadCacheDiskReadTimeoutMs());
    }

    /** Disabled caches never touch a file; explicit activation creates the configured disposable file. */
    @Test
    public void testBackendActivation() throws Exception {
        Path path = directory.resolve("snapshot-cache");
        Config config = new Config().snapshotReadEnable(true).snapshotReadCacheDiskPath(path.toString());
        DefaultS3Client client = new DefaultS3Client(null, config);
        assertNull(client.newSnapshotDiskCache());
        assertFalse(Files.exists(path));
        config.snapshotReadCacheDiskSize(8192).snapshotReadEnable(false);
        assertNull(client.newSnapshotDiskCache());
        assertFalse(Files.exists(path));
        config.snapshotReadEnable(true);
        try (AsyncStreamRecordBatchDiskCache cache = client.newSnapshotDiskCache()) {
            assertNotNull(cache);
            assertTrue(Files.exists(path));
            assertEquals(8192, Files.size(path));
        }
    }

    /** Missing paths and disk initialization failures retain the RAM/block-cache read behavior. */
    @Test
    public void testInitializationFallback() {
        Config config = new Config().snapshotReadEnable(true).snapshotReadCacheDiskSize(8192);
        DefaultS3Client client = new DefaultS3Client(null, config);
        assertNull(client.newSnapshotDiskCache());
        config.snapshotReadCacheDiskPath(directory.resolve("missing-parent/cache").toString());
        assertNull(client.newSnapshotDiskCache());
    }

    /** Each shutdown stage closes the owned backend even when it fails before normal completion. */
    @ParameterizedTest
    @ValueSource(strings = {"compaction", "streams", "storage"})
    public void testShutdownFailureClosesBackend(String stage) throws Exception {
        DefaultS3Client client = new DefaultS3Client(null, new Config());
        client.compactionManager = mock(CompactionManager.class);
        client.streamClient = mock(S3StreamClient.class);
        client.storage = mock(S3Storage.class);
        AsyncStreamRecordBatchDiskCache backend = mock(AsyncStreamRecordBatchDiskCache.class);
        Field field = DefaultS3Client.class.getDeclaredField("snapshotDiskCache");
        field.setAccessible(true);
        field.set(client, backend);
        RuntimeException failure = new RuntimeException("shutdown failed");
        switch (stage) {
            case "compaction":
                doThrow(failure).when(client.compactionManager).shutdown();
                break;
            case "streams":
                doThrow(failure).when(client.streamClient).shutdown();
                break;
            default:
                doThrow(failure).when(client.storage).shutdown();
        }
        assertEquals(failure, assertThrows(RuntimeException.class, client::shutdown));
        verify(backend).close();
        assertNull(field.get(client));
    }

}
