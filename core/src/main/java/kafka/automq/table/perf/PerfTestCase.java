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

package kafka.automq.table.perf;

import kafka.automq.table.process.DefaultRecordProcessor;
import kafka.automq.table.process.RecordProcessor;
import kafka.automq.table.process.convert.RawConverter;
import kafka.automq.table.process.transform.FlattenTransform;
import kafka.automq.table.worker.IcebergTableManager;
import kafka.automq.table.worker.IcebergWriter;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import com.google.common.collect.ImmutableMap;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
public abstract class PerfTestCase {
    // Cache InMemoryFileIO files map to avoid repeated reflection overhead
    private static final Map<String, byte[]> IN_MEMORY_FILES;

    static {
        Map<String, byte[]> files;
        try {
            Class<?> clazz = Class.forName("org.apache.iceberg.inmemory.InMemoryFileIO");
            java.lang.reflect.Field field = clazz.getDeclaredField("IN_MEMORY_FILES");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, byte[]> f = (Map<String, byte[]>) field.get(null);
            files = f;
        } catch (Exception e) {
            // Fallback to empty map; clear operation becomes a no-op
            files = new java.util.HashMap<>();
        }
        IN_MEMORY_FILES = files;
    }

    protected final DataType dataType;
    protected final String formatName;
    protected final int fieldCount;
    protected final int payloadCount;
    protected final int batchSizeBytes;

    public PerfTestCase(DataType dataType, String formatName, int fieldCount, int payloadCount, PerfConfig config) {
        this.dataType = dataType;
        this.formatName = formatName;
        this.fieldCount = fieldCount;
        this.payloadCount = payloadCount;
        this.batchSizeBytes = config.getBatchSizeBytes();
    }

    abstract PayloadManager getPayloadManager();

    public BenchmarkResult runBenchmark(long recordsCount) {
        try {
            // Simple warmup
            runTest(100_000);

            // Actual test
            long startTime = System.nanoTime();
            long fieldCount = runTest(recordsCount);
            long durationNs = System.nanoTime() - startTime;

            return BenchmarkResult.success(formatName, dataType.getName(), durationNs, recordsCount, fieldCount);
        } catch (Exception e) {
            return BenchmarkResult.failure(formatName, dataType.getName(), e.getMessage());
        }
    }

    private long runTest(long recordsCount) throws IOException {
        TableIdentifier tableId = TableIdentifier.parse("test.benchmark");
        WorkerConfig workerConfig = new BenchmarkWorkerConfig();
        IcebergWriter writer = null;
        int currentBatchSize = 0;
        final int batchSizeLimit = this.batchSizeBytes;
        long totalFieldCount = 0;

        for (long i = 0; i < recordsCount; i++) {
            if (writer == null) {
                InMemoryCatalog catalog = new InMemoryCatalog();
                catalog.initialize("test", ImmutableMap.of());
                RecordProcessor processor = new DefaultRecordProcessor("", RawConverter.INSTANCE, createConverter(), List.of(FlattenTransform.INSTANCE));
                writer = new IcebergWriter(new IcebergTableManager(catalog, tableId, workerConfig), processor, workerConfig);
                writer.setOffset(0, i);
            }

            byte[] payload = getPayloadManager().nextPayload();
            currentBatchSize += payload.length;

            writer.write(0, new SimpleRecord(i, payload));

            if (currentBatchSize > batchSizeLimit) {
                totalFieldCount += finalizeWriter(writer);
                writer = null;
                currentBatchSize = 0;
            }
        }

        if (writer != null) {
            totalFieldCount += finalizeWriter(writer);
        }
        return totalFieldCount;
    }

    public static void clearInMemoryFiles() {
        try {
            IN_MEMORY_FILES.clear();
        } catch (Exception ignored) {
            // Ignore cleanup failures
        }
    }

    private long finalizeWriter(IcebergWriter writer) throws IOException {
        try {
            writer.complete();
            return writer.topicMetric().fieldCount();
        } finally {
            clearInMemoryFiles();
        }
    }

    protected abstract byte[] generatePayload();
    protected abstract kafka.automq.table.process.Converter createConverter();

    static class SimpleRecord implements org.apache.kafka.common.record.Record {
        final long offset;
        final byte[] value;

        public SimpleRecord(long offset, byte[] value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int sequence() {
            return 0;
        }

        @Override
        public int sizeInBytes() {
            return value.length;
        }

        @Override
        public long timestamp() {
            return 0;
        }

        @Override
        public void ensureValid() {

        }

        @Override
        public int keySize() {
            return 0;
        }

        @Override
        public boolean hasKey() {
            return false;
        }

        @Override
        public ByteBuffer key() {
            return null;
        }

        @Override
        public int valueSize() {
            return 0;
        }

        @Override
        public boolean hasValue() {
            return true;
        }

        @Override
        public ByteBuffer value() {
            return ByteBuffer.wrap(value);
        }

        @Override
        public boolean hasMagic(byte b) {
            return false;
        }

        @Override
        public boolean isCompressed() {
            return false;
        }

        @Override
        public boolean hasTimestampType(TimestampType type) {
            return false;
        }

        @Override
        public Header[] headers() {
            return new Header[0];
        }
    }
}
