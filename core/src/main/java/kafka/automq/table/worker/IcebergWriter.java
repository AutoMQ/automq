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

package kafka.automq.table.worker;

import kafka.automq.table.events.PartitionMetric;
import kafka.automq.table.events.TopicMetric;
import kafka.automq.table.transformer.Converter;
import kafka.automq.table.transformer.InvalidDataException;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.LogSuppressor;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class IcebergWriter implements Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergWriter.class);
    private static final int TARGET_FILE_SIZE = 64 * 1024 * 1024;
    private static final LogSuppressor INVALID_DATA_LOGGER = new LogSuppressor(LOGGER, 60000);
    private static final int WRITER_RESULT_SIZE_LIMIT = 5;
    final List<WriteResult> results = new ArrayList<>();
    private final TableIdentifier tableId;
    private final Converter converter;
    private final IcebergTableManager icebergTableManager;
    private final Map<Integer, OffsetRange> offsetRangeMap = new HashMap<>();
    private TaskWriter<Record> writer;
    private int recordCount = 0;
    private long dirtyBytes = 0;
    private long fieldCount = 0;
    private final Map<Integer, Metric> metrics = new HashMap<>();
    private Status status = Status.WRITABLE;
    private final WorkerConfig config;
    private final boolean deltaWrite;

    public IcebergWriter(IcebergTableManager icebergTableManager, Converter converter, WorkerConfig config) {
        this.tableId = icebergTableManager.tableId();
        this.icebergTableManager = icebergTableManager;
        this.converter = converter;
        this.config = config;
        this.deltaWrite = StringUtils.isNoneBlank(config.cdcField()) || config.upsertEnable();
    }

    @Override
    public void write(int partition, org.apache.kafka.common.record.Record kafkaRecord) throws IOException {
        if (status != Status.WRITABLE) {
            throw new IOException(String.format("The writer %s isn't in WRITABLE status, current status is %s", this, status));
        }
        try {
            write0(partition, kafkaRecord);
            recordCount++;
            dirtyBytes += kafkaRecord.sizeInBytes();
            offsetRangeMap.compute(partition, (k, v) -> {
                if (v == null) {
                    throw new IllegalArgumentException(String.format("The partition %s initial offset is not set", partition));
                }
                long recordOffset = kafkaRecord.offset();
                if (recordOffset < v.end) {
                    throw new IllegalArgumentException(String.format("The record offset[%s] is less than the end[%s]", recordOffset, v.end));
                }
                v.end = recordOffset + 1;
                return v;
            });
        } catch (InvalidDataException e) {
            INVALID_DATA_LOGGER.warn("[INVALID_DATA],{}", this, e);
        } catch (Throwable e) {
            LOGGER.error("[WRITE_FAIL],{}", this, e);
            status = Status.ERROR;
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return tableId.toString();
    }

    protected void write0(int partition,
        org.apache.kafka.common.record.Record kafkaRecord) throws IOException, InvalidDataException {
        long beforeFieldCount = converter.fieldCount();
        Record record = converter.convert(kafkaRecord);

        if (!converter.currentSchema().isTableSchemaUsed()) {
            //  compare table schema and evolution
            boolean schemaChanges = icebergTableManager.handleSchemaChangesWithFlush(
                record,
                this::flush
            );
            Table table = icebergTableManager.getTableOrCreate(record.struct().asSchema());
            converter.tableSchema(table.schema());
            if (schemaChanges) {
                beforeFieldCount = converter.fieldCount();
                record = converter.convert(kafkaRecord);
            }
        }
        long recordFieldCount = converter.fieldCount() - beforeFieldCount;
        recordMetric(partition, recordFieldCount, kafkaRecord.timestamp());

        TaskWriter<Record> writer = getWriter(record.struct());
        if (deltaWrite) {
            writer.write(new RecordWrapper(record, config.cdcField(), config.upsertEnable()));
        } else {
            writer.write(record);
        }
    }

    @Override
    public CompletableFuture<Void> flush(FlushMode flushMode, ExecutorService flushExecutor, Executor eventLoop) {
        switch (status) {
            case WRITABLE:
                status = Status.FLUSHING;
                break;
            case FLUSHING:
                return FutureUtil.failedFuture(new IOException("The writer is already flushing"));
            case COMPLETED:
                return CompletableFuture.completedFuture(null);
            case ERROR:
                return FutureUtil.failedFuture(errorStateException());
            default:
                return FutureUtil.failedFuture(errorStateException());
        }
        CompletableFuture<Void> cf = new CompletableFuture<>();
        flushExecutor.submit(() -> {
            try {
                TimerUtil timerUtil = new TimerUtil();
                boolean hasData = flush();
                if (flushMode == FlushMode.COMPLETE) {
                    complete();
                }
                if (hasData && LOGGER.isTraceEnabled()) {
                    LOGGER.trace("[TABLE_FLUSH],{},{}ms", this, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                }
                cf.complete(null);
            } catch (Throwable e) {
                LOGGER.error("[DATA_FILE_FLUSH_FAIL],{}", this, e);
                cf.completeExceptionally(e);
            }
        });
        return cf.thenAcceptAsync(nil -> {
            if (status == Status.FLUSHING) {
                // the status may become ERROR or COMPLETED
                status = Status.WRITABLE;
            }
        }, eventLoop);
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public List<WriteResult> complete() throws IOException {
        check();
        if (status == Status.COMPLETED) {
            return results;
        }
        flush();
        status = Status.COMPLETED;
        return results;
    }

    @Override
    public List<WriteResult> results() {
        if (status != Status.COMPLETED) {
            throw new IllegalStateException("The writer isn't completed, current status is " + status);
        }
        return results;
    }

    @Override
    public boolean isCompleted() {
        return status == Status.COMPLETED;
    }

    @Override
    public boolean isFull() {
        return results.size() >= WRITER_RESULT_SIZE_LIMIT;
    }

    @Override
    public Map<Integer, OffsetRange> getOffsets() {
        return offsetRangeMap;
    }

    @Override
    public OffsetRange getOffset(int partition) {
        return offsetRangeMap.get(partition);
    }

    @Override
    public void setOffset(int partition, long offset) {
        if (offsetRangeMap.containsKey(partition)) {
            throw new IllegalArgumentException(String.format("The partition %s initial offset is already set", partition));
        }
        offsetRangeMap.put(partition, new OffsetRange(offset));
        metrics.put(partition, new Metric());
    }

    @Override
    public void setEndOffset(int partition, long offset) {
        offsetRangeMap.compute(partition, (k, v) -> {
            if (v == null) {
                throw new IllegalArgumentException(String.format("The partition %s initial offset is not set", partition));
            }
            v.end = offset;
            return v;
        });
    }

    @Override
    public long dirtyBytes() {
        return dirtyBytes;
    }

    public void updateWatermark(int partition, long watermark) {
        recordMetric(partition, 0, watermark);
    }

    @Override
    public TopicMetric topicMetric() {
        return new TopicMetric(fieldCount);
    }

    @Override
    public Map<Integer, PartitionMetric> partitionMetrics() {
        return metrics.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> new PartitionMetric(e.getKey(), e.getValue().watermark)
        ));
    }

    @Override
    public int targetFileSize() {
        return TARGET_FILE_SIZE;
    }

    private void check() throws IOException {
        if (status == Status.ERROR) {
            throw errorStateException();
        }
    }

    private IOException errorStateException() {
        return new IOException(String.format("The writer %s is in error state", this));
    }

    private TaskWriter<Record> getWriter(Types.StructType prototype) {
        if (writer == null) {
            writer = writer(icebergTableManager.getTableOrCreate(prototype.asSchema()));
        }
        return writer;
    }

    private TaskWriter<Record> writer(Table table) {
        FileAppenderFactory<Record> appenderFactory;

        Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
        if (!config.idColumns().isEmpty()) {
            identifierFieldIds = config.idColumns().stream()
                .map(colName -> table.schema().findField(colName).fieldId())
                .collect(toSet());
        }
        // Use a consistent partition spec instead of retrieve from table in real times.
        PartitionSpec spec = icebergTableManager.spec();
        if (identifierFieldIds.isEmpty()) {
            appenderFactory =
                new GenericAppenderFactory(table.schema(), spec, null, null, null)
                    .setAll(table.properties());
        } else {
            appenderFactory =
                new GenericAppenderFactory(
                    table.schema(),
                    spec,
                    identifierFieldIds.stream().mapToInt(i -> i).toArray(),
                    TypeUtil.select(table.schema(), identifierFieldIds),
                    null)
                    .setAll(table.properties());
        }

        // (partition ID + task ID + operation ID) must be unique
        OutputFileFactory fileFactory =
            OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(spec)
                .operationId(UUID.randomUUID().toString())
                .format(FileFormat.PARQUET)
                .build();

        TaskWriter<Record> writer;
        if (spec.isUnpartitioned()) {
            if (!deltaWrite) {
                writer =
                    new UnpartitionedWriter<>(
                        spec, FileFormat.PARQUET, appenderFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
            } else {
                writer =
                    new UnpartitionedDeltaWriter(
                        spec,
                        FileFormat.PARQUET,
                        appenderFactory,
                        fileFactory,
                        table.io(),
                        TARGET_FILE_SIZE,
                        table.schema(),
                        identifierFieldIds);
            }
        } else {
            if (!deltaWrite) {
                writer =
                    new PartitionedWriter(
                        table.spec(),
                        FileFormat.PARQUET,
                        appenderFactory,
                        fileFactory,
                        table.io(),
                        TARGET_FILE_SIZE,
                        table.schema());
            } else {
                writer =
                    new PartitionedDeltaWriter(
                        table.spec(),
                        FileFormat.PARQUET,
                        appenderFactory,
                        fileFactory,
                        table.io(),
                        TARGET_FILE_SIZE,
                        table.schema(),
                        identifierFieldIds);
            }
        }
        return writer;
    }

    private boolean flush() throws IOException {
        if (status == Status.ERROR) {
            throw errorStateException();
        }
        try {
            if (writer == null) {
                return false;
            }
            if (recordCount == 0) {
                return false;
            }
            results.add(this.writer.complete());
            this.writer = null;
            recordCount = 0;
            dirtyBytes = 0;
            return true;
        } catch (Throwable e) {
            status = Status.ERROR;
            throw e;
        }
    }

    private void recordMetric(int partition, long fieldCount, long timestamp) {
        Metric metric = metrics.get(partition);
        this.fieldCount += fieldCount;
        if (metric.watermark < timestamp) {
            metric.watermark = timestamp;
        }
    }

    static class Metric {
        long watermark = -1L;
    }

    enum Status {
        WRITABLE,
        FLUSHING,
        COMPLETED,
        ERROR,
    }

}
