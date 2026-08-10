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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class ObjectWALService implements WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(ObjectWALService.class);

    protected ObjectStorage objectStorage;
    protected ObjectWALConfig config;
    private final boolean closeObjectStorageOnShutdown;
    private final AtomicBoolean shutdown = new AtomicBoolean();

    protected final Writer writer;
    protected final DefaultReader reader;

    public ObjectWALService(Time time, ObjectStorage objectStorage, ObjectWALConfig config) {
        this(time, objectStorage, config, false);
    }

    /**
     * Creates a WAL with an optional shutdown-owned object storage.
     *
     * @param time the time source
     * @param objectStorage the storage used by this WAL
     * @param config the WAL configuration
     * @param closeObjectStorageOnShutdown whether this WAL owns storage shutdown
     */
    public ObjectWALService(Time time, ObjectStorage objectStorage, ObjectWALConfig config,
        boolean closeObjectStorageOnShutdown) {
        this.objectStorage = objectStorage;
        this.config = config;
        this.closeObjectStorageOnShutdown = closeObjectStorageOnShutdown;
        if (config.openMode() == OpenMode.READ_WRITE || config.openMode() == OpenMode.FAILOVER) {
            this.writer = new DefaultWriter(time, objectStorage, config);
        } else {
            this.writer = new NoopWriter();
        }
        this.reader = new DefaultReader(objectStorage, config.clusterId(), config.nodeId(), config.type(), time);
    }

    @Override
    public WriteAheadLog start() throws IOException {
        log.info("Start S3 WAL.");
        writer.start();
        return this;
    }

    @Override
    public void shutdownGracefully() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutdown S3 WAL.");
        try {
            writer.close();
        } catch (RuntimeException | Error e) {
            closeObjectStorage();
            throw e;
        }
        closeObjectStorage();
    }

    private void closeObjectStorage() {
        if (closeObjectStorageOnShutdown) {
            FutureUtil.suppress(objectStorage::close, log);
        }
    }

    @Override
    public WALMetadata metadata() {
        return new WALMetadata(config.nodeId(), config.epoch());
    }

    @Override
    public String uri() {
        return config.uri();
    }

    @Override
    public CompletableFuture<AppendResult> append(TraceContext context,
        StreamRecordBatch streamRecordBatch) throws OverCapacityException {
        return writer.append(streamRecordBatch);
    }

    @Override
    public CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset) {
        return reader.get(recordOffset);
    }

    @Override
    public CompletableFuture<List<StreamRecordBatch>> get(RecordOffset startOffset, RecordOffset endOffset) {
        return reader.get(startOffset, endOffset);
    }

    @Override
    public RecordOffset confirmOffset() {
        return writer.confirmOffset();
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return writer.recover();
    }

    @Override
    public CompletableFuture<Void> reset() {
        log.info("Reset S3 WAL");
        try {
            return writer.reset();
        } catch (Throwable e) {
            log.error("Reset S3 WAL failed, due to unrecoverable exception.", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> trim(RecordOffset offset) {
        log.info("Trim S3 WAL to offset: {}", offset);
        try {
            return writer.trim(offset);
        } catch (Throwable e) {
            log.error("Trim S3 WAL failed, due to unrecoverable exception.", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public String toString() {
        return String.format("ObjectWALService{%s@%s-%s-%s}", config.bucketId(), config.nodeId(), config.epoch(), config.type());
    }
}
