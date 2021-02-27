/*
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

package org.apache.kafka.shell;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.raft.metadata.MetadataRecordSerde;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * Reads Kafka metadata snapshots.
 */
public final class SnapshotFileReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SnapshotFileReader.class);

    private final String snapshotPath;
    private final MetaLogListener listener;
    private final KafkaEventQueue queue;
    private final CompletableFuture<Void> caughtUpFuture;
    private FileRecords fileRecords;
    private Iterator<FileChannelRecordBatch> batchIterator;
    private final MetadataRecordSerde serde = new MetadataRecordSerde();

    public SnapshotFileReader(String snapshotPath, MetaLogListener listener) {
        this.snapshotPath = snapshotPath;
        this.listener = listener;
        this.queue = new KafkaEventQueue(Time.SYSTEM,
            new LogContext("[snapshotReaderQueue] "), "snapshotReaderQueue_");
        this.caughtUpFuture = new CompletableFuture<>();
    }

    public void startup() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.append(new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                fileRecords = FileRecords.open(new File(snapshotPath), false);
                batchIterator = fileRecords.batches().iterator();
                scheduleHandleNextBatch();
                future.complete(null);
            }

            @Override
            public void handleException(Throwable e) {
                future.completeExceptionally(e);
                beginShutdown("startup error");
            }
        });
        future.get();
    }

    private void handleNextBatch() {
        if (!batchIterator.hasNext()) {
            beginShutdown("done");
            return;
        }
        FileChannelRecordBatch batch = batchIterator.next();
        if (batch.isControlBatch()) {
            handleControlBatch(batch);
        } else {
            handleMetadataBatch(batch);
        }
        scheduleHandleNextBatch();
    }

    private void scheduleHandleNextBatch() {
        queue.append(new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                handleNextBatch();
            }

            @Override
            public void handleException(Throwable e) {
                log.error("Unexpected error while handling a batch of events", e);
                beginShutdown("handleBatch error");
            }
        });
    }

    private void handleControlBatch(FileChannelRecordBatch batch) {
        for (Iterator<Record> iter = batch.iterator(); iter.hasNext(); ) {
            Record record = iter.next();
            try {
                short typeId = ControlRecordType.parseTypeId(record.key());
                ControlRecordType type = ControlRecordType.fromTypeId(typeId);
                switch (type) {
                    case LEADER_CHANGE:
                        LeaderChangeMessage message = new LeaderChangeMessage();
                        message.read(new ByteBufferAccessor(record.value()), (short) 0);
                        listener.handleNewLeader(new MetaLogLeader(message.leaderId(),
                            batch.partitionLeaderEpoch()));
                        break;
                    default:
                        log.error("Ignoring control record with type {} at offset {}",
                            type, record.offset());
                }
            } catch (Throwable e) {
                log.error("unable to read control record at offset {}", record.offset(), e);
            }
        }
    }

    private void handleMetadataBatch(FileChannelRecordBatch batch) {
        List<ApiMessage> messages = new ArrayList<>();
        for (Iterator<Record> iter = batch.iterator(); iter.hasNext(); ) {
            Record record = iter.next();
            ByteBufferAccessor accessor = new ByteBufferAccessor(record.value());
            try {
                ApiMessageAndVersion messageAndVersion = serde.read(accessor, record.valueSize());
                messages.add(messageAndVersion.message());
            } catch (Throwable e) {
                log.error("unable to read metadata record at offset {}", record.offset(), e);
            }
        }
        listener.handleCommits(batch.lastOffset(), messages);
    }

    public void beginShutdown(String reason) {
        if (reason.equals("done")) {
            caughtUpFuture.complete(null);
        } else {
            caughtUpFuture.completeExceptionally(new RuntimeException(reason));
        }
        queue.beginShutdown(reason, new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                listener.beginShutdown();
                if (fileRecords != null) {
                    fileRecords.close();
                    fileRecords = null;
                }
                batchIterator = null;
            }

            @Override
            public void handleException(Throwable e) {
                log.error("shutdown error", e);
            }
        });
    }

    @Override
    public void close() throws Exception {
        beginShutdown("closing");
        queue.close();
    }

    public CompletableFuture<Void> caughtUpFuture() {
        return caughtUpFuture;
    }
}
