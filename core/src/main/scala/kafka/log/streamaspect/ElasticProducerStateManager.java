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

package kafka.log.streamaspect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.CorruptSnapshotException;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.SnapshotFile;
import org.apache.kafka.storage.internals.log.VerificationStateEntry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

public class ElasticProducerStateManager extends ProducerStateManager {
    public static final long AWAIT_SEQ_ZERO_TIMEOUT = 120000L;
    private final PersistSnapshots persistSnapshots;
    private final long createTimestamp;

    public ElasticProducerStateManager(
        TopicPartition topicPartition,
        File logDir,
        int maxTransactionTimeoutMs,
        ProducerStateManagerConfig producerStateManagerConfig,
        Time time,

        NavigableMap<Long, ByteBuffer> snapshotsMap,
        PersistSnapshots persistSnapshots
    ) throws IOException {
        super(topicPartition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time);
        this.snapshotsMap = snapshotsMap;
        this.persistSnapshots = persistSnapshots;
        // ProducerStateManager will loadSnapshots in constructor, but at that moment, snapshotsMap is null.
        // So we need to call loadSnapshots again to load snapshots from snapshotsMap
        this.snapshots = loadSnapshots();
        this.createTimestamp = time.milliseconds();
    }

    @Override
    protected ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() {
        NavigableMap<Long, ByteBuffer> snapshotsMap = this.snapshotsMap;
        if (snapshotsMap.isEmpty()) {
            // ProducerStateManager will loadSnapshots in constructor
            return new ConcurrentSkipListMap<>();
        }
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        snapshotsMap.forEach((offset, snapshot) -> {
            File snapshotFile = new File(offset + ".snapshot");
            offsetToSnapshots.put(offset, new MockSnapshotFile(snapshotFile, offset));
        });
        return offsetToSnapshots;
    }

    @Override
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        // it's ok to call super method
        super.removeStraySnapshots(segmentBaseOffsets);
    }

    @Override
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs) throws IOException {
        // it's ok to call super method
        super.truncateAndReload(logStartOffset, logEndOffset, currentTimeMs);
        // TODO: check whether need to call onLogStartOffsetIncremented
    }

    @Override
    public ProducerAppendInfo prepareUpdate(long producerId, AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfoExt(topicPartition, producerId, currentEntry, origin, verificationStateEntry(producerId));    }

    @Override
    protected List<ProducerStateEntry> readSnapshot(File file) throws IOException {
        long offset = LogFileUtils.offsetFromFile(file);
        if (!snapshotsMap.containsKey(offset)) {
            throw new CorruptSnapshotException("Snapshot" + offset + " not found");
        }
        return ProducerStateManager.readSnapshot0(file, snapshotsMap.get(offset).array());
    }

    @Override
    public void updateParentDir(File parentDir) {
        // noop implementation
    }

    @Override
    protected void removeAndDeleteSnapshot(long snapshotOffset) {
        deleteSnapshot(snapshotOffset);
    }

    @Override
    public Optional<SnapshotFile> removeAndMarkSnapshotForDeletion(long snapshotOffset) {
        return deleteSnapshot(snapshotOffset);
    }

    @Override
    public void takeSnapshot() throws IOException {
        try {
            takeSnapshot0().get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<File> takeSnapshot(boolean sync) throws IOException {
        try {
            Optional<File> rst;
            if (lastMapOffset > lastSnapOffset) {
                rst = Optional.of(LogFileUtils.producerSnapshotFile(logDir, lastMapOffset));
            } else {
                rst = Optional.empty();
            }
            CompletableFuture<Void> cf = takeSnapshot0();
            if (sync) {
                cf.get();
            }
            return rst;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    CompletableFuture<Void> takeSnapshot0() {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile = new MockSnapshotFile(LogFileUtils.producerSnapshotFile(logDir, lastMapOffset), lastMapOffset);
            CompletableFuture<Void> rst = writeSnapshot(snapshotFile.offset, producers);
            snapshots.put(snapshotFile.offset, snapshotFile);
            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;
            return rst;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }


    /**
     * Only keep the last snapshot which offset less than or equals to the recoveryPointCheckpoint
     */
    public CompletableFuture<Void> takeSnapshotAndRemoveExpired(long recoveryPointCheckpoint) {
        Long lastSnapshotOffset = snapshotsMap.floorKey(recoveryPointCheckpoint);
        if (lastSnapshotOffset != null) {
            List<Long> expiredSnapshotOffsets = new ArrayList<>(snapshotsMap.headMap(lastSnapshotOffset, false).keySet());
            expiredSnapshotOffsets.forEach(offset -> {
                snapshotsMap.remove(offset);
                snapshots.remove(offset);
            });
        }
        return takeSnapshot0();
    }

    private CompletableFuture<Void> writeSnapshot(long offset, Map<Long, ProducerStateEntry> entries) {
        try {
            ByteBuffer buffer = ProducerStateManager.writeSnapshot(null, entries, false);
            byte[] rawSnapshot = new byte[buffer.remaining()];
            buffer.get(rawSnapshot);

            snapshotsMap.put(offset, ByteBuffer.wrap(rawSnapshot));
            ElasticPartitionProducerSnapshotsMeta meta = new ElasticPartitionProducerSnapshotsMeta(snapshotsMap);
            return persistSnapshots.persist(MetaKeyValue.of(MetaStream.PRODUCER_SNAPSHOTS_META_KEY, meta.encode()));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    private Optional<SnapshotFile> deleteSnapshot(long snapshotOffset) {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        snapshotsMap.remove(snapshotOffset);
        return Optional.ofNullable(snapshotFile);
    }

    public interface PersistSnapshots {
        CompletableFuture<Void> persist(MetaKeyValue kv);
    }

    NavigableMap<Long, ByteBuffer> snapshotsMap() {
        return snapshotsMap;
    }

    class MockSnapshotFile extends SnapshotFile {

        public MockSnapshotFile(File file, long offset) {
            super(file, offset);
        }

        @Override
        public boolean deleteIfExists() throws IOException {
            snapshotsMap.remove(offset);
            return true;
        }

        @Override
        public void renameToDelete() {
            snapshotsMap.remove(offset);
        }

        @Override
        public void updateParentDir(File parentDir) {
            // noop implementation
        }
    }

    class ProducerAppendInfoExt extends ProducerAppendInfo {
        public ProducerAppendInfoExt(TopicPartition topicPartition, long producerId, ProducerStateEntry currentEntry,
            AppendOrigin origin, VerificationStateEntry verificationStateEntry) {
            super(topicPartition, producerId, currentEntry, origin, verificationStateEntry);
        }

        @Override
        protected void checkSequence(short producerEpoch, int appendFirstSeq, long offset) {
            if (currentEntry.isEmpty() && updatedEntry.isEmpty() && appendFirstSeq != 0
                // await sequence 0 message append timeout and retry
                && time.milliseconds() - createTimestamp < AWAIT_SEQ_ZERO_TIMEOUT
            ) {
                throw new OutOfOrderSequenceException("Invalid sequence number for new created log, producer " + producerId() + " " +
                    "at offset " + offset + " in partition " + topicPartition + ": " + producerEpoch + " (request epoch), " + appendFirstSeq + " (seq. number), " +
                    updatedEntry.producerEpoch() + " (current producer epoch)");
            }
            super.checkSequence(producerEpoch, appendFirstSeq, offset);
        }
    }
}
