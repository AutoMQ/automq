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

package org.apache.kafka.image.publisher;

<<<<<<< HEAD
=======
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.FakeSnapshotWriter;
>>>>>>> trunk
import org.apache.kafka.image.MetadataImageTest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

<<<<<<< HEAD
import java.util.ArrayList;
=======
>>>>>>> trunk
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class SnapshotEmitterTest {
    static class MockRaftClient implements RaftClient<ApiMessageAndVersion> {
<<<<<<< HEAD
        TreeMap<OffsetAndEpoch, MockSnapshotWriter> writers = new TreeMap<>();
=======
        TreeMap<OffsetAndEpoch, FakeSnapshotWriter> writers = new TreeMap<>();
>>>>>>> trunk

        @Override
        public void initialize() {
            // nothing to do
        }

        @Override
        public void register(Listener<ApiMessageAndVersion> listener) {
            // nothing to do
        }

        @Override
        public void unregister(Listener<ApiMessageAndVersion> listener) {
            // nothing to do
        }

        @Override
        public OptionalLong highWatermark() {
            return OptionalLong.empty();
        }

        @Override
        public LeaderAndEpoch leaderAndEpoch() {
            return LeaderAndEpoch.UNKNOWN;
        }

        @Override
        public OptionalInt nodeId() {
            return OptionalInt.empty();
        }

        @Override
        public long scheduleAppend(int epoch, List<ApiMessageAndVersion> records) {
            return 0;
        }

        @Override
<<<<<<< HEAD
        public long scheduleAtomicAppend(int epoch, List<ApiMessageAndVersion> records) {
=======
        public long scheduleAtomicAppend(
            int epoch,
            OptionalLong requiredEndOffset,
            List<ApiMessageAndVersion> records
        ) {
>>>>>>> trunk
            return 0;
        }

        @Override
        public CompletableFuture<Void> shutdown(int timeoutMs) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void resign(int epoch) {
            // nothing to do
        }

        @Override
        public Optional<SnapshotWriter<ApiMessageAndVersion>> createSnapshot(
            OffsetAndEpoch snapshotId,
            long lastContainedLogTime
        ) {
            if (writers.containsKey(snapshotId)) {
                return Optional.empty();
            }
<<<<<<< HEAD
            MockSnapshotWriter writer = new MockSnapshotWriter(snapshotId);
=======
            FakeSnapshotWriter writer = new FakeSnapshotWriter(snapshotId);
>>>>>>> trunk
            writers.put(snapshotId, writer);
            return Optional.of(writer);
        }

        @Override
        public Optional<OffsetAndEpoch> latestSnapshotId() {
            NavigableSet<OffsetAndEpoch> descendingSet = writers.descendingKeySet();
            if (descendingSet.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(descendingSet.first());
            }
        }

        @Override
<<<<<<< HEAD
=======
        public long logEndOffset() {
            return 0;
        }

        @Override
>>>>>>> trunk
        public void close() throws Exception {
            // nothing to do
        }
    }

<<<<<<< HEAD
    static class MockSnapshotWriter implements SnapshotWriter<ApiMessageAndVersion> {
        private final OffsetAndEpoch snapshotId;
        private boolean frozen = false;
        private boolean closed = false;
        private final List<List<ApiMessageAndVersion>> batches;

        MockSnapshotWriter(OffsetAndEpoch snapshotId) {
            this.snapshotId = snapshotId;
            this.batches = new ArrayList<>();
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long lastContainedLogOffset() {
            return snapshotId.offset() - 1;
        }

        @Override
        public int lastContainedLogEpoch() {
            return snapshotId.epoch();
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public void append(List<ApiMessageAndVersion> records) {
            batches.add(records);
        }

        List<List<ApiMessageAndVersion>> batches() {
            List<List<ApiMessageAndVersion>> results = new ArrayList<>();
            batches.forEach(batch -> results.add(new ArrayList<>(batch)));
            return results;
        }

        @Override
        public void freeze() {
            frozen = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    @Test
    public void testEmit() throws Exception {
        MockRaftClient mockRaftClient = new MockRaftClient();
        SnapshotEmitter emitter = new SnapshotEmitter.Builder().
            setBatchSize(2).
            setRaftClient(mockRaftClient).
            build();
        emitter.maybeEmit(MetadataImageTest.IMAGE1);
        MockSnapshotWriter writer = mockRaftClient.writers.get(
=======
    @Test
    public void testEmit() {
        MockRaftClient mockRaftClient = new MockRaftClient();
        MockTime time = new MockTime(0, 10000L, 20000L);
        SnapshotEmitter emitter = new SnapshotEmitter.Builder().
            setTime(time).
            setBatchSize(2).
            setRaftClient(mockRaftClient).
            build();
        assertEquals(0L, emitter.metrics().latestSnapshotGeneratedAgeMs());
        assertEquals(0L, emitter.metrics().latestSnapshotGeneratedBytes());
        time.sleep(30000L);
        assertEquals(30000L, emitter.metrics().latestSnapshotGeneratedAgeMs());
        assertEquals(0L, emitter.metrics().latestSnapshotGeneratedBytes());
        emitter.maybeEmit(MetadataImageTest.IMAGE1);
        assertEquals(0L, emitter.metrics().latestSnapshotGeneratedAgeMs());
        assertEquals(1600L, emitter.metrics().latestSnapshotGeneratedBytes());
        FakeSnapshotWriter writer = mockRaftClient.writers.get(
>>>>>>> trunk
                MetadataImageTest.IMAGE1.provenance().snapshotId());
        assertNotNull(writer);
        assertEquals(MetadataImageTest.IMAGE1.highestOffsetAndEpoch().offset(),
                writer.lastContainedLogOffset());
        assertEquals(MetadataImageTest.IMAGE1.highestOffsetAndEpoch().epoch(),
                writer.lastContainedLogEpoch());
        assertTrue(writer.isFrozen());
        assertTrue(writer.isClosed());

        // Second call to emit does nothing because we already have a snapshot at that offset and epoch.
        emitter.maybeEmit(MetadataImageTest.IMAGE1);
        assertEquals(1, mockRaftClient.writers.size());
    }
}
