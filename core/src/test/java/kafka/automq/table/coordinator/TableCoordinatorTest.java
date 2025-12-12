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

package kafka.automq.table.coordinator;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Errors;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.PartitionMetric;
import kafka.automq.table.events.TopicMetric;
import kafka.automq.table.events.WorkerOffset;
import kafka.automq.table.utils.PartitionUtil;
import kafka.automq.table.utils.TableIdentifierUtil;
import kafka.log.streamaspect.MetaKeyValue;
import kafka.log.streamaspect.MetaStream;
import kafka.server.MetadataCache;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import scala.Option;
import scala.Some;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TableCoordinatorTest {

    private static final String TOPIC = "test-topic";

    @Mock
    Channel channel;
    @Mock
    MetaStream metaStream;
    @Mock
    MetadataCache metadataCache;

    private InMemoryCatalog catalog;
    private Table table;
    private TableCoordinator coordinator;
    private Supplier<LogConfig> configSupplier;
    private TableCoordinator.CommitStatusMachine machine;
    private FakeSubChannel subChannel;

    @BeforeEach
    void setUp() {
        LogConfig logConfig = new FakeLogConfig(1000L, "db", "");
        configSupplier = () -> logConfig;

        // metadata stubs
        UpdateMetadataRequestData.UpdateMetadataPartitionState state = new UpdateMetadataRequestData.UpdateMetadataPartitionState();
        state.setLeaderEpoch(1);
        doReturn((Option<Integer>) Some.apply(2)).when(metadataCache).numPartitions(TOPIC);
        when(metadataCache.getPartitionInfo(eq(TOPIC), anyInt())).thenReturn(Option.apply(state));

        // in-memory iceberg catalog & table
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionUtil.buildPartitionSpec(List.of(), schema);
        TableIdentifier identifier = TableIdentifierUtil.of("db", TOPIC);
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(identifier.namespace());
        table = catalog.createTable(identifier, schema, spec);

        // meta stream stub
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(new DummyAppendResult()));

        // channel stub
        subChannel = new FakeSubChannel();
        when(channel.subscribeData(eq(TOPIC), anyLong())).thenAnswer(invocation -> subChannel);

        coordinator = new TableCoordinator(catalog, TOPIC, metaStream, channel, new ImmediateEventLoop(), metadataCache, configSupplier);
        machine = coordinator.new CommitStatusMachine();
    }

    @Test
    void nextRoundCommitSendsCommitRequestAndCheckpoint() throws Exception {
        machine.nextRoundCommit();

        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(channel).send(eq(TOPIC), eventCaptor.capture());
        Event event = eventCaptor.getValue();
        assertEquals(EventType.COMMIT_REQUEST, event.type());
        CommitRequest payload = event.payload();
        assertNotNull(payload.commitId());
        assertEquals(2, payload.offsets().size());

        verify(metaStream).append(any(MetaKeyValue.class));
        assertEquals(Status.REQUEST_COMMIT, machine.status);
    }

    @Test
    void commitResponseMovesToCommittedAndWritesIcebergSnapshot() throws Exception {
        machine.nextRoundCommit();

        UUID commitId = machine.processing.commitId;
        Types.StructType partitionType = table.spec().partitionType();
        List<WorkerOffset> nextOffsets = List.of(new WorkerOffset(0, 1, 5L), new WorkerOffset(1, 1, 6L));
        DataFile dataFile = DataFiles.builder(table.spec())
            .withPath("file:///tmp/commit.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
        CommitResponse response = new CommitResponse(partitionType, Errors.NONE, commitId, TOPIC, nextOffsets,
            List.of(dataFile), List.of(), new TopicMetric(10),
            List.of(new PartitionMetric(0, 100L), new PartitionMetric(1, 200L)));
        subChannel.offer(new Envelope(0, 3L, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, response)));

        machine.tryMoveToCommittedStatus();
        table.refresh();

        assertEquals(Status.COMMITTED, machine.status);
        assertArrayEquals(new long[]{5L, 6L}, machine.last.nextOffsets);
        Snapshot snapshot = table.currentSnapshot();
        assertNotNull(snapshot);
        assertEquals(commitId.toString(), snapshot.summary().get("automq.commit.id"));
        assertEquals("100", snapshot.summary().get("automq.watermark"));
    }

    @Test
    void moreDataResponseEnablesFastNextCommit() throws Exception {
        machine.nextRoundCommit();
        UUID commitId = machine.processing.commitId;
        Types.StructType partitionType = table.spec().partitionType();
        List<WorkerOffset> nextOffsets = List.of(new WorkerOffset(0, 1, 2L), new WorkerOffset(1, 1, 3L));
        CommitResponse response = new CommitResponse(partitionType, Errors.MORE_DATA, commitId, TOPIC, nextOffsets, List.of(), List.of(), TopicMetric.NOOP, List.of());
        subChannel.offer(new Envelope(0, 1L, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, response)));

        machine.tryMoveToCommittedStatus();

        assertTrue(getPrivateBoolean(machine, "fastNextCommit"));
    }

    @Test
    void watermarkHelperWorks() {
        assertEquals(-1L, TableCoordinator.watermark(new long[]{-1L, -1L}));
        assertEquals(100L, TableCoordinator.watermark(new long[]{100L, 200L}));
        assertEquals(50L, TableCoordinator.watermark(new long[]{-1L, 50L}));
    }

    @Test
    void commitTimesOutButStillAdvances() throws Exception {
        machine.nextRoundCommit();
        setPrivateLong(machine, "requestCommitTimestamp", System.currentTimeMillis() - 60_000);

        machine.tryMoveToCommittedStatus();

        assertEquals(Status.COMMITTED, machine.status);
        assertTrue(getPrivateBoolean(machine, "fastNextCommit"));
    }

    @Test
    void checkpointRecoveryFromRequestCommitInitializesState() throws Exception {
        UUID commitId = UUID.randomUUID();
        long[] next = new long[]{1L, 2L};
        Checkpoint cp = new Checkpoint(Status.REQUEST_COMMIT, commitId, 10L, next, UUID.randomUUID(), 0L, new long[0]);
        TableCoordinator.CommitStatusMachine recovered = coordinator.new CommitStatusMachine(cp);

        assertEquals(Status.REQUEST_COMMIT, recovered.status);
        assertArrayEquals(next, recovered.processing.nextOffsets);
        assertEquals(2, getPartitionWatermarks(recovered).length);
    }

    @Test
    void checkpointPreCommitSkipsAlreadyCommittedSnapshot() {
        UUID commitId = UUID.randomUUID();
        table.newAppend()
            .set("automq.commit.id", commitId.toString())
            .set("automq.watermark", "123")
            .commit();

        long[] next = new long[]{3L, 4L};
        Checkpoint cp = new Checkpoint(Status.PRE_COMMIT, commitId, 5L, next, UUID.randomUUID(), 0L, new long[]{3L, 4L});
        TableCoordinator.CommitStatusMachine recovered = coordinator.new CommitStatusMachine(cp);

        assertEquals(Status.COMMITTED, recovered.status);
        assertArrayEquals(next, recovered.last.nextOffsets);
        assertEquals(commitId, recovered.last.commitId);
    }

    @Test
    void partitionNumIncreaseExpandsArrays() throws Exception {
        machine.nextRoundCommit(); // init with 2 partitions
        doReturn((Option<Integer>) Some.apply(4)).when(metadataCache).numPartitions(TOPIC);

        machine.nextRoundCommit();

        assertEquals(4, machine.processing.nextOffsets.length);
        assertEquals(4, getPartitionWatermarks(machine).length);
    }

    @Test
    void partitionByEvolutionTriggersEvolve() throws Exception {
        setPrivateField(coordinator, "table", table);
        setLogConfigField("tableTopicPartitionBy", "id");

        Method evolve = machine.getClass().getDeclaredMethod("tryEvolvePartition");
        evolve.setAccessible(true);
        boolean evolved = (boolean) evolve.invoke(machine);

        assertTrue(evolved);
    }

    @Test
    void expireSnapshotsHonorsDefaultRetention() throws Exception {
        SpyHolder spyHolder = spyTableForExpireVerification(table);
        setPrivateField(coordinator, "table", spyHolder.tableSpy);

        machine.nextRoundCommit();
        UUID commitId = machine.processing.commitId;

        DataFile dataFile = DataFiles.builder(table.spec())
            .withPath("file:///tmp/commit.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

        CommitResponse response = createCommitResponse(commitId, List.of(dataFile));
        subChannel.offer(new Envelope(0, 1L, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, response)));

        machine.tryMoveToCommittedStatus();

        assertEquals(Status.COMMITTED, machine.status);
        verifyExpireSnapshotsCalledWith(spyHolder.capturedExpireSnapshots, 1, 1);
    }

    @Test
    void expireSnapshotsUsesConfiguredValues() throws Exception {
        SpyHolder spyHolder = spyTableForExpireVerification(table);
        setPrivateField(coordinator, "table", spyHolder.tableSpy);
        setCustomExpireConfig(5, 3, true);

        machine.nextRoundCommit();
        UUID commitId = machine.processing.commitId;

        DataFile dataFile = DataFiles.builder(table.spec())
            .withPath("file:///tmp/commit.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

        CommitResponse response = createCommitResponse(commitId, List.of(dataFile));
        subChannel.offer(new Envelope(0, 1L, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, response)));

        machine.tryMoveToCommittedStatus();

        assertEquals(Status.COMMITTED, machine.status);
        verifyExpireSnapshotsCalledWith(spyHolder.capturedExpireSnapshots, 3, 5);
    }

    @Test
    void expireSnapshotsDisabledSkipsCall() throws Exception {
        SpyHolder spyHolder = spyTableForExpireVerification(table);
        setPrivateField(coordinator, "table", spyHolder.tableSpy);
        setCustomExpireConfig(0, 0, false);

        machine.nextRoundCommit();
        UUID commitId = machine.processing.commitId;

        CommitResponse response = createCommitResponse(commitId, List.of());
        subChannel.offer(new Envelope(0, 0L, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, response)));

        machine.tryMoveToCommittedStatus();

        assertEquals(Status.COMMITTED, machine.status);
        verify(spyHolder.tableSpy, Mockito.never()).newTransaction();
    }

    // --- test helpers ---
    private CommitResponse createCommitResponse(UUID commitId, List<DataFile> dataFiles) {
        Types.StructType partitionType = table.spec().partitionType();
        List<WorkerOffset> nextOffsets = List.of(new WorkerOffset(0, 1, 5L), new WorkerOffset(1, 1, 6L));
        TopicMetric topicMetric = dataFiles.isEmpty() ? TopicMetric.NOOP : new TopicMetric(1);
        List<PartitionMetric> partitionMetrics = List.of(new PartitionMetric(0, 10L), new PartitionMetric(1, 20L));

        return new CommitResponse(partitionType, Errors.NONE, commitId, TOPIC, nextOffsets,
            dataFiles, List.of(), topicMetric, partitionMetrics);
    }

    private void setCustomExpireConfig(int olderThanHours, int retainLast, boolean enabled) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(TopicConfig.TABLE_TOPIC_ENABLE_CONFIG, true);
        props.put(TopicConfig.TABLE_TOPIC_COMMIT_INTERVAL_CONFIG, 1000L);
        props.put(TopicConfig.TABLE_TOPIC_NAMESPACE_CONFIG, "db");
        props.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1073741824);
        props.put(TopicConfig.RETENTION_MS_CONFIG, 86400000L);
        props.put(TopicConfig.AUTOMQ_TABLE_TOPIC_EXPIRE_SNAPSHOT_ENABLED_CONFIG, enabled);
        if (enabled) {
            props.put(TopicConfig.AUTOMQ_TABLE_TOPIC_EXPIRE_SNAPSHOT_OLDER_THAN_HOURS_CONFIG, olderThanHours);
            props.put(TopicConfig.AUTOMQ_TABLE_TOPIC_EXPIRE_SNAPSHOT_RETAIN_LAST_CONFIG, retainLast);
        }
        Supplier<LogConfig> custom = () -> new LogConfig(props);
        setPrivateField(coordinator, "config", custom);
    }

    private void verifyExpireSnapshotsCalledWith(ExpireSnapshots expireSnapshots, int retainLast, int olderThanHours) {
        assertNotNull(expireSnapshots, "ExpireSnapshots should have been captured");

        verify(expireSnapshots).retainLast(retainLast);

        ArgumentCaptor<Long> olderThanCaptor = ArgumentCaptor.forClass(Long.class);
        verify(expireSnapshots).expireOlderThan(olderThanCaptor.capture());
        long expectedOlderThan = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(olderThanHours);
        long actualOlderThan = olderThanCaptor.getValue();
        assertTrue(Math.abs(actualOlderThan - expectedOlderThan) < TimeUnit.SECONDS.toMillis(5),
            String.format("Expected olderThan within 5s of %d hours ago, but was %d ms off",
                olderThanHours, Math.abs(actualOlderThan - expectedOlderThan)));

        verify(expireSnapshots).executeDeleteWith(any());
        verify(expireSnapshots).commit();
    }

    private static boolean getPrivateBoolean(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getBoolean(target);
    }

    private static class FakeSubChannel implements Channel.SubChannel {
        private final ArrayDeque<Envelope> queue = new ArrayDeque<>();

        void offer(Envelope envelope) {
            queue.offer(envelope);
        }

        @Override
        public Envelope poll() {
            return queue.poll();
        }

        @Override
        public void close() {
        }
    }

    private static class ImmediateEventLoop extends com.automq.stream.utils.threads.EventLoop {
        ImmediateEventLoop() {
            super("immediate-loop");
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> submit(Runnable task) {
            task.run();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class FakeLogConfig extends LogConfig {
        FakeLogConfig(long commitInterval, String namespace, String partitionBy) {
            super(buildProps(commitInterval, namespace, partitionBy));
        }

        private static Map<String, Object> buildProps(long commitInterval, String namespace, String partitionBy) {
            Map<String, Object> props = new HashMap<>();
            props.put(TopicConfig.TABLE_TOPIC_ENABLE_CONFIG, true);
            props.put(TopicConfig.TABLE_TOPIC_COMMIT_INTERVAL_CONFIG, commitInterval);
            props.put(TopicConfig.TABLE_TOPIC_NAMESPACE_CONFIG, namespace);
            props.put(TopicConfig.TABLE_TOPIC_PARTITION_BY_CONFIG, partitionBy);
            // supply required basics to satisfy defaults
            props.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1073741824);
            props.put(TopicConfig.RETENTION_MS_CONFIG, 86400000L);
            return props;
        }
    }

    private static class DummyAppendResult implements com.automq.stream.api.AppendResult {
        @Override
        public long baseOffset() {
            return 0;
        }
    }

    private static void setPrivateLong(Object target, String name, long value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void setPrivateField(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private void setLogConfigField(String name, Object value) throws Exception {
        Field f = LogConfig.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(configSupplier.get(), value);
    }

    private static long[] getPartitionWatermarks(TableCoordinator.CommitStatusMachine machine) throws Exception {
        Field f = machine.getClass().getDeclaredField("partitionWatermarks");
        f.setAccessible(true);
        return (long[]) f.get(machine);
    }

    // --- Spy infrastructure for testing ExpireSnapshots ---

    private static class SpyHolder {
        final Table tableSpy;
        volatile ExpireSnapshots capturedExpireSnapshots;

        SpyHolder(Table tableSpy) {
            this.tableSpy = tableSpy;
        }
    }

    private static SpyHolder spyTableForExpireVerification(Table delegate) {
        Table tableSpy = Mockito.spy(delegate);
        SpyHolder holder = new SpyHolder(tableSpy);

        Mockito.doAnswer(invocation -> {
            Transaction realTxn = (Transaction) invocation.callRealMethod();
            return new TransactionWrapper(realTxn, holder);
        }).when(tableSpy).newTransaction();

        return holder;
    }

    /**
     * Transparent wrapper for Transaction that only intercepts expireSnapshots()
     * to create a spy for verification purposes.
     */
    private static class TransactionWrapper implements Transaction {
        private final Transaction delegate;
        private final SpyHolder holder;

        TransactionWrapper(Transaction delegate, SpyHolder holder) {
            this.delegate = delegate;
            this.holder = holder;
        }

        @Override
        public ExpireSnapshots expireSnapshots() {
            ExpireSnapshots realExpire = delegate.expireSnapshots();
            ExpireSnapshots expireSpy = Mockito.spy(realExpire);
            holder.capturedExpireSnapshots = expireSpy;
            return expireSpy;
        }

        // All other methods delegate transparently
        @Override
        public org.apache.iceberg.AppendFiles newAppend() {
            return delegate.newAppend();
        }

        @Override
        public org.apache.iceberg.AppendFiles newFastAppend() {
            return delegate.newFastAppend();
        }

        @Override
        public org.apache.iceberg.RewriteFiles newRewrite() {
            return delegate.newRewrite();
        }

        @Override
        public org.apache.iceberg.RewriteManifests rewriteManifests() {
            return delegate.rewriteManifests();
        }

        @Override
        public org.apache.iceberg.OverwriteFiles newOverwrite() {
            return delegate.newOverwrite();
        }

        @Override
        public org.apache.iceberg.RowDelta newRowDelta() {
            return delegate.newRowDelta();
        }

        @Override
        public org.apache.iceberg.ReplacePartitions newReplacePartitions() {
            return delegate.newReplacePartitions();
        }

        @Override
        public org.apache.iceberg.DeleteFiles newDelete() {
            return delegate.newDelete();
        }

        @Override
        public org.apache.iceberg.UpdateProperties updateProperties() {
            return delegate.updateProperties();
        }

        @Override
        public org.apache.iceberg.UpdateSchema updateSchema() {
            return delegate.updateSchema();
        }

        @Override
        public org.apache.iceberg.UpdatePartitionSpec updateSpec() {
            return delegate.updateSpec();
        }

        @Override
        public org.apache.iceberg.UpdateLocation updateLocation() {
            return delegate.updateLocation();
        }

        @Override
        public org.apache.iceberg.ReplaceSortOrder replaceSortOrder() {
            return delegate.replaceSortOrder();
        }

        @Override
        public void commitTransaction() {
            delegate.commitTransaction();
        }

        @Override
        public org.apache.iceberg.Table table() {
            return delegate.table();
        }
    }
}
