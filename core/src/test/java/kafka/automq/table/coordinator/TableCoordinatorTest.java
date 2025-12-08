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
import kafka.log.streamaspect.MetaStream;
import kafka.server.MetadataCache;

import org.apache.kafka.storage.internals.log.LogConfig;

import com.automq.stream.utils.threads.EventLoop;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableCoordinatorTest {

    private Catalog catalog;
    private TableCoordinator coordinator;
    private Table mockTable;
    private Transaction mockTransaction;
    private ExpireSnapshots mockExpireSnapshots;
    private Supplier<LogConfig> configSupplier;

    @BeforeEach
    void setUp() {
        catalog = mock(Catalog.class);
        MetaStream metaStream = mock(MetaStream.class);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        MetadataCache metadataCache = mock(MetadataCache.class);
        
        // Mock subchannel to return null immediately to avoid infinite polling loops if it enters that block
        Channel.SubChannel subChannel = mock(Channel.SubChannel.class);
        when(channel.subscribeData(anyString(), anyLong())).thenReturn(subChannel);
        when(channel.subscribeData(anyString(), any())).thenReturn(subChannel);

        // Prepare default config map
        Map<String, Object> props = new HashMap<>();
        props.put("table.topic.namespace", "default");
        // We use a real LogConfig object because we need to access public final fields
        LogConfig realConfig = new LogConfig(props); 
        configSupplier = () -> realConfig;

        coordinator = new TableCoordinator(catalog, "test_topic", metaStream, channel, eventLoop, metadataCache, configSupplier);
        
        // Mock Iceberg components
        mockTable = mock(Table.class);
        mockTransaction = mock(Transaction.class);
        mockExpireSnapshots = mock(ExpireSnapshots.class);
        AppendFiles mockAppendFiles = mock(AppendFiles.class);

        when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
        when(mockTable.newTransaction()).thenReturn(mockTransaction);
        when(mockTable.name()).thenReturn("test_table");
        
        // Mock Transaction chain
        when(mockTransaction.newAppend()).thenReturn(mockAppendFiles);
        when(mockTransaction.expireSnapshots()).thenReturn(mockExpireSnapshots);
        
        // Mock ExpireSnapshots chain (Builder pattern)
        when(mockExpireSnapshots.expireOlderThan(anyLong())).thenReturn(mockExpireSnapshots);
        when(mockExpireSnapshots.retainLast(anyInt())).thenReturn(mockExpireSnapshots);
        when(mockExpireSnapshots.executeDeleteWith(any(ExecutorService.class))).thenReturn(mockExpireSnapshots);
    }

    @Test
    void testTryMoveToCommitedStatus_WithExpirationEnabled() throws Exception {
        // 1. Setup Configuration: Enable expiration
        Map<String, Object> props = new HashMap<>();
        props.put(LogConfig.TableSnapshotConfig.EXPIRE_SNAPSHOT_ENABLED_CONFIG, "true");
        props.put(LogConfig.TableSnapshotConfig.EXPIRE_SNAPSHOT_OLDER_THAN_HOURS_CONFIG, "5");
        props.put(LogConfig.TableSnapshotConfig.EXPIRE_SNAPSHOT_RETAIN_LAST_CONFIG, "3");
        props.put("table.topic.namespace", "default");
        
        // Update the supplier to return this specific config
        configSupplier = () -> new LogConfig(props);
        setField(coordinator, "config", configSupplier);

        // 2. Access the inner class CommitStatusMachine
        Object machine = instantiateInnerClass(coordinator, "kafka.automq.table.coordinator.TableCoordinator$CommitStatusMachine");

        // 3. Inject State to bypass the "Waiting for Partitions" loop
        // If unreadyPartitionCount is 0, the loop breaks immediately
        setField(machine, "unreadyPartitionCount", 0);
        setField(machine, "requestCommitTimestamp", System.currentTimeMillis());

        // 4. Inject DataFiles to ensure we enter the Transaction block
        List<DataFile> dataFiles = new ArrayList<>();
        dataFiles.add(mock(DataFile.class)); // Add one dummy file
        setField(machine, "dataFiles", dataFiles);

        // 5. Inject 'processing' CommitInfo (Required for logging/logic)
        Object commitInfo = instantiateInnerClassStatic("kafka.automq.table.coordinator.TableCoordinator$CommitInfo", 
                UUID.randomUUID(), 0L, new long[0]);
        setField(machine, "processing", commitInfo);
        
        // 6. Inject the mocked SubChannel (to prevent NPE on close)
        Channel.SubChannel mockSub = mock(Channel.SubChannel.class);
        setField(machine, "subChannel", mockSub);

        // 7. Inject the machine into the coordinator
        setField(coordinator, "commitStatusMachine", machine);

        // 8. Execute the method
        // Since tryMoveToCommitedStatus is in the inner class, we invoke it via reflection or direct call if accessible
        // Based on the provided file, it is package-private or public, so we can use reflection to call it
        machine.getClass().getMethod("tryMoveToCommitedStatus").invoke(machine);

        // 9. Verify logic
        verify(mockTransaction, times(1)).expireSnapshots();
        
        ArgumentCaptor<Integer> retainLastCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(mockExpireSnapshots).retainLast(retainLastCaptor.capture());
        assertEquals(3, retainLastCaptor.getValue(), "Should retain 3 snapshots as per config");

        verify(mockExpireSnapshots).expireOlderThan(anyLong());
        verify(mockExpireSnapshots).commit(); // Ensure the expiration commit was called
        verify(mockTransaction).commitTransaction(); // Ensure the main transaction was committed
    }

    @Test
    void testTryMoveToCommitedStatus_WithExpirationDisabled() throws Exception {
        // 1. Setup Configuration: Disable expiration
        Map<String, Object> props = new HashMap<>();
        props.put(LogConfig.TableSnapshotConfig.EXPIRE_SNAPSHOT_ENABLED_CONFIG, "false");
        props.put("table.topic.namespace", "default");
        
        configSupplier = () -> new LogConfig(props);
        setField(coordinator, "config", configSupplier);

        // 2. Setup Machine State (Same as above)
        Object machine = instantiateInnerClass(coordinator, "kafka.automq.table.coordinator.TableCoordinator$CommitStatusMachine");
        setField(machine, "unreadyPartitionCount", 0);
        setField(machine, "requestCommitTimestamp", System.currentTimeMillis());
        List<DataFile> dataFiles = new ArrayList<>();
        dataFiles.add(mock(DataFile.class)); 
        setField(machine, "dataFiles", dataFiles);
        Object commitInfo = instantiateInnerClassStatic("kafka.automq.table.coordinator.TableCoordinator$CommitInfo", 
                UUID.randomUUID(), 0L, new long[0]);
        setField(machine, "processing", commitInfo);
        setField(machine, "subChannel", mock(Channel.SubChannel.class));
        setField(coordinator, "commitStatusMachine", machine);

        // 3. Execute
        machine.getClass().getMethod("tryMoveToCommitedStatus").invoke(machine);

        // 4. Verify logic
        verify(mockTransaction, never()).expireSnapshots(); // Should NOT be called
        verify(mockTransaction).commitTransaction(); // Main commit should still happen
    }

    // --- Reflection Helpers to handle Private Fields/Inner Classes ---

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    // Helper to instantiate the non-static inner class CommitStatusMachine
    private Object instantiateInnerClass(Object outerInstance, String innerClassName) throws Exception {
        Class<?> innerClass = Class.forName(innerClassName);
        Constructor<?> constructor = innerClass.getDeclaredConstructor(outerInstance.getClass());
        constructor.setAccessible(true);
        return constructor.newInstance(outerInstance);
    }
    
    // Helper to instantiate the static inner class CommitInfo
    private Object instantiateInnerClassStatic(String innerClassName, Object... args) throws Exception {
        Class<?> innerClass = Class.forName(innerClassName);
        Constructor<?> constructor = innerClass.getDeclaredConstructors()[0]; // Assume first constructor
        constructor.setAccessible(true);
        return constructor.newInstance(args);
    }
}