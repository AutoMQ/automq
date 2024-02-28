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
package kafka.log.remote;

import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteLogReaderTest {
    public static final String TOPIC = "test";
    RemoteLogManager mockRLM = mock(RemoteLogManager.class);
    BrokerTopicStats brokerTopicStats = null;
    LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(100);
    Records records = mock(Records.class);

    @BeforeEach
    public void setUp() {
        TestUtils.clearYammerMetrics();
        Properties props = kafka.utils.TestUtils.createDummyBrokerConfig();
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        brokerTopicStats = new BrokerTopicStats(Optional.of(KafkaConfig.fromProps(props)));
    }

    @Test
    public void testRemoteLogReaderWithoutError() throws RemoteStorageException, IOException {
        FetchDataInfo fetchDataInfo = new FetchDataInfo(logOffsetMetadata, records);
        when(records.sizeInBytes()).thenReturn(100);
        when(mockRLM.read(any(RemoteStorageFetchInfo.class))).thenReturn(fetchDataInfo);

        Consumer<RemoteLogReadResult> callback = mock(Consumer.class);
        RemoteStorageFetchInfo remoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, new TopicPartition(TOPIC, 0), null, null, false);
        RemoteLogReader remoteLogReader = new RemoteLogReader(remoteStorageFetchInfo, mockRLM, callback, brokerTopicStats);
        remoteLogReader.call();

        // verify the callback did get invoked with the expected remoteLogReadResult
        ArgumentCaptor<RemoteLogReadResult> remoteLogReadResultArg = ArgumentCaptor.forClass(RemoteLogReadResult.class);
        verify(callback, times(1)).accept(remoteLogReadResultArg.capture());
        RemoteLogReadResult actualRemoteLogReadResult = remoteLogReadResultArg.getValue();
        assertFalse(actualRemoteLogReadResult.error.isPresent());
        assertTrue(actualRemoteLogReadResult.fetchDataInfo.isPresent());
        assertEquals(fetchDataInfo, actualRemoteLogReadResult.fetchDataInfo.get());

        // Verify metrics for remote reads are updated correctly
        assertEquals(1, brokerTopicStats.topicStats(TOPIC).remoteFetchRequestRate().count());
        assertEquals(100, brokerTopicStats.topicStats(TOPIC).remoteFetchBytesRate().count());
        assertEquals(0, brokerTopicStats.topicStats(TOPIC).failedRemoteFetchRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteFetchRequestRate().count());
        assertEquals(100, brokerTopicStats.allTopicsStats().remoteFetchBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteFetchRequestRate().count());
    }

    @Test
    public void testRemoteLogReaderWithError() throws RemoteStorageException, IOException {
        when(mockRLM.read(any(RemoteStorageFetchInfo.class))).thenThrow(new RuntimeException("error"));

        Consumer<RemoteLogReadResult> callback = mock(Consumer.class);
        RemoteStorageFetchInfo remoteStorageFetchInfo = new RemoteStorageFetchInfo(0, false, new TopicPartition(TOPIC, 0), null, null, false);
        RemoteLogReader remoteLogReader = new RemoteLogReader(remoteStorageFetchInfo, mockRLM, callback, brokerTopicStats);
        remoteLogReader.call();

        // verify the callback did get invoked with the expected remoteLogReadResult
        ArgumentCaptor<RemoteLogReadResult> remoteLogReadResultArg = ArgumentCaptor.forClass(RemoteLogReadResult.class);
        verify(callback, times(1)).accept(remoteLogReadResultArg.capture());
        RemoteLogReadResult actualRemoteLogReadResult = remoteLogReadResultArg.getValue();
        assertTrue(actualRemoteLogReadResult.error.isPresent());
        assertFalse(actualRemoteLogReadResult.fetchDataInfo.isPresent());

        // Verify metrics for remote reads are updated correctly
        assertEquals(1, brokerTopicStats.topicStats(TOPIC).remoteFetchRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(TOPIC).remoteFetchBytesRate().count());
        assertEquals(1, brokerTopicStats.topicStats(TOPIC).failedRemoteFetchRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteFetchRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteFetchBytesRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteFetchRequestRate().count());
    }
}
