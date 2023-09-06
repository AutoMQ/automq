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

package kafka.log.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.metadata.StreamMetadataManager.StreamMetadataListener;
import kafka.server.BrokerServer;
import kafka.server.metadata.BrokerMetadataListener;
import kafka.server.metadata.KRaftMetadataCache;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(40)
@Tag("S3Unit")
public class StreamMetadataManagerTest {

    private BrokerServer mockBroker;
    private KRaftMetadataCache mockMetadataCache;
    private BrokerMetadataListener mockBrokerMetadataListener;
    private StreamMetadataListener streamMetadataListener;
    private StreamMetadataManager manager;

    @BeforeEach
    public void setUp() {
        this.mockBroker = Mockito.mock(BrokerServer.class);
        this.mockMetadataCache = Mockito.mock(KRaftMetadataCache.class);
        this.mockBrokerMetadataListener = Mockito.mock(BrokerMetadataListener.class);
        Mockito.when(this.mockBroker.metadataCache()).thenReturn(this.mockMetadataCache);
        Mockito.when(this.mockBroker.metadataListener()).thenReturn(this.mockBrokerMetadataListener);
        Mockito.doAnswer(invocation -> {
            this.streamMetadataListener = invocation.getArgument(0);
            return null;
        }).when(this.mockBrokerMetadataListener).registerStreamMetadataListener(any());
        this.manager = new StreamMetadataManager(this.mockBroker, null);
    }

    @Test
    public void testFetch() throws Exception {
        Mockito.when(this.mockMetadataCache.getStreamOffsetRange(1L)).thenReturn(new StreamOffsetRange(1L, 0L, 100L));
        S3ObjectMetadata object0 = new S3ObjectMetadata(1L, 128, S3ObjectType.WAL_LOOSE);
        Mockito.when(this.mockMetadataCache.getObjects(1L, 10L, 100L, 5))
            .thenReturn(new InRangeObjects(1L, 10L, 100L, List.of(object0)));

        // 1. normal fetch
        CompletableFuture<InRangeObjects> result = this.manager.fetch(1L, 10L, 100L, 5);
        Mockito.verify(this.mockMetadataCache).getStreamOffsetRange(1L);
        Mockito.verify(this.mockMetadataCache).getObjects(1L, 10L, 100L, 5);
        Mockito.verifyNoMoreInteractions(this.mockMetadataCache);
        InRangeObjects inRangeObjects = result.get();
        assertEquals(1L, inRangeObjects.streamId());
        assertEquals(10L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(object0, inRangeObjects.objects().get(0));

        // 2. fetch with invalid streamId
        result = this.manager.fetch(2L, 0L, 100L, 5);
        inRangeObjects = result.get();
        assertEquals(InRangeObjects.INVALID, inRangeObjects);

        // 5. fetch with smaller startOffset
        result = this.manager.fetch(1L, 5L, 100L, 5);
        inRangeObjects = result.get();
        assertEquals(InRangeObjects.INVALID, inRangeObjects);

        // 6. fetch with larger endOffset
        result = this.manager.fetch(1L, 10L, 200L, 5);
        CompletableFuture<InRangeObjects> finalResult = result;
        assertThrows(TimeoutException.class, () -> {
            finalResult.get(1, TimeUnit.SECONDS);
        });

        // 7. notify the manager that streams' end offset has been advanced
        Mockito.when(this.mockMetadataCache.getStreamOffsetRange(1L)).thenReturn(new StreamOffsetRange(1L, 0L, 150L));
        Mockito.when(this.mockMetadataCache.getStreamOffsetRange(2L)).thenReturn(new StreamOffsetRange(2L, 0L, 150L));
        S3ObjectMetadata object1 = new S3ObjectMetadata(2L, 128, S3ObjectType.WAL_LOOSE);
        Mockito.when(this.mockMetadataCache.getObjects(1L, 10L, 150L, 5))
            .thenReturn(new InRangeObjects(1L, 10L, 200L, List.of(object0, object1)));

        streamMetadataListener.onChange(null, null);

        assertThrows(TimeoutException.class, () -> {
            finalResult.get(1, TimeUnit.SECONDS);
        });

        // 8. notify with correct end offset
        Mockito.when(this.mockMetadataCache.getStreamOffsetRange(1L)).thenReturn(new StreamOffsetRange(1L, 0L, 200L));
        S3ObjectMetadata object2 = new S3ObjectMetadata(3L, 128, S3ObjectType.WAL_LOOSE);
        Mockito.when(this.mockMetadataCache.getObjects(1L, 10L, 200L, 5))
            .thenReturn(new InRangeObjects(1L, 10L, 200L, List.of(object0, object1, object2)));

        streamMetadataListener.onChange(null, null);

        assertDoesNotThrow(() -> {
            InRangeObjects rangeObjects = finalResult.get(1, TimeUnit.SECONDS);
            assertEquals(1L, rangeObjects.streamId());
            assertEquals(10L, rangeObjects.startOffset());
            assertEquals(200L, rangeObjects.endOffset());
            assertEquals(3, rangeObjects.objects().size());
            assertEquals(object0, rangeObjects.objects().get(0));
            assertEquals(object1, rangeObjects.objects().get(1));
            assertEquals(object2, rangeObjects.objects().get(2));
        });

    }
}
