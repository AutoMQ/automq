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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.metadata.StreamMetadataManager.StreamMetadataListener;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import kafka.server.metadata.BrokerMetadataListener;
import kafka.server.metadata.KRaftMetadataCache;
import org.apache.kafka.image.BrokerS3WALMetadataImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.apache.kafka.metadata.stream.SortedWALObjectsList;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.apache.kafka.metadata.stream.StreamState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(40)
@Tag("S3Unit")
public class StreamMetadataManagerTest {

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final long STREAM0 = 0;
    private static final long STREAM1 = 1;
    private static final long STREAM2 = 2;

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
        Mockito.when(this.mockMetadataCache.currentImage()).thenReturn(MetadataImage.EMPTY);
        KafkaConfig config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(BROKER0);
        this.manager = new StreamMetadataManager(this.mockBroker, config);
    }

    private static MetadataImage image0;
    private static MetadataImage image1;
    private static MetadataImage image2;

    static {
        S3ObjectsImage objectsImage = new S3ObjectsImage(2L, Map.of(
            0L, new S3Object(0L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED),
            1L, new S3Object(1L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED),
            2L, new S3Object(2L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED)
        ));

        Map<Integer, RangeMetadata> ranges = Map.of(
            0, new RangeMetadata(STREAM0, 0L, 0, 10L, 100L, BROKER0)
        );
        Map<Long, S3StreamObject> streamObjects = Map.of(
            0L, new S3StreamObject(0L, 128, STREAM0, 10L, 100L));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 1L, StreamState.OPENED, 0, 10L, ranges, streamObjects);

        BrokerS3WALMetadataImage walMetadataImage0 = new BrokerS3WALMetadataImage(BROKER0, new SortedWALObjectsList(List.of(
                new S3WALObject(1L, BROKER0, Map.of(
                        STREAM1, List.of(new StreamOffsetRange(STREAM1, 0L, 100L))), 1L),
                new S3WALObject(2L, BROKER0, Map.of(
                        STREAM2, List.of(new StreamOffsetRange(STREAM2, 0L, 100L))), 1L))));

        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
            Map.of(BROKER0, walMetadataImage0));
        image0 = new MetadataImage(new MetadataProvenance(0, 0, 0), null, null, null, null, null, null, null, streamsImage, objectsImage, null);

        ranges = new HashMap<>(ranges);
        ranges.put(1, new RangeMetadata(STREAM0, 1L, 1, 100L, 150L, BROKER0));
        streamObjects = new HashMap<>(streamObjects);
        streamObjects.put(1L, new S3StreamObject(1L, 128, STREAM0, 100L, 150L));
        streamImage = new S3StreamMetadataImage(STREAM0, 2L, StreamState.OPENED, 1, 10L, ranges, streamObjects);
        streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
            Map.of(BROKER0, BrokerS3WALMetadataImage.EMPTY));
        image1 = new MetadataImage(new MetadataProvenance(1, 1, 1), null, null, null, null, null, null, null, streamsImage, objectsImage, null);

        ranges = new HashMap<>(ranges);
        ranges.put(2, new RangeMetadata(STREAM0, 2L, 2, 150L, 200L, BROKER0));
        streamObjects = new HashMap<>(streamObjects);
        streamObjects.put(2L, new S3StreamObject(2L, 128, STREAM0, 150L, 200L));
        streamImage = new S3StreamMetadataImage(STREAM0, 3L, StreamState.OPENED, 2, 10L, ranges, streamObjects);
        streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
            Map.of(BROKER0, BrokerS3WALMetadataImage.EMPTY));
        image2 = new MetadataImage(new MetadataProvenance(2, 2, 2), null, null, null, null, null, null, null, streamsImage, objectsImage, null);
    }

    @Test
    public void testFetch() throws Exception {
        S3ObjectMetadata object0 = new S3ObjectMetadata(0L, 128, S3ObjectType.STREAM);
        S3ObjectMetadata object1 = new S3ObjectMetadata(1L, 128, S3ObjectType.STREAM);
        S3ObjectMetadata object2 = new S3ObjectMetadata(2L, 128, S3ObjectType.STREAM);

        this.streamMetadataListener.onChange(null, image0);

        // 1. normal fetch
        CompletableFuture<InRangeObjects> result = this.manager.fetch(STREAM0, 10L, 100L, 5);
        InRangeObjects inRangeObjects = result.get();
        assertEquals(STREAM0, inRangeObjects.streamId());
        assertEquals(10L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(object0, inRangeObjects.objects().get(0));

        // 2. fetch with invalid streamId
        result = this.manager.fetch(STREAM1, 0L, 100L, 5);
        inRangeObjects = result.get();
        assertEquals(InRangeObjects.INVALID, inRangeObjects);

        // 3. fetch with larger startOffset
        result = this.manager.fetch(STREAM0, 20L, 100L, 5);
        inRangeObjects = result.get();
        assertEquals(STREAM0, inRangeObjects.streamId());
        assertEquals(20L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(object0, inRangeObjects.objects().get(0));

        // 4. fetch with smaller endOffset
        result = this.manager.fetch(STREAM0, 10L, 50L, 5);
        inRangeObjects = result.get();
        assertEquals(STREAM0, inRangeObjects.streamId());
        assertEquals(10L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(object0, inRangeObjects.objects().get(0));

        // 5. fetch with smaller startOffset
        result = this.manager.fetch(STREAM0, 5L, 100L, 5);
        inRangeObjects = result.get();
        assertEquals(InRangeObjects.INVALID, inRangeObjects);

        // 6. fetch with larger endOffset
        result = this.manager.fetch(STREAM0, 10L, 200L, 5);
        CompletableFuture<InRangeObjects> finalResult = result;
        assertThrows(TimeoutException.class, () -> {
            finalResult.get(1, TimeUnit.SECONDS);
        });

        // 7. notify the manager that streams' end offset has been advanced
        streamMetadataListener.onChange(null, image1);

        assertThrows(TimeoutException.class, () -> {
            finalResult.get(1, TimeUnit.SECONDS);
        });

        // 8. notify with correct end offset
        streamMetadataListener.onChange(null, image2);

        assertDoesNotThrow(() -> {
            InRangeObjects rangeObjects = finalResult.get(1, TimeUnit.SECONDS);
            assertEquals(STREAM0, rangeObjects.streamId());
            assertEquals(10L, rangeObjects.startOffset());
            assertEquals(200L, rangeObjects.endOffset());
            assertEquals(3, rangeObjects.objects().size());
            assertEquals(object0, rangeObjects.objects().get(0));
            assertEquals(object1, rangeObjects.objects().get(1));
            assertEquals(object2, rangeObjects.objects().get(2));
        });

    }

    @Test
    public void testGetWALObjects() {
        this.streamMetadataListener.onChange(null, image0);
        List<S3WALObjectMetadata> objectMetadata = this.manager.getWALObjects();
        List<S3ObjectMetadata> expected = List.of(new S3ObjectMetadata(1L, 128, S3ObjectType.UNKNOWN),
                new S3ObjectMetadata(2L, 128, S3ObjectType.UNKNOWN));
        // compare objectMetadata with expected
        assertEquals(expected.size(), objectMetadata.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), objectMetadata.get(i).getObjectMetadata());
        }
    }

}
