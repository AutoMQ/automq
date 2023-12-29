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

package kafka.log.stream.s3;

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import kafka.log.stream.s3.metadata.MetadataListener;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import kafka.server.metadata.BrokerMetadataListener;
import kafka.server.metadata.KRaftMetadataCache;
import org.apache.kafka.image.DeltaMap;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.NodeS3StreamSetObjectMetadataImage;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    private MetadataListener streamMetadataListener;
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
        }).when(this.mockBrokerMetadataListener).registerMetadataListener(ArgumentMatchers.any());
        Mockito.when(this.mockMetadataCache.currentImage()).thenReturn(MetadataImage.EMPTY);
        KafkaConfig config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(BROKER0);
        this.manager = new StreamMetadataManager(this.mockBroker, config);
    }

    private static MetadataImage image0;
    private static MetadataImage image1;
    private static MetadataImage image2;

    static {
        DeltaMap<Long, S3Object> map = new DeltaMap<>(new int[]{});
        map.putAll(Map.of(
                0L, new S3Object(0L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED),
                1L, new S3Object(1L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED),
                2L, new S3Object(2L, 128, null, -1, -1, -1, -1, S3ObjectState.COMMITTED)
        ));
        S3ObjectsImage objectsImage = new S3ObjectsImage(2L, map);

        Map<Integer, RangeMetadata> ranges = Map.of(
                0, new RangeMetadata(STREAM0, 0L, 0, 10L, 100L, BROKER0)
        );
        Map<Long, S3StreamObject> streamObjects = Map.of(
                0L, new S3StreamObject(0L, STREAM0, 10L, 100L, S3StreamConstant.INVALID_TS));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 1L, StreamState.OPENED, 0, 10L, ranges, streamObjects);

        NodeS3StreamSetObjectMetadataImage walMetadataImage0 = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH, Map.of(
                1L, new S3StreamSetObject(1L, BROKER0, List.of(
                        new StreamOffsetRange(STREAM1, 0L, 100L)), 1L),
                2L, new S3StreamSetObject(2L, BROKER0, List.of(
                        new StreamOffsetRange(STREAM2, 0L, 100L)), 2L)));

        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
                Map.of(BROKER0, walMetadataImage0));
        image0 = new MetadataImage(new MetadataProvenance(0, 0, 0), null, null, null, null, null, null, null, streamsImage, objectsImage, null, null);

        ranges = new HashMap<>(ranges);
        ranges.put(1, new RangeMetadata(STREAM0, 1L, 1, 100L, 150L, BROKER0));
        streamObjects = new HashMap<>(streamObjects);
        streamObjects.put(1L, new S3StreamObject(1L, STREAM0, 100L, 150L, S3StreamConstant.INVALID_TS));
        streamImage = new S3StreamMetadataImage(STREAM0, 2L, StreamState.OPENED, 1, 10L, ranges, streamObjects);
        streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
                Map.of(BROKER0, NodeS3StreamSetObjectMetadataImage.EMPTY));
        image1 = new MetadataImage(new MetadataProvenance(1, 1, 1), null, null, null, null, null, null, null, streamsImage, objectsImage, null, null);

        ranges = new HashMap<>(ranges);
        ranges.put(2, new RangeMetadata(STREAM0, 2L, 2, 150L, 200L, BROKER0));
        streamObjects = new HashMap<>(streamObjects);
        streamObjects.put(2L, new S3StreamObject(2L, STREAM0, 150L, 200L, S3StreamConstant.INVALID_TS));
        streamImage = new S3StreamMetadataImage(STREAM0, 3L, StreamState.OPENED, 2, 10L, ranges, streamObjects);
        streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
                Map.of(BROKER0, NodeS3StreamSetObjectMetadataImage.EMPTY));
        image2 = new MetadataImage(new MetadataProvenance(2, 2, 2), null, null, null, null, null, null, null, streamsImage, objectsImage, null, null);
    }

    @Test
    public void testFetch() throws Exception {

        this.streamMetadataListener.onChange(null, image0);

        // 1. normal fetch
        CompletableFuture<InRangeObjects> result = this.manager.fetch(STREAM0, 10L, 100L, 5);
        InRangeObjects inRangeObjects = result.get();
        assertEquals(STREAM0, inRangeObjects.streamId());
        assertEquals(10L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(0L, inRangeObjects.objects().get(0).objectId());

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
        assertEquals(0L, inRangeObjects.objects().get(0).objectId());

        // 4. fetch with smaller endOffset
        result = this.manager.fetch(STREAM0, 10L, 50L, 5);
        inRangeObjects = result.get();
        assertEquals(STREAM0, inRangeObjects.streamId());
        assertEquals(10L, inRangeObjects.startOffset());
        assertEquals(100L, inRangeObjects.endOffset());
        assertEquals(1, inRangeObjects.objects().size());
        assertEquals(0L, inRangeObjects.objects().get(0).objectId());

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
            assertEquals(0L, rangeObjects.objects().get(0).objectId());
            assertEquals(1L, rangeObjects.objects().get(1).objectId());
            assertEquals(2L, rangeObjects.objects().get(2).objectId());
        });

    }
}
