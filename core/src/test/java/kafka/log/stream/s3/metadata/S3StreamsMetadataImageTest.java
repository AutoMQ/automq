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
package kafka.log.stream.s3.metadata;

import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.image.DeltaList;
import org.apache.kafka.image.NodeS3StreamSetObjectMetadataImage;
import org.apache.kafka.image.RegistryRef;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.image.TopicIdPartition;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.blockcache.DefaultObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.index.lazy.StreamSetObjectRangeIndex;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.google.common.base.Preconditions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;


@Tag("S3Unit")
public class S3StreamsMetadataImageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamsMetadataImageTest.class);

    private static class NodeData {
        private final List<S3StreamSetObject> streamSetObjects;

        public NodeData(int nodeId, List<S3StreamSetObject> streamSetObjects) {
            this.streamSetObjects = streamSetObjects.stream()
                .map(sso ->
                    new S3StreamSetObject(sso.objectId(), sso.nodeId(), Bytes.EMPTY, sso.orderId(), sso.dataTimeInMs()))
                .collect(Collectors.toList());
            this.streamSetObjects.sort(Comparator.comparingLong(S3StreamSetObject::orderId));
        }

        public List<S3StreamSetObject> getStreamSetObjects() {
            return streamSetObjects;
        }
    }

    private static class GeneratedStreamMetadata implements S3StreamsMetadataImage.RangeGetter {
        private final List<S3StreamObject> streamObjects;
        private final Map<Integer, NodeData> nodeData;
        private final Map<Long, List<StreamOffsetRange>> ssoRanges;
        private final StreamMetadataManager.DefaultRangeGetter rangeGetter;
        private final Random r;

        public GeneratedStreamMetadata(Random r, List<S3StreamObject> streamObjects, Map<Integer, NodeData> nodeData, Map<Long, List<StreamOffsetRange>> ssoRanges) {
            RegistryRef registryRef = new RegistryRef();
            this.r = r;
            TimelineHashMap<Long, S3Object> object = new TimelineHashMap<>(registryRef.registry(), 0);
            ssoRanges.keySet().forEach(id -> object.put(id, new S3Object(id, 0, 0, S3ObjectState.COMMITTED, 0)));
            S3ObjectsImage image = new S3ObjectsImage(10000, object, registryRef);
            MemoryObjectStorage memoryObjectStorage = new MemoryObjectStorage();
            ObjectReaderFactory factory = new DefaultObjectReaderFactory(memoryObjectStorage) {
                @Override
                public synchronized ObjectReader get(S3ObjectMetadata metadata) {
                    return new ObjectReader() {
                        @Override
                        public S3ObjectMetadata metadata() {
                            return metadata;
                        }

                        @Override
                        public String objectKey() {
                            return "";
                        }

                        @Override
                        public CompletableFuture<BasicObjectInfo> basicObjectInfo() {
                            return CompletableFuture.completedFuture(new BasicObjectInfo(0, null) {
                                @Override
                                public IndexBlock indexBlock() {
                                    return new IndexBlock(metadata, ByteBufAlloc.byteBuffer(10)) {
                                        @Override
                                        public Optional<StreamOffsetRange> findStreamOffsetRange(long streamId) {
                                            List<StreamOffsetRange> streamOffsetRanges = GeneratedStreamMetadata.this.getSsoRanges().get(metadata.objectId());
                                            return streamOffsetRanges.stream()
                                                .filter(range -> range.streamId() == streamId).findAny();
                                        }

                                        @Override
                                        public List<StreamOffsetRange> streamOffsetRanges() {
                                            return GeneratedStreamMetadata.this.getSsoRanges().get(metadata.objectId());
                                        }
                                    };
                                }
                            });
                        }

                        @Override
                        public CompletableFuture<DataBlockGroup> read(ReadOptions readOptions, DataBlockIndex block) {
                            return null;
                        }

                        @Override
                        public ObjectReader retain() {
                            return null;
                        }

                        @Override
                        public ObjectReader release() {
                            return null;
                        }

                        @Override
                        public void close() {

                        }

                        @Override
                        public CompletableFuture<Integer> size() {
                            return null;
                        }
                    };
                }
            };
            StreamMetadataManager.DefaultRangeGetter rangeGetter = new StreamMetadataManager.DefaultRangeGetter(image, factory);

            this.rangeGetter = rangeGetter;
            this.streamObjects = streamObjects;
            this.nodeData = nodeData;
            this.ssoRanges = ssoRanges;
        }

        public List<S3StreamObject> getStreamObjects() {
            return streamObjects;
        }

        public Map<Integer, NodeData> getNodeData() {
            return new TreeMap<>(nodeData);
        }

        public Map<Long, List<StreamOffsetRange>> getSsoRanges() {
            return ssoRanges;
        }

        private static final Object DUMMY_OBJECT = new Object();
        private ConcurrentHashMap<Long, Object> accessed = new ConcurrentHashMap<>();

        public S3StreamsMetadataImage.GetObjectsContext getObjectsContext;

        @Override
        public void attachGetObjectsContext(S3StreamsMetadataImage.GetObjectsContext ctx) {
            this.rangeGetter.attachGetObjectsContext(ctx);
            this.getObjectsContext = ctx;
        }

        @Override
        public CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId, long nodeId, long orderId) {
            return this.rangeGetter.find(objectId, streamId, nodeId, orderId)
                .thenCompose(
                    r -> {
                        if (accessed.contains(objectId)) {
                            return CompletableFuture.completedFuture(r);
                        }

                        accessed.put(objectId, DUMMY_OBJECT);
                        if (this.r.nextDouble() > 0.75) {
                            return new CompletableFuture<Optional<StreamOffsetRange>>()
                                .completeOnTimeout(r, 1, TimeUnit.MILLISECONDS);
                        } else {
                            return CompletableFuture.completedFuture(r);
                        }
                    }

                );
        }

        @Override
        public CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not implemented"));
        }
    }

    public static class GeneratorResult {
        long seed;
        S3StreamsMetadataImage image;
        GeneratedStreamMetadata generatedStreamMetadata;

        public GeneratorResult(long seed, S3StreamsMetadataImage image, GeneratedStreamMetadata generatedStreamMetadata) {
            this.seed = seed;
            this.image = image;
            this.generatedStreamMetadata = generatedStreamMetadata;
        }
    }

    @SuppressWarnings("NPathComplexity")
    public static S3StreamsMetadataImageTest.GeneratorResult generate(
        long randomSeed,
        Random random,
        long streamId, long totalLength, int numNodes, int maxSegmentSize,
        double streamSetObjectProbability, double nodeMigrationProbability, int maxStreamPerSSO,
        double streamSetObjectNotContainsStreamProbability) {

        List<S3StreamObject> streamObjects = new ArrayList<>();
        Map<Integer, List<S3StreamSetObject>> nodeObjectsMap = new HashMap<>();
        Map<Integer, List<S3StreamSetObject>> notStreamNodeObjectsMap = new HashMap<>();

        List<RangeMetadata> rangeMetadatas = new ArrayList<>();

        long currentOffset = 0L;
        int nextObjectId = 0;
        int rangeIndex = 0;
        long nextEpoch = 0;

        int currentNodeId = numNodes <= 0 ? -1 : random.nextInt(numNodes);

        RangeMetadata rangeMetadata = new RangeMetadata(streamId, nextEpoch, rangeIndex,
            currentOffset, currentOffset, currentNodeId);
        while (currentOffset < totalLength) {
            if (random.nextDouble() < streamSetObjectNotContainsStreamProbability) {
                notStreamNodeObjectsMap.computeIfAbsent(currentNodeId, k -> new ArrayList<>())
                    .add(new S3StreamSetObject(nextObjectId, random.nextInt(numNodes), new ArrayList<>(), nextObjectId));
                nextObjectId++;
            }

            long segmentLen = random.nextInt(maxSegmentSize - 20 + 1) + 20;
            long startOffset = currentOffset;
            long endOffset = Math.min(startOffset + segmentLen, totalLength);

            if (random.nextDouble() < streamSetObjectProbability) {

                if (numNodes > 1 && random.nextDouble() < nodeMigrationProbability) {
                    int newNodeId;
                    do {
                        newNodeId = random.nextInt(numNodes);
                    } while (newNodeId == currentNodeId);
                    currentNodeId = newNodeId;
                }

                int nodeId = currentNodeId;

                if (rangeMetadata.nodeId() != nodeId) {
                    // set endOffset
                    rangeMetadata = new RangeMetadata(rangeMetadata.streamId(), rangeMetadata.epoch(), rangeIndex, rangeMetadata.startOffset(), startOffset, rangeMetadata.nodeId());
                    rangeMetadatas.add(rangeMetadata);
                    rangeIndex++;

                    // create new rangeMetadata
                    rangeMetadata = new RangeMetadata(rangeMetadata.streamId(), rangeMetadata.epoch() + 1, rangeIndex,
                        rangeMetadata.endOffset(), endOffset, nodeId);
                }

                int orderId = nextObjectId;
                S3StreamSetObject sso = new S3StreamSetObject(nextObjectId, nodeId, List.of(new StreamOffsetRange(streamId, startOffset, endOffset)), orderId);

                nodeObjectsMap.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(sso);
            } else {
                S3StreamObject so = new S3StreamObject(nextObjectId, streamId, startOffset, endOffset);
                streamObjects.add(so);
            }

            nextObjectId++;
            currentOffset = endOffset;
        }

        if (rangeMetadata.startOffset() != totalLength) {
            rangeMetadata = new RangeMetadata(rangeMetadata.streamId(), rangeMetadata.epoch(), rangeIndex, rangeMetadata.startOffset(), totalLength, rangeMetadata.nodeId());
            rangeMetadatas.add(rangeMetadata);
        }

        Map<Integer, NodeData> finalNodeData = new HashMap<>();
        for (Map.Entry<Integer, List<S3StreamSetObject>> entry : nodeObjectsMap.entrySet()) {
            finalNodeData.put(entry.getKey(), new NodeData(entry.getKey(), entry.getValue()));
        }

        RegistryRef ref = new RegistryRef();

        DeltaList<S3StreamObject> streamObject = new DeltaList<S3StreamObject>();
        streamObjects.forEach(streamObject::add);
        S3StreamMetadataImage streamsMetadataImage = new S3StreamMetadataImage(streamId, 1, StreamState.OPENED,
            new S3StreamRecord.TagCollection(),
            0, rangeMetadatas, streamObject);

        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataImageTimelineHashMap =
            new TimelineHashMap<>(ref.registry(), 10000);
        streamMetadataImageTimelineHashMap.put(streamId, streamsMetadataImage);

        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap =
            new TimelineHashMap<>(ref.registry(), 10000);

        Map<Long /*objectId*/, List<StreamOffsetRange>> ranges = new HashMap<>();
        nodeObjectsMap.entrySet().forEach(entry -> {
            Integer nodeId = entry.getKey();
            List<S3StreamSetObject> objects = entry.getValue();

            DeltaList<S3StreamSetObject> s3StreamSetObjectDeltaList = new DeltaList<>();
            for (S3StreamSetObject object : objects) {
                S3StreamsMetadataImageTest.StreamSetObjectRange streamSetObjectRange = fillRandomStreamRangeInfo(streamId, random, maxStreamPerSSO, object);
                ranges.put(object.objectId(), streamSetObjectRange.sfrs);
                s3StreamSetObjectDeltaList.add(streamSetObjectRange.s3StreamSetObject);
            }

            List<S3StreamSetObject> notStreamNodeObjects = notStreamNodeObjectsMap.get(nodeId);
            if (notStreamNodeObjects != null && !notStreamNodeObjects.isEmpty()) {
                notStreamNodeObjects.forEach(sso -> {
                    S3StreamsMetadataImageTest.StreamSetObjectRange streamSetObjectRange = fillRandomStreamRangeInfo(streamId, random, maxStreamPerSSO, sso);
                    ranges.put(sso.objectId(), streamSetObjectRange.sfrs);
                    s3StreamSetObjectDeltaList.add(streamSetObjectRange.s3StreamSetObject);
                });
            }

            NodeS3StreamSetObjectMetadataImage image =
                new NodeS3StreamSetObjectMetadataImage(nodeId, 2, s3StreamSetObjectDeltaList);

            nodeMetadataMap.put(nodeId, image);
        });


        TimelineHashMap<TopicIdPartition, Set<Long>> partition2streams =
            new TimelineHashMap<>(ref.registry(), 10000);

        TimelineHashMap<Long, TopicIdPartition> stream2partition =
            new TimelineHashMap<>(ref.registry(), 10000);

        TimelineHashMap<Long, Long> streamEndOffsets =
            new TimelineHashMap<>(ref.registry(), 10000);

        streamEndOffsets.put(streamId, currentOffset);


        S3StreamsMetadataImage image = new S3StreamsMetadataImage(streamId + 1, ref, streamMetadataImageTimelineHashMap,
            nodeMetadataMap, partition2streams, stream2partition, streamEndOffsets);

        GeneratedStreamMetadata generatedStreamMetadata = new GeneratedStreamMetadata(random, streamObjects, finalNodeData, ranges);

        LOGGER.info("total stream object num: {}", streamObjects.size());
        LOGGER.info("total stream set object num: {}", notStreamNodeObjectsMap.values().stream().mapToInt(List::size).sum() +
            nodeObjectsMap.values().stream().mapToInt(List::size).sum());
        LOGGER.info("total range num: {}", rangeMetadatas.size());
        LOGGER.info("node2sso num: ");

        nodeObjectsMap.entrySet().forEach(entry -> {
            LOGGER.info("nodeId: {}", entry.getKey());
            LOGGER.info("sso num: {}", entry.getValue().size() + notStreamNodeObjectsMap.getOrDefault(entry.getKey(), List.of()).size());
        });
        return new S3StreamsMetadataImageTest.GeneratorResult(randomSeed, image, generatedStreamMetadata);
    }

    public static List<S3ObjectMetadata> getS3ObjectMetadata(long streamId, GeneratedStreamMetadata metadata) {
        List<S3ObjectMetadata> allObjects = new ArrayList<>();
        for (S3StreamObject so : metadata.getStreamObjects()) {
            List<StreamOffsetRange> ranges = Collections.singletonList(new StreamOffsetRange(so.streamId(), so.startOffset(), so.endOffset()));
            allObjects.add(new S3ObjectMetadata(so.objectId(), S3ObjectType.STREAM, ranges,
                S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_OBJECT_SIZE,
                S3StreamConstant.INVALID_ORDER_ID));
        }
        for (Map.Entry<Integer, NodeData> entry : metadata.getNodeData().entrySet()) {
            for (S3StreamSetObject sso : entry.getValue().getStreamSetObjects()) {
                allObjects.add(new S3ObjectMetadata(sso.objectId(), S3ObjectType.STREAM_SET, find(metadata.getSsoRanges().get(sso.objectId()), streamId),
                    sso.dataTimeInMs(), sso.dataTimeInMs(), sso.orderId(), sso.nodeId()));
            }
        }
        return allObjects;
    }

    public static List<StreamOffsetRange> find(List<StreamOffsetRange> list, long streamId) {
        return list.stream().filter(r -> r.streamId() == streamId).collect(Collectors.toList());
    }

    public static class StreamSetObjectRange {
        S3StreamSetObject s3StreamSetObject;
        List<StreamOffsetRange> sfrs;

        public StreamSetObjectRange(S3StreamSetObject s3StreamSetObject, List<StreamOffsetRange> sfrs) {
            this.s3StreamSetObject = s3StreamSetObject;
            this.sfrs = sfrs;
        }
    }

    public static S3StreamsMetadataImageTest.StreamSetObjectRange fillRandomStreamRangeInfo(long streamId, Random r, int maxStreamPerSso, S3StreamSetObject s3StreamSetObject) {
        S3StreamSetObject object = new S3StreamSetObject(s3StreamSetObject.objectId(), s3StreamSetObject.nodeId(), Bytes.EMPTY, s3StreamSetObject.orderId(), s3StreamSetObject.dataTimeInMs());

        Set<Long> generatedStream = new HashSet<>();
        generatedStream.add(streamId);
        List<StreamOffsetRange> sfrs = new ArrayList<>(s3StreamSetObject.offsetRangeList());
        for (int i = 0; i < maxStreamPerSso; i++) {
            long startOffset = r.nextLong(0, Long.MAX_VALUE / 2);
            long gStreamId;
            do {
                gStreamId = r.nextInt(0, (int) streamId * 2);
            } while (generatedStream.contains(gStreamId));
            sfrs.add(new StreamOffsetRange(gStreamId, startOffset,
                startOffset + r.nextLong(0, 1024)));
        }

        Collections.sort(sfrs);

        return new S3StreamsMetadataImageTest.StreamSetObjectRange(object, sfrs);
    }

    public static void check(long now, List<Long> seedList, int startOffset, int endOffset, List<S3ObjectMetadata> allObjects, InRangeObjects inRangeObjects, long costMs) {
        LOGGER.info("=====check==========================");
        LOGGER.info("seed is {}", now);
        LOGGER.info("seedList is seedList: {}", seedList);
        LOGGER.info("startOffset is {}", startOffset);
        LOGGER.info("endOffset is {}", endOffset);
        LOGGER.info("cost={}ms", costMs);
        try {
            Preconditions.checkArgument(allObjects.size() == inRangeObjects.objects().size(), "result size not equal: allObjects: " + allObjects.size() + ", result: " + inRangeObjects.objects().size());

            for (int i = 0; i < allObjects.size(); i++) {
                S3ObjectMetadata metadata = allObjects.get(i);
                S3ObjectMetadata metadata1 = inRangeObjects.objects().get(i);

                Assertions.assertEquals(metadata.objectId(), metadata1.objectId(), "objectId not equal at " + i);
                Assertions.assertEquals(metadata.getType(), metadata1.getType(), "object Type not equal at " + i);
                Assertions.assertEquals(metadata.startOffset(), metadata1.startOffset(), "startOffset not equal at " + i);
                Assertions.assertEquals(metadata.endOffset(), metadata1.endOffset(), "endOffset not equal at " + i);
            }
        } catch (IllegalArgumentException e) {
            Assertions.fail(e);
        }

    }

    public static List<S3ObjectMetadata> slice(List<S3ObjectMetadata> allObjects, long startOffset, long endOffset, int limit) {
        ArrayList<S3ObjectMetadata> ans = new ArrayList<>();
        for (S3ObjectMetadata object : allObjects) {
            if (object.endOffset() > startOffset && object.startOffset() < endOffset && ans.size() < limit) {
                ans.add(object);
            }
        }

        return ans;
    }

    private static void enableStreamSetObjectRangeIndex() {
        try {
            Class<?> clazz = StreamSetObjectRangeIndex.class;
            // 2. 获取 ENABLED 字段
            Field field = clazz.getDeclaredField("ENABLED");

            // 3. 确保我们可以访问该字段，即使它是私有的
            field.setAccessible(true);

            // 4. 去掉字段的 final 修饰符
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            // 5. 设置字段的新值
            field.set(null, true);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testGetObjectsResult() {
        Random random = new Random();
        ArrayList<Long> seedList = new ArrayList<>();
        enableStreamSetObjectRangeIndex();

        StreamSetObjectRangeIndex.getInstance().clear();

        StreamMetadataManager.DefaultRangeGetter.STREAM_ID_BLOOM_FILTER.clear();
        long streamId = 420000L;
        long totalStreamLength = 50000L;
        int numberOfNodes = 10;
        int maxSegmentSize = 100;
        double streamSetObjectProbability = 0.7;
        double nodeMigrationProbability = 0.2;
        double streamSetObjectNotContainsStreamProbability = 0.5;

        long now = System.currentTimeMillis();
        seedList.add(now);
        random.setSeed(now);

        S3StreamsMetadataImageTest.GeneratorResult generatorResult = generate(now, random,
            streamId, totalStreamLength, numberOfNodes, maxSegmentSize,
            streamSetObjectProbability, nodeMigrationProbability,
            10000, streamSetObjectNotContainsStreamProbability
        );

        List<S3ObjectMetadata> allObjects = getS3ObjectMetadata(streamId, generatorResult.generatedStreamMetadata);

        allObjects.sort(Comparator.comparingLong(S3ObjectMetadata::startOffset));

        List<S3ObjectMetadata> originResult = allObjects;

        CompletableFuture<InRangeObjects> objects = null;
        objects = generatorResult.image.getObjects(streamId, 0, totalStreamLength, Integer.MAX_VALUE, generatorResult.generatedStreamMetadata);
        InRangeObjects inRangeObjects = null;
        try {
            inRangeObjects = objects.get();
        } catch (Exception e) {
            LOGGER.error("seed is {}", now);
            LOGGER.error("seedList is seedList: {}", seedList);
            Assertions.fail(e);
        }
        System.out.println(generatorResult.generatedStreamMetadata.getObjectsContext.dumpStatistics());

        check(now, seedList, 0, (int) totalStreamLength, originResult, inRangeObjects, 0);


        int limit = 4;
        for (int i = 0; i < 1000; i++) {
            long startMs = System.nanoTime();
            int startOffset = random.nextInt((int) totalStreamLength);
            int endOffset = random.nextInt(startOffset, (int) totalStreamLength);

            objects = generatorResult.image.getObjects(streamId, startOffset, endOffset, limit, generatorResult.generatedStreamMetadata);

            try {
                inRangeObjects = objects.get();
            } catch (Exception e) {
                LOGGER.error("seed is {}", now);
                LOGGER.error("seedList is seedList: {}", seedList);
                Assertions.fail(e);
            }

            long costMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startMs, TimeUnit.NANOSECONDS);
            check(now, seedList, startOffset, endOffset, slice(originResult, startOffset, endOffset, limit), inRangeObjects, costMs);
        }
    }
}
