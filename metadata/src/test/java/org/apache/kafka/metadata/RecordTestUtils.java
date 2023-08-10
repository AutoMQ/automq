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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.internals.MemoryBatchReader;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.util.MockRandom;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Utilities for testing classes that deal with metadata records.
 */
public class RecordTestUtils {
    /**
     * Replay a list of records.
     *
     * @param target                The object to invoke the replay function on.
     * @param recordsAndVersions    A list of records.
     */
    public static void replayAll(Object target,
                                 List<ApiMessageAndVersion> recordsAndVersions) {
        for (ApiMessageAndVersion recordAndVersion : recordsAndVersions) {
            ApiMessage record = recordAndVersion.message();
            try {
                try {
                    Method method = target.getClass().getMethod("replay", record.getClass());
                    method.invoke(target, record);
                } catch (NoSuchMethodException e) {
                    try {
                        Method method = target.getClass().getMethod("replay",
                            record.getClass(),
                            long.class);
                        method.invoke(target, record, 0L);
                    } catch (NoSuchMethodException i) {
                        // ignore
                    }
                }
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e.getCause());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void replayOne(
        Object target,
        ApiMessageAndVersion recordAndVersion
    ) {
        replayAll(target, Collections.singletonList(recordAndVersion));
    }

    public static class TestThroughAllIntermediateImagesLeadingToFinalImageHelper<D, I> {
        private final Supplier<I> emptyImageSupplier;
        private final Function<I, D> deltaUponImageCreator;

        public TestThroughAllIntermediateImagesLeadingToFinalImageHelper(
            Supplier<I> emptyImageSupplier, Function<I, D> deltaUponImageCreator
        ) {
            this.emptyImageSupplier = Objects.requireNonNull(emptyImageSupplier);
            this.deltaUponImageCreator = Objects.requireNonNull(deltaUponImageCreator);
        }

        public I getEmptyImage() {
            return this.emptyImageSupplier.get();
        }

        public D createDeltaUponImage(I image) {
            return this.deltaUponImageCreator.apply(image);
        }

        @SuppressWarnings("unchecked")
        public I createImageByApplyingDelta(D delta) {
            try {
                try {
                    Method method = delta.getClass().getMethod("apply");
                    return (I) method.invoke(delta);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e.getCause());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public void test(I finalImage, List<ApiMessageAndVersion> fromRecords) {
            for (int numRecordsForfirstImage = 1; numRecordsForfirstImage <= fromRecords.size(); ++numRecordsForfirstImage) {
                // create first image from first numRecordsForfirstImage records
                D delta = createDeltaUponImage(getEmptyImage());
                RecordTestUtils.replayAll(delta, fromRecords.subList(0, numRecordsForfirstImage));
                I firstImage = createImageByApplyingDelta(delta);
                // for all possible further batch sizes, apply as many batches as it takes to get to the final image
                int remainingRecords = fromRecords.size() - numRecordsForfirstImage;
                if (remainingRecords == 0) {
                    assertEquals(finalImage, firstImage);
                } else {
                    // for all possible further batch sizes...
                    for (int maxRecordsForSuccessiveBatches = 1; maxRecordsForSuccessiveBatches <= remainingRecords; ++maxRecordsForSuccessiveBatches) {
                        I latestIntermediateImage = firstImage;
                        // ... apply as many batches as it takes to get to the final image
                        int numAdditionalBatches = (int) Math.ceil(remainingRecords * 1.0 / maxRecordsForSuccessiveBatches);
                        for (int additionalBatchNum = 0; additionalBatchNum < numAdditionalBatches; ++additionalBatchNum) {
                            // apply up to maxRecordsForSuccessiveBatches records on top of the latest intermediate image
                            // to obtain the next intermediate image.
                            delta = createDeltaUponImage(latestIntermediateImage);
                            int applyFromIndex = numRecordsForfirstImage + additionalBatchNum * maxRecordsForSuccessiveBatches;
                            int applyToIndex = Math.min(fromRecords.size(), applyFromIndex + maxRecordsForSuccessiveBatches);
                            RecordTestUtils.replayAll(delta, fromRecords.subList(applyFromIndex, applyToIndex));
                            latestIntermediateImage = createImageByApplyingDelta(delta);
                        }
                        // The final intermediate image received should be the expected final image
                        assertEquals(finalImage, latestIntermediateImage);
                    }
                }
            }
        }
    }

    /**
     * Replay a list of record batches.
     *
     * @param target        The object to invoke the replay function on.
     * @param batches       A list of batches of records.
     */
    public static void replayAllBatches(Object target,
                                        List<List<ApiMessageAndVersion>> batches) {
        for (List<ApiMessageAndVersion> batch : batches) {
            replayAll(target, batch);
        }
    }

    /**
     * Materialize the output of an iterator into a set.
     *
     * @param iterator      The input iterator.
     *
     * @return              The output set.
     */
    public static <T> Set<T> iteratorToSet(Iterator<T> iterator) {
        HashSet<T> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    /**
     * Assert that a batch iterator yields a given set of record batches.
     *
     * @param batches       A list of record batches.
     * @param iterator      The input iterator.
     */
    public static void assertBatchIteratorContains(List<List<ApiMessageAndVersion>> batches,
                                                   Iterator<List<ApiMessageAndVersion>> iterator) throws Exception {
        List<List<ApiMessageAndVersion>> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(new ArrayList<>(iterator.next()));
        }
        deepSortRecords(actual);
        List<List<ApiMessageAndVersion>> expected = new ArrayList<>();
        for (List<ApiMessageAndVersion> batch : batches) {
            expected.add(new ArrayList<>(batch));
        }
        deepSortRecords(expected);
        assertEquals(expected, actual);
    }

    /**
     * Sort the contents of an object which contains records.
     *
     * @param o     The input object. It will be modified in-place.
     */
    @SuppressWarnings("unchecked")
    public static void deepSortRecords(Object o) throws Exception {
        if (o == null) {
            return;
        } else if (o instanceof List) {
            List<?> list = (List<?>) o;
            for (Object entry : list) {
                if (entry != null) {
                    if (Number.class.isAssignableFrom(entry.getClass())) {
                        return;
                    }
                    deepSortRecords(entry);
                }
            }
            list.sort(Comparator.comparing(Object::toString));
        } else if (o instanceof ImplicitLinkedHashCollection) {
            ImplicitLinkedHashCollection<?> coll = (ImplicitLinkedHashCollection<?>) o;
            for (Object entry : coll) {
                deepSortRecords(entry);
            }
            coll.sort(Comparator.comparing(Object::toString));
        } else if (o instanceof Message || o instanceof ApiMessageAndVersion) {
            for (Field field : o.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                deepSortRecords(field.get(o));
            }
        }
    }

    /**
     * Create a batch reader for testing.
     *
     * @param lastOffset the last offset of the given list of records
     * @param appendTimestamp the append timestamp for the batches created
     * @param records the records
     * @return a batch reader which will return the given records
     */
    public static BatchReader<ApiMessageAndVersion> mockBatchReader(
        long lastOffset,
        long appendTimestamp,
        List<ApiMessageAndVersion> records
    ) {
        List<Batch<ApiMessageAndVersion>> batches = new ArrayList<>();
        long offset = lastOffset - records.size() + 1;
        Iterator<ApiMessageAndVersion> iterator = records.iterator();
        List<ApiMessageAndVersion> curRecords = new ArrayList<>();
        assertTrue(iterator.hasNext()); // At least one record is required
        while (true) {
            if (!iterator.hasNext() || curRecords.size() >= 2) {
                batches.add(Batch.data(offset, 0, appendTimestamp, sizeInBytes(curRecords), curRecords));
                if (!iterator.hasNext()) {
                    break;
                }
                offset += curRecords.size();
                curRecords = new ArrayList<>();
            }
            curRecords.add(iterator.next());
        }
        return MemoryBatchReader.of(batches, __ -> { });
    }


    private static int sizeInBytes(List<ApiMessageAndVersion> records) {
        int size = 0;
        for (ApiMessageAndVersion record : records) {
            ObjectSerializationCache cache = new ObjectSerializationCache();
            size += MetadataRecordSerde.INSTANCE.recordSize(record, cache);
        }
        return size;
    }

    public static ApiMessageAndVersion testRecord(int index) {
        MockRandom random = new MockRandom(index);
        return new ApiMessageAndVersion(
            new TopicRecord().setName("test" + index).
            setTopicId(new Uuid(random.nextLong(), random.nextLong())), (short) 0);
    }
}
