/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller.stream.lifecycle;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

@Timeout(40)
@Tag("S3Unit")
public class ObjectCleanerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCleanerTest.class);

    private ObjectStorage objectStorage;
    private QuorumController quorumController;

    @BeforeEach
    public void setUp() {
        objectStorage = Mockito.mock(ObjectStorage.class);
        quorumController = Mockito.mock(QuorumController.class);
        Mockito.when(objectStorage.delete(anyList())).then(inv -> {
            return CompletableFuture.completedFuture(null);
        });

    }

    private List<S3Object> prepareCompositeObject(LongSupplier id, int number, ObjectAttributes attributes) {
        long preparedTimeInMs = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        long expiredTimeInMs = preparedTimeInMs + TimeUnit.MINUTES.toMillis(10);
        long committedTimeInMs = preparedTimeInMs + TimeUnit.MILLISECONDS.toMillis(20);
        long markDestroyedTimeInMs = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(120);

        ArrayList<S3Object> list = new ArrayList<>(number);

        for (int i = 0; i < number; i++) {
            long objectId = id.getAsLong();
            list.add(new S3Object(objectId,
                1024,
                ObjectUtils.genKey(0, objectId),
                preparedTimeInMs,
                expiredTimeInMs,
                committedTimeInMs,
                markDestroyedTimeInMs,
                S3ObjectState.MARK_DESTROYED,
                attributes.attributes()));
        }

        return list;
    }

    @Test
    public void testDeepDeleteShouldDeleteAllCompositeObjectInBatch() throws ExecutionException, InterruptedException {
        AtomicLong totalDeleteObjectNumber = new AtomicLong();
        AtomicLong deleteObjectMethodCallNumber = new AtomicLong();
        Set<Long> deleteObjectId = new ConcurrentSkipListSet<>();


        Mockito.when(objectStorage.delete(anyList())).then(inv -> {
            List<ObjectStorage.ObjectPath> argument = inv.getArgument(0);
            // check no duplicate delete
            for (ObjectStorage.ObjectPath objectPath : argument) {
                assertTrue(deleteObjectId.add(objectPath.getObjectId()));
            }

            // record all deleteObject number
            totalDeleteObjectNumber.addAndGet(argument.size());

            // record deleteObjects method call number
            deleteObjectMethodCallNumber.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        // generate composite object
        ObjectAttributes objectAttributes = ObjectAttributes.builder()
            .deepDelete().bucket((short) 0)
            .type(ObjectAttributes.Type.Composite)
            .build();

        AtomicLong objectId = new AtomicLong();

        Random r = new Random();
        long seed = System.currentTimeMillis();
        r.setSeed(seed);

        int compositeObjectNumber = r.nextInt(2000);
        int linkedObjectNumberPerCompositeObject = r.nextInt(2000);

        // prepare composite object need to delete
        List<S3Object> compositeObjectList = prepareCompositeObject(objectId::incrementAndGet,
            compositeObjectNumber, objectAttributes);

        AtomicLong totalObjectNotified = new AtomicLong();
        Set<Long> notifiedDeleteObjectId = new ConcurrentSkipListSet<>();

        Mockito.when(quorumController.notifyS3ObjectDeleted(any(), any())).then(inv -> {
            List<Long/*objectId*/> deletedObjectIds = inv.getArgument(1);
            // when delete object inc counter
            totalObjectNotified.addAndGet(deletedObjectIds.size());
            for (Long l : deletedObjectIds) {
                assertTrue(notifiedDeleteObjectId.add(l));
            }
            return CompletableFuture.completedFuture(null);
        });
        ObjectCleaner cleaner = new ObjectCleaner(objectStorage, quorumController);

        AtomicLong readCompositeNumber = new AtomicLong();
        CompletableFuture<Void> clean = cleaner.deepDeleteCompositeObject(compositeObjectList, (metadata, objectStorage) -> {
            List<ObjectStorage.ObjectPath> linkedCompositeObject =
                prepareCompositeObject(objectId::incrementAndGet,
                    linkedObjectNumberPerCompositeObject, objectAttributes).stream().map(s3Object ->
                        new ObjectStorage.ObjectPath(s3Object.bucket(), s3Object.getObjectId()))
                    .collect(Collectors.toList());
            // mock delay
            try {
                Thread.sleep(r.nextInt(10));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            readCompositeNumber.incrementAndGet();
            return CompletableFuture.completedFuture(
                new AbstractMap.SimpleEntry<>(metadata, linkedCompositeObject));
        });

        clean.get();

        LOGGER.info("seed={}, compositeObjectNumber={}, " +
                "linkedObjectNumberPerCompositeObject={}, " +
                "deleteObjectMethodCallNumber={}",
            seed, compositeObjectNumber, linkedObjectNumberPerCompositeObject,
            deleteObjectMethodCallNumber.get());

        // all composited object should receive deletedObject notification
        assertEquals(compositeObjectNumber, totalObjectNotified.get());


        for (Long l : notifiedDeleteObjectId) {
            assertTrue(deleteObjectId.contains(l));
        }

        // all composite object read objectPath once
        assertEquals(compositeObjectNumber, readCompositeNumber.get());
        // all object deleted
        assertEquals(objectId.get(), totalDeleteObjectNumber.get());
        // check all object deleted
        assertEquals(compositeObjectNumber + compositeObjectNumber * linkedObjectNumberPerCompositeObject,
            totalDeleteObjectNumber.get());
    }

    @Test
    public void testDeepDeleteOneCompositeObjectGetLinkedObjectFailNotFailTheWholeRequest() throws ExecutionException, InterruptedException {
        AtomicLong totalDeleteObjectNumber = new AtomicLong();
        AtomicLong deleteObjectMethodCallNumber = new AtomicLong();
        Set<Long> deleteObjectId = new ConcurrentSkipListSet<>();

        Mockito.when(objectStorage.delete(anyList())).then(inv -> {
            List<ObjectStorage.ObjectPath> argument = inv.getArgument(0);
            // check no duplicate delete
            for (ObjectStorage.ObjectPath objectPath : argument) {
                assertTrue(deleteObjectId.add(objectPath.getObjectId()));
            }

            // record all deleteObject number
            totalDeleteObjectNumber.addAndGet(argument.size());

            // record deleteObjects method call number
            deleteObjectMethodCallNumber.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        // generate composite object
        ObjectAttributes objectAttributes = ObjectAttributes.builder()
            .deepDelete().bucket((short) 0)
            .type(ObjectAttributes.Type.Composite)
            .build();

        AtomicLong objectId = new AtomicLong();

        Random r = new Random();
        long seed = System.currentTimeMillis();
        r.setSeed(seed);

        int compositeObjectNumber = r.nextInt(30);
        int linkedObjectNumberPerCompositeObject = r.nextInt(100);

        // prepare composite object need to delete
        List<S3Object> compositeObjectList = prepareCompositeObject(objectId::incrementAndGet,
            compositeObjectNumber, objectAttributes);

        AtomicLong totalObjectNotified = new AtomicLong();
        Set<Long> notifiedDeleteObjectId = new ConcurrentSkipListSet<>();
        Mockito.when(quorumController.notifyS3ObjectDeleted(any(), any())).then(inv -> {
            List<Long/*objectId*/> deletedObjectIds = inv.getArgument(1);
            // when delete object inc counter
            totalObjectNotified.addAndGet(deletedObjectIds.size());
            for (Long l : deletedObjectIds) {
                assertTrue(notifiedDeleteObjectId.add(l));
            }
            return CompletableFuture.completedFuture(null);
        });
        ObjectCleaner cleaner = new ObjectCleaner(objectStorage, quorumController);

        AtomicLong readCompositeNumber = new AtomicLong();
        CompletableFuture<Void> clean = cleaner.deepDeleteCompositeObject(compositeObjectList, (metadata, objectStorage) -> {
            readCompositeNumber.incrementAndGet();

            List<ObjectStorage.ObjectPath> linkedCompositeObject =
                prepareCompositeObject(objectId::incrementAndGet,
                    linkedObjectNumberPerCompositeObject, objectAttributes).stream().map(s3Object ->
                        new ObjectStorage.ObjectPath(s3Object.bucket(), s3Object.getObjectId()))
                    .collect(Collectors.toList());

            // mock exception
            if (metadata.objectId() == 1) {
                return CompletableFuture.failedFuture(new RuntimeException("mock exception get object linked object"));
            }

            return CompletableFuture.completedFuture(
                new AbstractMap.SimpleEntry<>(metadata, linkedCompositeObject));
        });

        clean.get();

        LOGGER.info("seed={}, compositeObjectNumber={}, " +
                "linkedObjectNumberPerCompositeObject={}, " +
                "deleteObjectMethodCallNumber={}",
            seed, compositeObjectNumber, linkedObjectNumberPerCompositeObject,
            deleteObjectMethodCallNumber.get());

        // objectId=1 should not be deleted.
        assertFalse(deleteObjectId.contains((long) 1));
        assertFalse(notifiedDeleteObjectId.contains((long) 1));

        // all composited object should receive deletedObject notification
        assertEquals(compositeObjectNumber - 1, totalObjectNotified.get());
        // all composite object read objectPath once
        assertEquals(compositeObjectNumber, readCompositeNumber.get());
        // all object deleted
        assertEquals(objectId.get() - 1 - linkedObjectNumberPerCompositeObject, totalDeleteObjectNumber.get());
        // check all object deleted
        assertEquals(compositeObjectNumber - 1 + (compositeObjectNumber - 1) * linkedObjectNumberPerCompositeObject,
            totalDeleteObjectNumber.get());
    }
}
