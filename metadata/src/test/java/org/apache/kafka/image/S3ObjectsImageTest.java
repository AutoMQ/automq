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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.objects.ObjectAttributes;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
@Tag("S3Unit")
public class S3ObjectsImageTest {

    private static S3ObjectsImage getAndRetain(AtomicReference<S3ObjectsImage> current, Object lock) {
        synchronized (lock) {
            S3ObjectsImage image = current.get();
            image.retain();
            return image;
        }
    }

    static final S3ObjectsImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final S3ObjectsDelta DELTA1;

    static final S3ObjectsImage IMAGE2;

    static {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Long, S3Object> map = new TimelineHashMap<>(registry, 10);
        for (int i = 0; i < 4; i++) {
            S3Object object = new S3Object(i, -1, -1, S3ObjectState.PREPARED, ObjectAttributes.DEFAULT.attributes());
            map.put(object.getObjectId(), object);
        }
        registry.getOrCreateSnapshot(0);

        RegistryRef ref1 = new RegistryRef(registry, 0, new ArrayList<>());

        short objectRecordVersion = AutoMQVersion.LATEST.objectRecordVersion();
        int attribute = ObjectAttributes.builder().bucket((short) 1).build().attributes();

        IMAGE1 = new S3ObjectsImage(3, map, ref1);
        DELTA1_RECORDS = new ArrayList<>();
        // try to update object0 and object1 to committed
        // try to make object2 expired and mark it to be destroyed
        // try to remove destroy object3
        // try to add applied object4
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(0L).
            setObjectState((byte) S3ObjectState.COMMITTED.ordinal()).setAttributes(attribute).setObjectSize(233)
            .setTimestamp(2), objectRecordVersion));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(1L).
            setObjectState((byte) S3ObjectState.COMMITTED.ordinal()).setAttributes(attribute), objectRecordVersion));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(2L).
            setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal()).setAttributes(attribute), objectRecordVersion));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveS3ObjectRecord()
            .setObjectId(3L), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new AssignedS3ObjectIdRecord()
            .setAssignedS3ObjectId(4L), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(4L).
            setObjectState((byte) S3ObjectState.PREPARED.ordinal()), objectRecordVersion));
        DELTA1 = new S3ObjectsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Long/*objectId*/, S3Object> map2 = new TimelineHashMap<>(registry, 10);

        RegistryRef ref2 = new RegistryRef(registry, 1, new ArrayList<>());
        map2.put(0L, new S3Object(0L, 233, 2, S3ObjectState.COMMITTED, attribute));
        map2.put(1L, new S3Object(1L, 0,  0, S3ObjectState.COMMITTED, attribute));
        map2.put(2L, new S3Object(2L, 0, 0, S3ObjectState.MARK_DESTROYED, attribute));
        map2.put(4L, new S3Object(4L, 0, 0, S3ObjectState.PREPARED, ObjectAttributes.DEFAULT.attributes()));
        registry.getOrCreateSnapshot(1);
        IMAGE2 = new S3ObjectsImage(4L, map2, ref2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImageAndBack(S3ObjectsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(S3ObjectsImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        S3ObjectsDelta delta = new S3ObjectsDelta(S3ObjectsImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3ObjectsImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

    @Test
    public void testConcurrentRefRetainAndReleaseNotThrowException() throws InterruptedException {
        LogContext logContext = new LogContext("[S3ObjectsImageTest] ");
        Logger log = LoggerFactory.getLogger(getClass());

        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        Object imageLock = new Object();
        AtomicReference<S3ObjectsImage> current = new AtomicReference<>();
        TimelineHashMap<Long/*objectId*/, S3Object> map = new TimelineHashMap<>(registry, 10);
        List<Long> liveEpochs = new ArrayList<>();
        liveEpochs.add(0L);
        RegistryRef ref = new RegistryRef(registry, 0, liveEpochs);

        S3ObjectsImage start = new S3ObjectsImage(4L, map, ref);
        synchronized (imageLock) {
            current.set(start);
        }

        AtomicBoolean running = new AtomicBoolean(true);

        AtomicLong updateExceptionCounter = new AtomicLong();
        AtomicLong accessExceptionCounter = new AtomicLong();
        AtomicLong updateCounter = new AtomicLong();
        AtomicLong accessCounter = new AtomicLong();

        int threadCount = 9; // 1 update task + 8 access tasks
        CountDownLatch startLatch = new CountDownLatch(threadCount);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        ExecutorService es = Executors.newFixedThreadPool(threadCount);

        // Update task
        es.submit(() -> {
            startLatch.countDown();
            try {
                startLatch.await();
                while (running.get()) {
                    S3ObjectsImage previous;
                    synchronized (imageLock) {
                        previous = current.get();
                    }
                    TimeUnit.MILLISECONDS.sleep(1);
                    S3ObjectsImage newImage = new S3ObjectsImage(1, map, ref.next());
                    synchronized (imageLock) {
                        current.set(newImage);
                    }
                    updateCounter.incrementAndGet();
                    TimeUnit.MILLISECONDS.sleep(1);

                    if (previous != newImage) {
                        try {
                            previous.release();
                        } catch (Throwable e) {
                            updateExceptionCounter.incrementAndGet();
                            log.error("Exception in updateImageTask", e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        });


        for (int i = 0; i < 8; i++) {
            es.submit(() -> {
                startLatch.countDown();
                try {
                    startLatch.await();
                    while (running.get()) {
                        S3ObjectsImage image = getAndRetain(current, imageLock);
                        try {
                            TimeUnit.MILLISECONDS.sleep(10);
                            accessCounter.incrementAndGet();
                        } finally {
                            try {
                                image.release();
                            } catch (Throwable e) {
                                accessExceptionCounter.incrementAndGet();
                                log.error("Exception in accessImageTask", e);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.await();
        TimeUnit.SECONDS.sleep(10);
        running.set(false);


        assertTrue(endLatch.await(5, TimeUnit.SECONDS), "Not all threads finished in time");

        es.shutdownNow();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS), "ExecutorService did not terminate in time");

        assertEquals(0, updateExceptionCounter.get(), "Exceptions in update task: " + updateExceptionCounter.get());
        assertEquals(0, accessExceptionCounter.get(), "Exceptions in access tasks: " + accessExceptionCounter.get());

        log.info("Update operations: {}", updateCounter.get());
        log.info("Access operations: {}", accessCounter.get());

        S3ObjectsImage finalImage;
        synchronized (imageLock) {
            finalImage = current.get();
        }
        try {
            finalImage.release();
        } catch (Throwable ignore) {
            // best effort clean-up; errors here are irrelevant to the concurrency assertions
        }
    }

}
