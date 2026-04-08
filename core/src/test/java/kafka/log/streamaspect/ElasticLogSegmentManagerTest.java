/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.log.streamaspect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class ElasticLogSegmentManagerTest {
    @Test
    public void testSegmentDelete() {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        ElasticLogSegment logSegment = mock(ElasticLogSegment.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));
        manager.put(1, logSegment);

        doReturn(CompletableFuture.completedFuture(logMeta)).when(manager).asyncPersistLogMeta();

        ElasticLogSegmentManager.EventListener listener = manager.new EventListener();

        // mismatch
        listener.onEvent(1, mock(ElasticLogSegment.class), ElasticLogSegmentEvent.SEGMENT_DELETE);
        assertEquals(1, manager.segments.size());

        // match
        listener.onEvent(1, logSegment, ElasticLogSegmentEvent.SEGMENT_DELETE);
        assertEquals(0, manager.segments.size());

        verify(manager, atLeastOnce()).asyncPersistLogMeta();
        verify(manager, atMost(2)).asyncPersistLogMeta();
    }

    @Test
    public void testLargeScaleSegmentDelete() throws InterruptedException {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));
        List<ElasticLogSegment> segments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ElasticLogSegment segment = mock(ElasticLogSegment.class);
            manager.put(i, segment);
            segments.add(segment);
        }

        Set<Long> removedSegmentId = new HashSet<>();

        when(manager.remove(anyLong(), any())).thenAnswer(invocation -> {
            long id = invocation.getArgument(0);
            removedSegmentId.add(id);
            return invocation.callRealMethod();
        });

        CountDownLatch latch = new CountDownLatch(2);

        doAnswer(invocation -> {
            CompletableFuture<Object> cf = new CompletableFuture<>()
                .completeOnTimeout(logMeta, 100, TimeUnit.MILLISECONDS);

            cf.whenComplete((res, e) -> {
                latch.countDown();
            });

            return cf;
        }).when(manager).asyncPersistLogMeta();

        ElasticLogSegmentManager.EventListener listener = spy(manager.new EventListener());

        for (int i = 0; i < 10; i++) {
            listener.onEvent(i, segments.get(i), ElasticLogSegmentEvent.SEGMENT_DELETE);
        }

        latch.await();

        // expect the first and the tail should call the persist method.
        verify(manager, times(2)).asyncPersistLogMeta();

        // check all segmentId removed.
        for (long i = 0; i < 10L; i++) {
            assertTrue(removedSegmentId.contains(i));
        }

        // the request can be finished.
        CompletableFuture<ElasticLogMeta> pendingPersistentMetaCf = listener.getPendingPersistentMetaCf();
        pendingPersistentMetaCf.join();

        // all the queue can be removed.
        assertTrue(listener.getPendingDeleteSegmentQueue().isEmpty());

    }
}
