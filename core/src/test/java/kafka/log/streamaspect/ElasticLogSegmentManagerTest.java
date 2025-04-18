/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.log.streamaspect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
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

        when(manager.remove(anyLong())).thenReturn(logSegment);

        when(manager.asyncPersistLogMeta()).thenReturn(CompletableFuture.completedFuture(logMeta));

        ElasticLogSegmentManager.EventListener listener = manager.new EventListener();
        listener.onEvent(1, ElasticLogSegmentEvent.SEGMENT_DELETE);

        verify(manager, atLeastOnce()).asyncPersistLogMeta();
        verify(manager, atMost(2)).asyncPersistLogMeta();
    }

    @Test
    public void testLargeScaleSegmentDelete() throws InterruptedException {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        ElasticLogSegment logSegment = mock(ElasticLogSegment.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));

        Set<Long> removedSegmentId = new HashSet<>();

        when(manager.remove(anyLong())).thenAnswer(invocation -> {
            long id = invocation.getArgument(0);
            removedSegmentId.add(id);
            return logSegment;
        });

        CountDownLatch latch = new CountDownLatch(2);

        when(manager.asyncPersistLogMeta())
            .thenAnswer(invocation -> {
                CompletableFuture<Object> cf = new CompletableFuture<>()
                    .completeOnTimeout(logMeta, 100, TimeUnit.MILLISECONDS);

                cf.whenComplete((res, e) -> {
                    latch.countDown();
                });

                return cf;
            });

        ElasticLogSegmentManager.EventListener listener = spy(manager.new EventListener());

        for (long i = 0L; i < 10L; i++) {
            listener.onEvent(i, ElasticLogSegmentEvent.SEGMENT_DELETE);
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
