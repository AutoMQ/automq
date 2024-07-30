/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class ElasticLogSegmentManagerTest {
    @Test
    public void testSegmentDelete() {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));

        when(manager.asyncPersistLogMeta()).thenReturn(CompletableFuture.completedFuture(logMeta));

        ElasticLogSegmentManager.EventListener listener = manager.new EventListener();
        listener.onEvent(1, ElasticLogSegmentEvent.SEGMENT_DELETE);

        verify(manager, atMost(1)).asyncPersistLogMeta();
    }

    @Test
    public void testLargeScaleSegmentDelete() throws InterruptedException {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));

        Queue<CompletableFuture<ElasticLogMeta>> queue = new ConcurrentLinkedQueue<>();

        when(manager.asyncPersistLogMeta()).thenAnswer(invocation -> {
            CompletableFuture<ElasticLogMeta> cf = new CompletableFuture<>();
            queue.add(cf);

            return cf;
        });

        ElasticLogSegmentManager.EventListener listener = manager.new EventListener();
        for (long i = 0L; i < 1000L; i++) {
            listener.onEvent(i, ElasticLogSegmentEvent.SEGMENT_DELETE);
        }

        Thread.sleep(20);

        for (long i = 1001L; i < 2000L; i++) {
            listener.onEvent(i, ElasticLogSegmentEvent.SEGMENT_DELETE);
        }

        for (CompletableFuture<ElasticLogMeta> elasticLogMetaCompletableFuture : queue) {
            elasticLogMetaCompletableFuture.complete(logMeta);
        }

        verify(manager, atMost(2)).asyncPersistLogMeta();
    }
}
