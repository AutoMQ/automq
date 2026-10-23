/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protects bounded recovery priority, topic lifecycle and deferred-context isolation. */
@Tag("S3Unit")
class RecoveryProduceSchedulingTest {
    private static final Uuid TOPIC_ID = Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ");
    private static final TopicPartition PARTITION = new TopicPartition("recovery", 0);

    /** Given a handoff, prioritize the queued burst without extending its fixed deadline. */
    @Test
    void testBoundedBurstAfterHandoff() {
        MockTime time = new MockTime();
        RecoveryProduceScheduling scheduling = new RecoveryProduceScheduling(time);
        MetadataDelta initial = initial();
        scheduling.onMetadataUpdate(initial);
        assertFalse(scheduling.consume(Set.of(PARTITION)));
        scheduling.onMetadataUpdate(handoff(initial.apply(MetadataProvenance.EMPTY)));
        assertFalse(scheduling.consume(Set.of(new TopicPartition("other", 0))));
        assertTrue(scheduling.consume(Set.of(PARTITION, new TopicPartition("other", 0))));
        time.sleep(600);
        assertTrue(scheduling.consume(Set.of(PARTITION)));
        time.sleep(67);
        assertFalse(scheduling.consume(Set.of(PARTITION)));
    }

    /** Given concurrent handlers, both use the same burst and neither extends its expiry. */
    @Test
    void testConcurrentConsume() throws Exception {
        MockTime time = new MockTime();
        RecoveryProduceScheduling scheduling = new RecoveryProduceScheduling(time);
        scheduling.onMetadataUpdate(handoff(initial().apply(MetadataProvenance.EMPTY)));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Boolean>> results = executor.invokeAll(List.of(
                () -> scheduling.consume(Set.of(PARTITION)), () -> scheduling.consume(Set.of(PARTITION))));
            assertTrue(results.get(0).get());
            assertTrue(results.get(1).get());
            time.sleep(667);
            assertFalse(scheduling.consume(Set.of(PARTITION)));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /** Given topic deletion or broker close, stale hints cannot affect a future topic of the same name. */
    @Test
    void testDeletionAndCloseDiscardHints() {
        RecoveryProduceScheduling scheduling = new RecoveryProduceScheduling();
        MetadataDelta moved = handoff(initial().apply(MetadataProvenance.EMPTY));
        scheduling.onMetadataUpdate(moved);
        MetadataDelta deleted = new MetadataDelta(moved.apply(MetadataProvenance.EMPTY));
        deleted.replay(new RemoveTopicRecord().setTopicId(TOPIC_ID));
        scheduling.onMetadataUpdate(deleted);
        assertFalse(scheduling.consume(Set.of(PARTITION)));
        scheduling.onMetadataUpdate(handoff(initial().apply(MetadataProvenance.EMPTY)));
        scheduling.clear();
        assertFalse(scheduling.consume(Set.of(PARTITION)));
    }

    /** Given an urgent routed write, preserve the internal-topic bit and isolate its detached append context. */
    @Test
    void testFlagAndDeferredContextIsolation() {
        ZoneRouterProduceRequest.Flag flag = new ZoneRouterProduceRequest.Flag().internalTopicsAllowed(true)
            .recoveryScheduling(true);
        assertTrue(new ZoneRouterProduceRequest.Flag(flag.value()).internalTopicsAllowed());
        assertTrue(new ZoneRouterProduceRequest.Flag(flag.value()).recoveryScheduling());
        assertFalse(flag.recoveryScheduling(false).recoveryScheduling());
        ZeroZoneThreadLocalContext.writeContext().recoveryScheduling = true;
        ZeroZoneThreadLocalContext.WriteContext detached = ZeroZoneThreadLocalContext.writeContext().detach();
        assertTrue(detached.recoveryScheduling());
        assertFalse(ZeroZoneThreadLocalContext.writeContext().recoveryScheduling());
        detached.reset();
        assertFalse(detached.recoveryScheduling());
    }

    /** Given a slow apply, use the published image without renewing the burst on callback catch-up. */
    @Test
    void testPublishedHandoffBeforeMetadataCallback() {
        MockTime time = new MockTime();
        RecoveryProduceScheduling scheduling = new RecoveryProduceScheduling(time);
        MetadataImage before = initial().apply(MetadataProvenance.EMPTY);
        MetadataDelta moved = handoff(before);
        MetadataImage published = moved.apply(MetadataProvenance.EMPTY);
        scheduling.observePendingHandoffs(before, published, Set.of(PARTITION));
        assertTrue(scheduling.consume(Set.of(PARTITION)));
        time.sleep(667);
        scheduling.onMetadataUpdate(moved);
        scheduling.observePendingHandoffs(before, published, Set.of(PARTITION));
        assertFalse(scheduling.consume(Set.of(PARTITION)));
        scheduling.observePendingHandoffs(MetadataImage.EMPTY, before, Set.of(PARTITION));
        assertFalse(scheduling.consume(Set.of(PARTITION)));
    }

    private static MetadataDelta initial() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(TOPIC_ID).setName("recovery"));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(0)
            .setReplicas(List.of(0, 1)).setIsr(List.of(0, 1)));
        return delta;
    }

    private static MetadataDelta handoff(MetadataImage image) {
        MetadataDelta delta = new MetadataDelta(image);
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(1));
        return delta;
    }
}
