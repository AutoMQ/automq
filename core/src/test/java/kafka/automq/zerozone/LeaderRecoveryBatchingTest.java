/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.wal.impl.object.RecoveryBatchingWindow;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers leader-transition detection without treating initial metadata or ISR changes as recovery. */
@Tag("S3Unit")
public class LeaderRecoveryBatchingTest {
    private static final Uuid TOPIC_ID = Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ");

    /** Drain hint uses the same bounded policy once, without starting it on older or unknown feature versions. */
    @Test
    public void testDrainStartsOnlyForZeroZoneV2() {
        RecoveryBatchingWindow window = Mockito.mock(RecoveryBatchingWindow.class);
        Mockito.when(window.startRecovery()).thenReturn(true, false);
        try (MockedStatic<RecoveryBatchingWindow> policies = Mockito.mockStatic(RecoveryBatchingWindow.class)) {
            policies.when(() -> RecoveryBatchingWindow.forNode(7)).thenReturn(window);
            assertFalse(LeaderRecoveryBatching.startBeforeDrain(null, 7));
            assertFalse(LeaderRecoveryBatching.startBeforeDrain(AutoMQVersion.V2, 7));
            policies.verifyNoInteractions();
            assertTrue(LeaderRecoveryBatching.startBeforeDrain(AutoMQVersion.V3, 7));
            assertFalse(LeaderRecoveryBatching.startBeforeDrain(AutoMQVersion.V5, 7));
            Mockito.verify(window, Mockito.times(2)).startRecovery();
            policies.verify(() -> RecoveryBatchingWindow.forNode(7), Mockito.times(2));
        }
    }

    /** A topic's first publication, including a partition added to an existing topic, is not a handoff. */
    @Test
    public void testInitialImageAndNewPartitionAreIgnored() {
        MetadataDelta initial = initial(0);
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(initial));
        MetadataDelta delta = new MetadataDelta(initial.apply(MetadataProvenance.EMPTY));
        delta.replay(partition(1, 1));
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(delta));
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(new MetadataDelta(MetadataImage.EMPTY)));
    }

    /** An existing partition moving to another leader or recovering from no leader starts recovery. */
    @Test
    public void testLeaderHandoffAndOfflineRecovery() {
        MetadataDelta delta = new MetadataDelta(initial(0).apply(MetadataProvenance.EMPTY));
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(1));
        assertTrue(LeaderRecoveryBatching.hasLeaderHandoff(delta));
        delta = new MetadataDelta(initial(-1).apply(MetadataProvenance.EMPTY));
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(1));
        assertTrue(LeaderRecoveryBatching.hasLeaderHandoff(delta));
    }

    /** Losing the leader or changing epochs/ISR without a new usable leader does not start a window. */
    @Test
    public void testUnchangedLeaderAndLeaderLossAreIgnored() {
        MetadataImage image = initial(0).apply(MetadataProvenance.EMPTY);
        MetadataDelta delta = new MetadataDelta(image);
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(0));
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(delta));
        delta = new MetadataDelta(image);
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setIsr(Arrays.asList(0)));
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(delta));
        delta = new MetadataDelta(image);
        delta.replay(new PartitionChangeRecord().setTopicId(TOPIC_ID).setPartitionId(0).setLeader(-1));
        assertFalse(LeaderRecoveryBatching.hasLeaderHandoff(delta));
    }

    private static MetadataDelta initial(int leader) {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(TOPIC_ID).setName("recovery"));
        delta.replay(partition(0, leader));
        return delta;
    }

    private static PartitionRecord partition(int id, int leader) {
        return new PartitionRecord().setTopicId(TOPIC_ID).setPartitionId(id).setLeader(leader)
            .setReplicas(Arrays.asList(0, 1)).setIsr(Arrays.asList(0, 1));
    }
}
