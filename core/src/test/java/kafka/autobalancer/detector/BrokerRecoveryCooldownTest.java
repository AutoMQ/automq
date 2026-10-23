/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.autobalancer.detector;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protects healthy and unrelated partitions during broker recovery without disabling emergency evacuation. */
@Tag("S3Unit")
public class BrokerRecoveryCooldownTest {
    private static final long COOLDOWN_MS = 30_000L;
    private static final TopicPartition RECOVERED = new TopicPartition("topic", 0);
    private static final TopicPartition UNRELATED = new TopicPartition("topic", 1);

    /** Initial replay and a newly registered broker's first activation must not fabricate a restart. */
    @Test
    public void testInitialReplayAndActivationDoNotHoldBalancing() {
        BrokerRecoveryCooldown guard = guard(new MockTime());
        guard.onBrokerRegister(broker(3, true));
        guard.onBrokerRegistrationChanged(change(3, (byte) -1));
        assertFalse(guard.isCoolingDown(COOLDOWN_MS));
        assertEquals(0, guard.generation());
        assertEquals(0, guard.excludeFrom(snapshot(false), COOLDOWN_MS));
    }

    /** A real shutdown filters recovered and unrelated healthy sources, retaining all broker load accounting. */
    @Test
    public void testControlledShutdownHoldsAllHealthySources() {
        BrokerRecoveryCooldown guard = guard(new MockTime());
        guard.onBrokerRegistrationChanged(new BrokerRegistrationChangeRecord().setBrokerId(0)
            .setInControlledShutdown((byte) 1));
        ClusterModelSnapshot snapshot = snapshot(false);
        double load = snapshot.broker(1).loadValue((byte) 0);
        assertEquals(2, guard.excludeFrom(snapshot, COOLDOWN_MS));
        assertNull(snapshot.replica(1, RECOVERED));
        assertNull(snapshot.replica(2, UNRELATED));
        assertEquals(load, snapshot.broker(1).loadValue((byte) 0));
    }

    /** Duplicate/no-op lifecycle records do not postpone expiry or require an absent broker to return. */
    @Test
    public void testDuplicateFenceAndUnregisterDoNotExtendExpiry() {
        MockTime time = new MockTime(0, 1_000L, 0);
        BrokerRecoveryCooldown guard = guard(time);
        guard.onBrokerRegistrationChanged(change(0, (byte) 1));
        time.sleep(COOLDOWN_MS - 1);
        guard.onBrokerRegistrationChanged(change(0, (byte) 1));
        guard.onBrokerRegistrationChanged(change(0, (byte) 0));
        guard.onBrokerUnregister(new UnregisterBrokerRecord().setBrokerId(0));
        guard.onBrokerUnregister(new UnregisterBrokerRecord().setBrokerId(0));
        assertTrue(guard.isCoolingDown(COOLDOWN_MS));
        assertEquals(1, guard.generation());
        time.sleep(1);
        assertFalse(guard.isCoolingDown(COOLDOWN_MS));
        assertEquals(0, guard.excludeFrom(snapshot(false), COOLDOWN_MS));
    }

    /** Slow-source partitions stay eligible while healthy sources are held; normal model data is not deleted. */
    @Test
    public void testSlowSourceEvacuationRemainsEligible() {
        BrokerRecoveryCooldown guard = guard(new MockTime());
        guard.onBrokerRegistrationChanged(change(0, (byte) 1));
        ClusterModelSnapshot snapshot = snapshot(true);
        assertEquals(1, guard.excludeFrom(snapshot, COOLDOWN_MS));
        assertNotNull(snapshot.replica(1, RECOVERED));
        assertNull(snapshot.replica(2, UNRELATED));
    }

    /** A known incarnation change invalidates a plan; repeated registration of the same incarnation does not. */
    @Test
    public void testIncarnationAndSecondFailureResetGeneration() {
        MockTime time = new MockTime();
        BrokerRecoveryCooldown guard = guard(time);
        RegisterBrokerRecord replacement = broker(0, false);
        guard.onBrokerRegister(replacement);
        assertEquals(1, guard.generation());
        time.sleep(COOLDOWN_MS - 1);
        guard.onBrokerRegister(replacement);
        assertEquals(1, guard.generation());
        guard.onBrokerRegistrationChanged(change(1, (byte) 1));
        time.sleep(1);
        assertTrue(guard.isCoolingDown(COOLDOWN_MS));
        assertEquals(2, guard.generation());
        assertFalse(guard.isCoolingDown(0));
    }

    /** Re-activation of an existing broker extends cooldown once, but repeated un-fence records do not. */
    @Test
    public void testReturningBrokerStartsOnePostRecoveryHold() {
        MockTime time = new MockTime();
        BrokerRecoveryCooldown guard = guard(time);
        guard.onBrokerRegistrationChanged(change(0, (byte) 1));
        time.sleep(COOLDOWN_MS);
        guard.onBrokerRegistrationChanged(change(0, (byte) -1));
        assertEquals(2, guard.generation());
        time.sleep(COOLDOWN_MS - 1);
        guard.onBrokerRegistrationChanged(change(0, (byte) -1));
        assertTrue(guard.isCoolingDown(COOLDOWN_MS));
        time.sleep(1);
        assertFalse(guard.isCoolingDown(COOLDOWN_MS));
    }

    /** A long partition cooldown must not pin all other healthy load for the same five-minute duration. */
    @Test
    public void testGlobalHoldIsCappedWhilePartitionCooldownIsLonger() {
        MockTime time = new MockTime();
        BrokerRecoveryCooldown guard = guard(time);
        guard.onBrokerRegistrationChanged(change(0, (byte) 1));
        time.sleep(29_999);
        assertTrue(guard.isCoolingDown(300_000));
        time.sleep(1);
        assertFalse(guard.isCoolingDown(300_000));
    }

    /** Controller recovery protects only its handed-off replica, without holding unrelated load or extending on reads. */
    @Test
    public void testControllerRecoveryCooldownProtectsOnlyChangedPartition() {
        ClusterModelSnapshot snapshot = recoverySnapshot(false);
        TopicPartitionReplicaUpdater.TopicPartitionReplica recovered = snapshot.replica(1, RECOVERED);
        long observed = recovered.leaderChangeTimeMs();
        assertTrue(observed >= 0);
        assertEquals(observed, ((TopicPartitionReplicaUpdater.TopicPartitionReplica) recovered.copy()).leaderChangeTimeMs());
        MockTime time = new MockTime(0, observed + 299_999, 0);
        BrokerRecoveryCooldown guard = new BrokerRecoveryCooldown(time);
        assertEquals(1, guard.excludeRecoveredPartitions(snapshot, 300_000));
        assertNull(snapshot.replica(1, RECOVERED));
        assertNotNull(snapshot.replica(1, UNRELATED));
        snapshot.addTopicPartition(1, RECOVERED, recovered);
        time.sleep(1);
        assertEquals(0, guard.excludeRecoveredPartitions(snapshot, 300_000));
        assertNotNull(snapshot.replica(1, RECOVERED));
    }

    /** A slow source remains evacuable, and disabling partition cooldown preserves both recovery and ordinary load. */
    @Test
    public void testControllerRecoveryCooldownAllowsSlowSourceAndDisable() {
        BrokerRecoveryCooldown guard = new BrokerRecoveryCooldown(new MockTime(0, System.currentTimeMillis() + 1000, 0));
        ClusterModelSnapshot slow = recoverySnapshot(true);
        assertEquals(0, guard.excludeRecoveredPartitions(slow, 300_000));
        assertNotNull(slow.replica(1, RECOVERED));
        ClusterModelSnapshot disabled = recoverySnapshot(false);
        assertEquals(0, guard.excludeRecoveredPartitions(disabled, 0));
        assertNotNull(disabled.replica(1, RECOVERED));
        assertNotNull(disabled.replica(1, UNRELATED));
    }

    /** A recovery between action batches discards the old plan, including batches delayed by an interval. */
    @Test
    public void testDetectorDiscardsQueuedBatchAfterRecovery() throws Exception {
        MockTime time = new MockTime();
        ClusterModel model = Mockito.mock(ClusterModel.class);
        Mockito.when(model.snapshot(Mockito.anySet(), Mockito.anySet(), Mockito.anyLong()))
            .thenReturn(snapshot(false));
        ActionExecutorService executor = Mockito.mock(ActionExecutorService.class);
        AnomalyDetectorImpl detector = Mockito.spy(new AnomalyDetectorImpl(Map.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, Long.MAX_VALUE,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, 1),
            null, model, executor, time));
        Action first = new Action(ActionType.MOVE, RECOVERED, 1, 2);
        Action second = new Action(ActionType.MOVE, UNRELATED, 2, 1);
        Mockito.doReturn(List.of(List.of(first), List.of(second))).when(detector).checkAndGroupActions(
            Mockito.anyList(), Mockito.anyInt(), Mockito.anyDouble(), Mockito.anyInt(), Mockito.anyMap());
        detector.recoveryListener().onBrokerRegister(broker(0, false));
        Mockito.when(executor.execute(Mockito.anyList())).thenAnswer(invocation -> {
            detector.recoveryListener().onBrokerRegistrationChanged(change(0, (byte) 1));
            return CompletableFuture.completedFuture(null);
        });
        try {
            detector.onLeaderChanged(true);
            detector.run();
            detector.detect0();
            Mockito.verify(executor).execute(List.of(first));
            Mockito.verify(executor, Mockito.never()).execute(List.of(second));
        } finally {
            detector.shutdown();
        }
    }

    private static BrokerRecoveryCooldown guard(MockTime time) {
        BrokerRecoveryCooldown guard = new BrokerRecoveryCooldown(time);
        for (int id : Set.of(0, 1, 2)) {
            guard.onBrokerRegister(broker(id, false));
        }
        return guard;
    }

    private static ClusterModelSnapshot recoverySnapshot(boolean slowSource) {
        ClusterModel model = new ClusterModel();
        Uuid topicId = Uuid.randomUuid();
        model.registerBroker(0, "rack", true);
        model.registerBroker(1, "rack", true);
        model.createTopic(topicId, RECOVERED.topic());
        model.reassignPartition(topicId, RECOVERED.partition(), 0);
        model.reassignPartition(topicId, RECOVERED.partition(), 1);
        model.reassignPartition(topicId, UNRELATED.partition(), 1);
        ClusterModelSnapshot snapshot = model.snapshot();
        snapshot.broker(1).setSlowBroker(slowSource);
        return snapshot;
    }

    private static RegisterBrokerRecord broker(int id, boolean fenced) {
        return new RegisterBrokerRecord().setBrokerId(id).setIncarnationId(Uuid.randomUuid()).setFenced(fenced);
    }

    private static BrokerRegistrationChangeRecord change(int id, byte fenced) {
        return new BrokerRegistrationChangeRecord().setBrokerId(id).setFenced(fenced);
    }

    private static ClusterModelSnapshot snapshot(boolean slow) {
        ClusterModelSnapshot snapshot = new ClusterModelSnapshot();
        for (int id : Set.of(1, 2)) {
            BrokerUpdater.Broker broker = new BrokerUpdater.Broker(id, "rack", 0, Map.of(), MetricVersion.V1, false);
            broker.setSlowBroker(id == 1 && slow);
            snapshot.addBroker(broker);
            TopicPartition tp = id == 1 ? RECOVERED : UNRELATED;
            snapshot.addTopicPartition(id, tp, new TopicPartitionReplicaUpdater.TopicPartitionReplica(tp, 0, MetricVersion.V1, false));
        }
        return snapshot;
    }
}
