/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.autobalancer.detector;

import kafka.autobalancer.common.Utils;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Holds optional balancing after observed broker lifecycle transitions, including controlled shutdown.
 * Metadata replay owns membership; synchronized methods share the bounded membership and generation with
 * the detector. Initial membership does not start a hold. The hold expires without requiring a failed
 * broker to return and never gates controller failover or slow-source evacuation. The whole-cluster
 * hold is capped at 30 seconds so unaffected load can rebalance; recently recovered partitions retain
 * their configured individual cooldown. It is a cooldown,
 * not a claim that storage/client recovery has been measured. No health-service dependency is added.
 */
final class BrokerRecoveryCooldown implements BrokerStatusListener {
    private static final long MAX_BROKER_HOLD_MS = TimeUnit.SECONDS.toMillis(30);
    private final Time time;
    private final Map<Integer, Uuid> incarnations = new HashMap<>();
    private final Map<Integer, Boolean> activeBrokers = new HashMap<>();
    private final Set<Integer> activatedBrokers = new HashSet<>();
    private boolean recovering;
    private long lastRecoveryNanos;
    private long generation;

    BrokerRecoveryCooldown(Time time) {
        this.time = time;
    }

    /** Registers initial membership without inventing a recovery; a known incarnation change starts a hold. */
    @Override
    public synchronized void onBrokerRegister(RegisterBrokerRecord record) {
        Uuid previous = incarnations.put(record.brokerId(), record.incarnationId());
        boolean active = !Utils.isBrokerFenced(record);
        Boolean wasActive = activeBrokers.put(record.brokerId(), active);
        if (previous != null && activatedBrokers.contains(record.brokerId())
            && (!previous.equals(record.incarnationId()) || !Boolean.valueOf(active).equals(wasActive))) {
            recordRecovery();
        }
        if (active) {
            activatedBrokers.add(record.brokerId());
        }
    }

    /** Unregistering known active membership starts recovery; duplicate removal does not extend it. */
    @Override
    public synchronized void onBrokerUnregister(UnregisterBrokerRecord record) {
        incarnations.remove(record.brokerId());
        activatedBrokers.remove(record.brokerId());
        if (Boolean.TRUE.equals(activeBrokers.remove(record.brokerId()))) {
            recordRecovery();
        }
    }

    /** A real active/draining/fenced transition starts a hold; duplicate and no-op records do not. */
    @Override
    public synchronized void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        Utils.isBrokerFenced(record).ifPresent(fenced -> {
            Boolean wasActive = activeBrokers.get(record.brokerId());
            if (wasActive != null && wasActive != !fenced) {
                activeBrokers.put(record.brokerId(), !fenced);
                if (activatedBrokers.contains(record.brokerId())) {
                    recordRecovery();
                }
                if (!fenced) {
                    activatedBrokers.add(record.brokerId());
                }
            }
        });
    }

    private void recordRecovery() {
        lastRecoveryNanos = time.nanoseconds();
        recovering = true;
        generation++;
    }

    synchronized long generation() {
        return generation;
    }

    synchronized boolean isCoolingDown(long cooldownMs) {
        return cooldownMs > 0 && recovering
            && time.nanoseconds() - lastRecoveryNanos
                < TimeUnit.MILLISECONDS.toNanos(Math.min(cooldownMs, MAX_BROKER_HOLD_MS));
    }

    int excludeRecoveredPartitions(ClusterModelSnapshot snapshot, long cooldownMs) {
        if (cooldownMs <= 0) {
            return 0;
        }
        int excluded = 0;
        long now = time.milliseconds();
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            if (broker.isSlowBroker()) {
                continue;
            }
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica :
                new ArrayList<>(snapshot.replicasFor(broker.getBrokerId()))) {
                long changed = replica.leaderChangeTimeMs();
                if (changed >= 0 && now - changed < cooldownMs
                    && snapshot.excludeTopicPartition(broker.getBrokerId(), replica.getTopicPartition())) {
                    excluded++;
                }
            }
        }
        return excluded;
    }

    int excludeFrom(ClusterModelSnapshot snapshot, long cooldownMs) {
        if (!isCoolingDown(cooldownMs)) {
            return 0;
        }
        int excluded = 0;
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            if (broker.isSlowBroker()) {
                continue;
            }
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica :
                new ArrayList<>(snapshot.replicasFor(broker.getBrokerId()))) {
                if (snapshot.excludeTopicPartition(broker.getBrokerId(), replica.getTopicPartition())) {
                    excluded++;
                }
            }
        }
        return excluded;
    }
}
