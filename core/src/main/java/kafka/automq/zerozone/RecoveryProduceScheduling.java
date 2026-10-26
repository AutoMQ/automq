/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Broker-owned scheduling hints for a bounded queued-write burst after a
 * published leader handoff. Deleted topics and broker close discard hints.
 * Only scheduling changes; no executor, acknowledgement or ownership changes.
 */
public final class RecoveryProduceScheduling {
    // Match one existing recovery-batching burst, without extending it on each request.
    private static final long BURST_NANOS = TimeUnit.SECONDS.toNanos(2) / 3;
    private final Set<TopicPartition> pending = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, Integer> grantedEpochs = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> burstStarts = new ConcurrentHashMap<>();
    private final Time time;

    /** Uses the monotonic broker clock; no scheduled tasks are created. */
    public RecoveryProduceScheduling() {
        this(Time.SYSTEM);
    }

    RecoveryProduceScheduling(Time time) {
        this.time = time;
    }

    /** Registers handoffs, but not initial topic creation, and removes deleted topic state. */
    public void onMetadataUpdate(MetadataDelta delta) {
        if (delta.topicsDelta() == null) {
            return;
        }
        delta.topicsDelta().deletedTopicIds().forEach(id -> {
            TopicImage previous = delta.topicsDelta().image().getTopic(id);
            if (previous != null) {
                pending.removeIf(tp -> tp.topic().equals(previous.name()));
                grantedEpochs.keySet().removeIf(tp -> tp.topic().equals(previous.name()));
                burstStarts.keySet().removeIf(tp -> tp.topic().equals(previous.name()));
            }
        });
        for (TopicDelta topic : delta.topicsDelta().changedTopics().values()) {
            for (Map.Entry<Integer, PartitionRegistration> change : topic.partitionChanges().entrySet()) {
                PartitionRegistration previous = topic.image().partitions().get(change.getKey());
                int leader = change.getValue().leader;
                if (previous != null && leader >= 0 && leader != previous.leader) {
                    grant(new TopicPartition(topic.image().name(), change.getKey()), change.getValue().leaderEpoch);
                }
            }
        }
    }

    /**
     * Observes the already published cache image while a partition open/close is
     * delaying this interceptor's metadata callback. Each leader epoch still
     * grants at most one burst, even when the callback subsequently catches up.
     * Initial creation and same-name topic recreation never grant recovery.
     */
    public void observePendingHandoffs(MetadataImage applied, MetadataImage published, Set<TopicPartition> partitions) {
        if (applied == published) {
            return;
        }
        for (TopicPartition tp : partitions) {
            TopicImage oldTopic = applied.topics().getTopic(tp.topic());
            TopicImage newTopic = published.topics().getTopic(tp.topic());
            if (oldTopic == null || newTopic == null || !oldTopic.id().equals(newTopic.id())) {
                continue;
            }
            PartitionRegistration before = oldTopic.partitions().get(tp.partition());
            PartitionRegistration after = newTopic.partitions().get(tp.partition());
            if (before != null && after != null && after.leader >= 0 && after.leader != before.leader) {
                grant(tp, after.leaderEpoch);
            }
        }
    }

    private void grant(TopicPartition partition, int leaderEpoch) {
        grantedEpochs.compute(partition, (tp, previous) -> {
            if (previous == null || leaderEpoch > previous) {
                burstStarts.remove(tp);
                pending.add(tp);
                return leaderEpoch;
            }
            return previous;
        });
    }

    /**
     * Starts a fixed-length burst on the first frame and includes queued writes
     * that follow its ACK. Concurrent handlers never extend the deadline.
     * Urgency covers healthy siblings sharing the recovery response frame.
     */
    public boolean consume(Set<TopicPartition> partitions) {
        boolean recovery = false;
        for (TopicPartition tp : partitions) {
            grantedEpochs.computeIfPresent(tp, (partition, epoch) -> {
                if (pending.remove(partition)) {
                    burstStarts.put(partition, time.nanoseconds());
                }
                return epoch;
            });
            Long start = burstStarts.get(tp);
            recovery |= start != null && time.nanoseconds() - start < BURST_NANOS;
        }
        return recovery;
    }

    /** Drops this broker's hints without altering any partition or stream. */
    public void clear() {
        pending.clear();
        grantedEpochs.clear();
        burstStarts.clear();
    }
}
