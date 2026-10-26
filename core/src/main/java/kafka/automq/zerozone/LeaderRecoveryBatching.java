/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.wal.impl.object.RecoveryBatchingWindow;

import java.util.Map;

/** Identifies leader handoffs and starts bounded object-WAL batching before a ZeroZone V2 leader drains. */
public final class LeaderRecoveryBatching {
    private LeaderRecoveryBatching() {
    }

    /**
     * Shortens already accepted WAL batches before controlled shutdown or partition close can wait on them.
     * Uses the shared two-second rolling-minute budget without changing persistence or admission.
     * Unknown/older feature versions retain their original scheduling; abrupt failure uses the handoff hook.
     */
    public static boolean startBeforeDrain(AutoMQVersion version, int nodeId) {
        return version != null && version.isZeroZoneV2Supported()
            && RecoveryBatchingWindow.forNode(nodeId).startRecovery();
    }

    static boolean hasLeaderHandoff(MetadataDelta delta) {
        if (delta.topicsDelta() == null) {
            return false;
        }
        for (TopicDelta topic : delta.topicsDelta().changedTopics().values()) {
            for (Map.Entry<Integer, PartitionRegistration> change : topic.partitionChanges().entrySet()) {
                PartitionRegistration previous = topic.image().partitions().get(change.getKey());
                int leader = change.getValue().leader;
                if (previous != null && leader >= 0 && leader != previous.leader) {
                    return true;
                }
            }
        }
        return false;
    }
}
