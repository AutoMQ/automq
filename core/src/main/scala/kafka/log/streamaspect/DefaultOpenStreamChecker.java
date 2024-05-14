/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.automq.stream.s3.metadata.StreamState;
import kafka.server.metadata.KRaftMetadataCache;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.s3.StreamFencedException;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.PartitionRegistration;

public class DefaultOpenStreamChecker implements OpenStreamChecker {
    private final KRaftMetadataCache metadataCache;

    public DefaultOpenStreamChecker(KRaftMetadataCache metadataCache) {
        this.metadataCache = metadataCache;
    }

    @Override
    public boolean check(Uuid topicId, int partition, long streamId, long epoch) throws StreamFencedException {
        // When ABA reassign happens:
        // 1. Assign P0 to broker0 with epoch=0, broker0 opens the partition
        // 2. Assign P0 to broker1 with epoch=1, broker1 waits for the partition to be closed
        // 3. Quick reassign P0 to broker0 with epoch=2, broker0 merge step2/3 image and keep stream opened with epoch=0
        // 4. So broker1 should check partition leader epoch to fail the waiting
        TopicImage topicImage = metadataCache.currentImage().topics().getTopic(topicId);
        if (topicImage == null) {
            throw new StreamFencedException(String.format("topicId=%s cannot be found, it may be deleted or not created yet", topicId));
        }
        PartitionRegistration partitionImage = topicImage.partitions().get(partition);
        if (partitionImage == null) {
            throw new StreamFencedException(String.format("partition=%s-%d cannot be found, it may be deleted or not created yet", topicId, partition));
        }
        int currentEpoch = partitionImage.leaderEpoch;
        if (currentEpoch > epoch) {
            throw new StreamFencedException(String.format("partition=%s-%d with epoch=%d is fenced by new leader epoch=%d", topicId, partition, epoch, currentEpoch));
        }
        S3StreamMetadataImage stream = metadataCache.currentImage().streamsMetadata().getStreamMetadata(streamId);
        if (stream == null) {
            throw new StreamFencedException(String.format("streamId=%d cannot be found, it may be deleted or not created yet", streamId));
        }
        if (stream.getEpoch() > epoch)
            throw new StreamFencedException(String.format("streamId=%d with epoch=%d is fenced by new leader epoch=%d", streamId, epoch, stream.getEpoch()));
        return StreamState.CLOSED.equals(stream.state());
    }
}
