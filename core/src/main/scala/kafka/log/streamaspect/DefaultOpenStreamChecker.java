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
import org.apache.kafka.common.errors.s3.StreamFencedException;
import org.apache.kafka.image.S3StreamMetadataImage;

public class DefaultOpenStreamChecker implements OpenStreamChecker {
    private final KRaftMetadataCache metadataCache;

    public DefaultOpenStreamChecker(KRaftMetadataCache metadataCache) {
        this.metadataCache = metadataCache;
    }

    @Override
    public boolean check(long streamId, long epoch) throws StreamFencedException {
        S3StreamMetadataImage stream = metadataCache.currentImage().streamsMetadata().getStreamMetadata(streamId);
        if (stream == null) {
            throw new StreamFencedException("streamId=" + streamId + " cannot be found, it may be deleted or not created yet");
        }
        if (stream.getEpoch() > epoch) {
            throw new StreamFencedException("streamId=" + streamId + " with epoch=" + epoch + " is fenced by new epoch=" + stream.getEpoch());
        }
        return StreamState.CLOSED.equals(stream.state());
    }
}
