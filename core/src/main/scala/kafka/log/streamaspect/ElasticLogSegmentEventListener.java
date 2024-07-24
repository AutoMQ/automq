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

import java.util.List;

public interface ElasticLogSegmentEventListener {
    ElasticLogSegmentEventListener NOOP = new ElasticLogSegmentEventListener() {
        @Override
        public void onEvent(long segmentBaseOffset, ElasticLogSegmentEvent event) {

        }

        @Override
        public void onSegmentsDelete(List<Long> segmentBaseOffset, ElasticLogSegmentEvent event) {

        }
    };

    void onEvent(long segmentBaseOffset, ElasticLogSegmentEvent event);

    void onSegmentsDelete(List<Long> segmentBaseOffset, ElasticLogSegmentEvent event);
}
