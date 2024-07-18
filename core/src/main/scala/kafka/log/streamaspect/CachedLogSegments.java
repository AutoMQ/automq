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

import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegments;

public class CachedLogSegments extends LogSegments {
    /**
     * Its [[LogSegment.baseOffset]] will always be the largest one among all segments.
     */
    private LogSegment activeSegment = null;

    public CachedLogSegments(TopicPartition topicPartition) {
        super(topicPartition);
    }

    @Override
    public LogSegment add(LogSegment segment) {
        synchronized (this) {
            if (null == activeSegment || segment.baseOffset() >= activeSegment.baseOffset()) {
                activeSegment = segment;
            }
            return super.add(segment);
        }
    }

    @Override
    public void remove(long offset) {
        synchronized (this) {
            if (null != activeSegment && offset == activeSegment.baseOffset()) {
                activeSegment = null;
            }
            super.remove(offset);
        }
    }

    @Override
    public void clear() {
        synchronized (this) {
            activeSegment = null;
            super.clear();
        }
    }

    @Override
    public Optional<LogSegment> lastSegment() {
        synchronized (this) {
            return Optional.of(activeSegment);
        }
    }

    @Override
    public LogSegment activeSegment() {
        synchronized (this) {
            return activeSegment;
        }
    }
}

