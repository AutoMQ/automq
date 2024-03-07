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

package kafka.log.streamaspect

import kafka.utils.threadsafe
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.internals.log.{LogSegment, LogSegments}

import java.util.Optional

/**
 * This class caches the active segment to avoid the overhead of searching the segment map.
 * Note: any modification to the returned iterator from [[LogSegments.values]] may cause
 * inconsistency between the cached active segment and the actual active segment. However,
 * now there is no such modification in the code base, so we just ignore this case.
 */
// TODO: to java
class CachedLogSegments(_topicPartition: TopicPartition) extends LogSegments(_topicPartition){

    /**
     * Its [[LogSegment.baseOffset]] will always be the largest one among all segments.
     */
    private var _activeSegment: LogSegment = null

    @threadsafe
    override def add(segment: LogSegment): LogSegment = {
        this.synchronized {
            if (null == _activeSegment || segment.baseOffset >= _activeSegment.baseOffset) {
                _activeSegment = segment
            }
            super.add(segment)
        }
    }

    @threadsafe
    override def remove(offset: Long): Unit = {
        this.synchronized {
            if (null != _activeSegment && offset == _activeSegment.baseOffset) {
                _activeSegment = null
            }
            super.remove(offset)
        }
    }

    @threadsafe
    override def clear(): Unit = {
        this.synchronized {
            _activeSegment = null
            super.clear()
        }
    }

    @threadsafe
    override def lastSegment: Optional[LogSegment] = {
        this.synchronized {
            Optional.of(_activeSegment)
        }
    }

    @threadsafe
    override def activeSegment: LogSegment = {
        this.synchronized {
            _activeSegment
        }
    }
}
