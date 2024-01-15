/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import kafka.utils.threadsafe
import org.apache.kafka.common.TopicPartition

/**
 * This class caches the active segment to avoid the overhead of searching the segment map.
 * Note: any modification to the returned iterator from [[LogSegments.values]] may cause
 * inconsistency between the cached active segment and the actual active segment. However,
 * now there is no such modification in the code base, so we just ignore this case.
 */
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
  override def lastSegment: Option[LogSegment] = {
    this.synchronized {
      Option(_activeSegment)
    }
  }

  @threadsafe
  override def activeSegment: LogSegment = {
    this.synchronized {
      _activeSegment
    }
  }
}
