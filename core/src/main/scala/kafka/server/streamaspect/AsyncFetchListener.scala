/*
 * Copyright 2026, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.streamaspect

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import java.util.concurrent.ExecutorService

class AsyncFetchListener(
  delegate: FetchListener,
  executor: ExecutorService
) extends FetchListener with Logging {

  override def onFetch(topicPartition: TopicPartition, sessionId: Int,
                       fetchOffset: Long, timestamp: Long): Unit = {
    try {
      executor.execute(() => {
        try {
          delegate.onFetch(topicPartition, sessionId, fetchOffset, timestamp)
        } catch {
          case e: Exception =>
            error(s"Error in fetch listener for partition $topicPartition and session $sessionId", e)
        }
      })
    } catch {
      case e: Exception =>
        error(s"Failed to submit fetch listener task for partition $topicPartition and session $sessionId", e)
    }
  }

  override def onSessionClosed(topicPartition: TopicPartition, sessionId: Int): Unit = {
    try {
      executor.execute(() => {
        try {
          delegate.onSessionClosed(topicPartition, sessionId)
        } catch {
          case e: Exception =>
            error(s"Error in session close listener for partition $topicPartition and session $sessionId", e)
        }
      })
    } catch {
      case e: Exception =>
        error(s"Failed to submit session close task for partition $topicPartition and session $sessionId", e)
    }
  }
}
