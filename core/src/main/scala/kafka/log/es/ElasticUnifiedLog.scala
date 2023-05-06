/*
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

package kafka.log.es

import kafka.log.{LogSegment, ProducerStateManager, UnifiedLog}
import kafka.server.BrokerTopicStats
import kafka.server.epoch.LeaderEpochFileCache
import org.apache.kafka.common.Uuid

class ElasticUnifiedLog(logStartOffset: Long,
                        elasticLog: ElasticLog,
                        brokerTopicStats: BrokerTopicStats,
                        producerIdExpirationCheckIntervalMs: Int,
                        leaderEpochCache: Option[LeaderEpochFileCache],
                        producerStateManager: ProducerStateManager,
                        _topicId: Option[Uuid],
                        keepPartitionMetadataFile: Boolean)
  extends UnifiedLog(logStartOffset, elasticLog, brokerTopicStats, producerIdExpirationCheckIntervalMs,
    leaderEpochCache, producerStateManager, _topicId, keepPartitionMetadataFile) {
  override private[log] def replaceSegments(newSegments: collection.Seq[LogSegment], oldSegments: collection.Seq[LogSegment]): Unit = {
    elasticLog.replaceSegments(newSegments, oldSegments)
  }
}
