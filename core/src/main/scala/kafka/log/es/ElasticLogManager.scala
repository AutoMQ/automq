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

import kafka.log._
import kafka.server.LogDirFailureChannel
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import sdk.elastic.stream.api.Client

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client) {
  private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticLog]()

  def getLog(dir: File,
             config: LogConfig,
             scheduler: Scheduler,
             time: Time,
             topicPartition: TopicPartition,
             logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    // TODO: add log close hook, remove closed elastic log
    val elasticLog = ElasticLog(client, dir, config, scheduler, time, topicPartition, logDirFailureChannel)
    elasticLogs.put(topicPartition, elasticLog)
    elasticLog
  }

  /**
   * New elastic log segment.
   */
  def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time): ElasticLogSegment = {
    val elasticLog = elasticLogs.get(topicPartition)
    if (elasticLog == null) {
      throw new IllegalStateException(s"Cannot find elastic log for $topicPartition")
    }
    elasticLog.newSegment(baseOffset, time)
  }

}

object ElasticLogManager {
  val Default = new ElasticLogManager(new MemoryClient())

  def getElasticLog(topicPartition: TopicPartition): ElasticLog = Default.elasticLogs.get(topicPartition)

  def getAllElasticLogs: Iterable[ElasticLog] = Default.elasticLogs.asScala.values

  def getLog(dir: File,
             config: LogConfig,
             scheduler: Scheduler,
             time: Time,
             topicPartition: TopicPartition,
             logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    Default.getLog(dir, config, scheduler, time, topicPartition, logDirFailureChannel)
  }

  def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time): ElasticLogSegment = {
    Default.newSegment(topicPartition, baseOffset, time)
  }
}
