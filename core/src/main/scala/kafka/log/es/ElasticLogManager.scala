package kafka.log.es

import kafka.log._
import kafka.server.LogDirFailureChannel
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import sdk.elastic.stream.api.Client

import java.io.File
import java.util.concurrent.ConcurrentHashMap

class ElasticLogManager(val client: Client) {
  private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticLog]()

  def getLog(dir: File,
             config: LogConfig,
             recoveryPoint: Long,
             scheduler: Scheduler,
             time: Time,
             topicPartition: TopicPartition,
             logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    // TODO: add log close hook, remove closed elastic log
    val elasticLog = ElasticLog(client, dir, config, recoveryPoint, scheduler, time, topicPartition, logDirFailureChannel)
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

  def getLog(dir: File,
             config: LogConfig,
             recoveryPoint: Long,
             scheduler: Scheduler,
             time: Time,
             topicPartition: TopicPartition,
             logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    Default.getLog(dir, config, recoveryPoint, scheduler, time, topicPartition, logDirFailureChannel)
  }

  def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time): ElasticLogSegment = {
    Default.newSegment(topicPartition, baseOffset, time)
  }
}
